"""Leak-free features for a direct 1-7 day forecast, built with Spark SQL window functions.

Two families of demand features:
  * lag features relative to the target day t, all using sales up to t-7 at the latest;
  * "as of origin" features: every row is assigned a forecast origin o on a weekly grid
    (o < t <= o+7, horizon h = t-o), and gets the latest demand known at o (last value, 7- and
    28-day means ending at o, days since the last sale). Because each row has exactly one origin,
    the model learns how recent demand carries over 1-7 days ahead without replicating rows.
A forecast made at origin o therefore only sees data that exists at o. Price and calendar
features are known in advance (planned prices, published event calendar), so they are used for
day t itself.
"""
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from rdf import config as C

FEATURES = [
    "lag_7", "lag_14", "lag_21", "lag_28",
    "rm_7", "rm_28", "rm_56", "rstd_28", "zero_share_28", "dow_mean_4", "rm_365",
    "dept_store_rm_28", "price", "price_rel_max", "price_change_7", "markdown",
    "snap", "has_event", "event_sporting", "event_cultural", "event_national", "event_religious",
    "dow", "month", "day_of_month", "dept_idx", "cat_idx", "store_idx",
    "horizon", "o_last", "o_rm_7", "o_rm_28", "o_days_since_sale",
]
KEYS = ["item_id", "store_id", "sale_date"]


def sales_source(spark: SparkSession, stores=None) -> DataFrame:
    """History from the batch import plus any days that arrived through the stream."""
    stores = stores or C.SCOPE_STORES
    hist = (spark.table(f"{C.HIVE_DB}.sales_history")
            .where(F.col("store_id").isin(stores))
            .select("item_id", "store_id", "sale_date", "units", F.lit(0).alias("stockout")))
    if spark.catalog.tableExists(f"{C.HIVE_DB}.sales_stream"):
        stream = (spark.table(f"{C.HIVE_DB}.sales_stream")
                  .where(F.col("store_id").isin(stores))
                  .select("item_id", "store_id", "sale_date", "units", "stockout"))
        hist = hist.join(stream.select(*KEYS), KEYS, "left_anti").unionByName(stream)
    return hist


def add_future_spine(spark: SparkSession, sales: DataFrame, origin: str, horizon: int = C.HORIZON) -> DataFrame:
    """Append empty target rows for origin+1..origin+horizon for every series alive at origin."""
    series = sales.where(F.col("sale_date") <= F.to_date(F.lit(origin))).select("item_id", "store_id").distinct()
    days = spark.range(1, horizon + 1).select(F.date_add(F.to_date(F.lit(origin)), F.col("id").cast("int")).alias("sale_date"))
    future = (series.crossJoin(days)
              .withColumn("units", F.lit(None).cast("int"))
              .withColumn("stockout", F.lit(0)))
    return sales.where(F.col("sale_date") <= F.to_date(F.lit(origin))).unionByName(future)


HISTORY_NEEDED_DAYS = 372   # longest look-back (rm_365 ends at t-7)


def build_features(spark: SparkSession, sales: DataFrame, anchor: str = C.HISTORY_END,
                   items: DataFrame | None = None, calendar: DataFrame | None = None,
                   prices: DataFrame | None = None) -> DataFrame:
    """`anchor` is one forecast origin; the weekly origin grid runs through it.
    Dimension tables default to the Hive tables; tests pass small DataFrames instead."""
    db = C.HIVE_DB
    items = items if items is not None else spark.table(f"{db}.items")
    calendar = calendar if calendar is not None else spark.table(f"{db}.calendar")
    prices = prices if prices is not None else spark.table(f"{db}.sell_prices")
    s = sales.withColumn("dnum", F.datediff("sale_date", F.lit("2011-01-01")))
    w = Window.partitionBy("item_id", "store_id").orderBy("dnum")
    rng = lambda lo, hi: w.rangeBetween(lo, hi)
    y = F.col("units").cast("double")

    s = s.select(
        "*",
        F.avg(y).over(rng(-7, -7)).alias("lag_7"),
        F.avg(y).over(rng(-14, -14)).alias("lag_14"),
        F.avg(y).over(rng(-21, -21)).alias("lag_21"),
        F.avg(y).over(rng(-28, -28)).alias("lag_28"),
        F.avg(y).over(rng(-13, -7)).alias("rm_7"),
        F.avg(y).over(rng(-34, -7)).alias("rm_28"),
        F.avg(y).over(rng(-62, -7)).alias("rm_56"),
        F.stddev(y).over(rng(-34, -7)).alias("rstd_28"),
        F.avg((y == 0).cast("double")).over(rng(-34, -7)).alias("zero_share_28"),
    )
    s = s.withColumn("dow_mean_4", (F.coalesce("lag_7", F.lit(0)) + F.coalesce("lag_14", F.lit(0))
                                    + F.coalesce("lag_21", F.lit(0)) + F.coalesce("lag_28", F.lit(0))) / 4)
    s = s.withColumn("rm_365", F.avg(y).over(rng(-371, -7)))

    # As-of-origin features: demand state at the row's forecast origin.
    anchor_dnum = F.datediff(F.to_date(F.lit(anchor)), F.lit("2011-01-01"))
    s = s.withColumn("origin_dnum", anchor_dnum + F.floor((F.col("dnum") - anchor_dnum - 1) / 7) * 7)
    s = s.withColumn("horizon", (F.col("dnum") - F.col("origin_dnum")).cast("double"))
    last_sale = F.max(F.when(y > 0, F.col("dnum"))).over(w.rowsBetween(Window.unboundedPreceding, 0))
    state = s.where(F.col("units").isNotNull()).select(
        "item_id", "store_id", F.col("dnum").alias("origin_dnum"),
        y.alias("o_last"),
        F.avg(y).over(rng(-6, 0)).alias("o_rm_7"),
        F.avg(y).over(rng(-27, 0)).alias("o_rm_28"),
        (F.col("dnum") - last_sale).cast("double").alias("o_days_since_sale"))
    s = s.join(state, ["item_id", "store_id", "origin_dnum"], "left")

    s = s.join(F.broadcast(items.select("item_id", "dept_id", "cat_id")), "item_id", "left")

    # Department x store demand level, lagged the same way (an aggregate signal for sparse SKUs).
    dept_daily = (s.groupBy("dept_id", "store_id", "dnum").agg(F.sum(y).alias("dept_units")))
    wd = Window.partitionBy("dept_id", "store_id").orderBy("dnum")
    dept_daily = dept_daily.withColumn("dept_store_rm_28", F.avg("dept_units").over(wd.rangeBetween(-34, -7))).drop("dept_units")
    s = s.join(dept_daily, ["dept_id", "store_id", "dnum"], "left")

    cal = calendar.select(
        F.col("date").alias("sale_date"), "wm_yr_wk", "snap_CA", "snap_TX", "snap_WI",
        "event_type_1", "event_type_2")
    s = s.join(F.broadcast(cal), "sale_date", "left")
    s = s.withColumn("snap", F.when(F.col("store_id").startswith("CA"), F.col("snap_CA"))
                            .when(F.col("store_id").startswith("TX"), F.col("snap_TX"))
                            .otherwise(F.col("snap_WI")).cast("double"))
    for et in ["Sporting", "Cultural", "National", "Religious"]:
        s = s.withColumn(f"event_{et.lower()}", ((F.col("event_type_1") == et) | (F.col("event_type_2") == et)).cast("double"))
    s = s.fillna(0.0, subset=[f"event_{et}" for et in ["sporting", "cultural", "national", "religious"]])
    s = s.withColumn("has_event", F.greatest("event_sporting", "event_cultural", "event_national", "event_religious"))

    prices = prices.select("store_id", "item_id", "wm_yr_wk", "sell_price")
    s = s.join(prices, ["store_id", "item_id", "wm_yr_wk"], "left")
    s = s.withColumn("price", F.last("sell_price", ignorenulls=True).over(w.rowsBetween(Window.unboundedPreceding, 0)))
    s = s.withColumn("_max_price", F.max("price").over(w.rowsBetween(Window.unboundedPreceding, 0)))
    s = s.withColumn("price_rel_max", F.col("price") / F.col("_max_price"))
    s = s.withColumn("price_change_7", F.col("price") / F.avg("price").over(rng(-7, -7)) - 1)
    s = s.withColumn("markdown", (F.col("price_rel_max") < 0.95).cast("double"))

    s = (s.withColumn("dow", F.dayofweek("sale_date").cast("double"))
          .withColumn("month", F.month("sale_date").cast("double"))
          .withColumn("day_of_month", F.dayofmonth("sale_date").cast("double")))

    dept_codes = {d: i for i, d in enumerate(["FOODS_1", "FOODS_2", "FOODS_3", "HOBBIES_1", "HOBBIES_2", "HOUSEHOLD_1", "HOUSEHOLD_2"])}
    cat_codes = {"FOODS": 0, "HOBBIES": 1, "HOUSEHOLD": 2}
    store_codes = {st: i for i, st in enumerate(["CA_1", "CA_2", "CA_3", "CA_4", "TX_1", "TX_2", "TX_3", "WI_1", "WI_2", "WI_3"])}
    as_map = lambda d: F.create_map(*[F.lit(x) for kv in d.items() for x in kv])
    s = (s.withColumn("dept_idx", as_map(dept_codes)[F.col("dept_id")].cast("double"))
          .withColumn("cat_idx", as_map(cat_codes)[F.col("cat_id")].cast("double"))
          .withColumn("store_idx", as_map(store_codes)[F.col("store_id")].cast("double")))

    out = s.select(*KEYS, "dept_id", "cat_id", F.col("units").cast("double").alias("y"), "stockout", *FEATURES)
    return out.fillna(1.0, subset=["price_rel_max"]).fillna(0.0, subset=[f for f in FEATURES if f != "price_rel_max"])
