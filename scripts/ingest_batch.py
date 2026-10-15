"""Batch import from the operational DB into the lake (the job Sqoop used to do).

Spark JDBC reads each table in parallel (daily_sales is split into one partition per store,
like `sqoop import --split-by store_id`), lands it as raw parquet, and registers curated Hive
tables on top.
"""
import time

from pyspark.sql import functions as F

from rdf import config as C
from rdf.spark import get_spark

JDBC_URL = f"jdbc:sqlite:{C.RETAIL_DB.as_posix()}"


def read_table(spark, table, predicates=None):
    reader = spark.read.option("driver", "org.sqlite.JDBC").option("fetchsize", 50_000)
    if predicates:
        return reader.jdbc(JDBC_URL, table, predicates=predicates)
    return reader.jdbc(JDBC_URL, table)


def main():
    t0 = time.time()
    spark = get_spark("ingest_batch")
    ingest_ts = F.current_timestamp()
    db = C.HIVE_DB

    stores = [r[0] for r in read_table(spark, "stores").select("store_id").collect()]
    tables = {
        "items": read_table(spark, "items"),
        "stores": read_table(spark, "stores"),
        "calendar": read_table(spark, "calendar"),
        "sell_prices": read_table(spark, "sell_prices"),
        "daily_sales": read_table(spark, "daily_sales", [f"store_id = '{s}'" for s in stores]),
    }
    for name, df in tables.items():
        path = (C.LAKE_RAW / "retail_db" / name).as_uri()
        df.withColumn("_ingest_ts", ingest_ts).write.mode("overwrite").parquet(path)
        print(f"raw {name}: landed ({time.time() - t0:.0f}s)")

    raw = lambda name: spark.read.parquet((C.LAKE_RAW / "retail_db" / name).as_uri())

    raw("items").drop("_ingest_ts").write.mode("overwrite").saveAsTable(f"{db}.items")
    raw("stores").drop("_ingest_ts").write.mode("overwrite").saveAsTable(f"{db}.stores")
    (raw("calendar").drop("_ingest_ts")
        .withColumn("date", F.to_date("date"))
        .write.mode("overwrite").saveAsTable(f"{db}.calendar"))
    (raw("sell_prices").drop("_ingest_ts")
        .withColumn("sell_price", F.col("sell_price").cast("double"))
        .write.mode("overwrite").partitionBy("store_id").saveAsTable(f"{db}.sell_prices"))
    (raw("daily_sales").drop("_ingest_ts")
        .withColumn("sale_date", F.to_date("sale_date"))
        .withColumn("units", F.col("units").cast("int"))
        .write.mode("overwrite").partitionBy("store_id").saveAsTable(f"{db}.sales_history"))

    # Reconcile row counts between source and lake, as a Sqoop --validate would.
    src = read_table(spark, "(SELECT store_id, COUNT(*) AS n, SUM(units) AS u FROM daily_sales GROUP BY store_id) t")
    lake = spark.table(f"{db}.sales_history").groupBy("store_id").agg(F.count("*").alias("n"), F.sum("units").alias("u"))
    bad = src.join(lake, "store_id", "full").where((src.n != lake.n) | (src.u != lake.u) | src.n.isNull() | lake.n.isNull())
    assert bad.count() == 0, bad.collect()
    for t in ["items", "stores", "calendar", "sell_prices", "sales_history"]:
        print(f"{db}.{t}: {spark.table(f'{db}.{t}').count():,} rows")
    print(f"ingest done in {time.time() - t0:.0f}s, source and lake reconcile")


if __name__ == "__main__":
    main()
