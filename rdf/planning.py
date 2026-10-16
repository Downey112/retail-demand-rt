"""Forecast refresh and replenishment policy, shared by the batch pipeline and the stream."""
from datetime import date, timedelta

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rdf import config as C
from rdf import inventory, serving
from rdf.features import HISTORY_NEEDED_DAYS, add_future_spine, build_features, sales_source
from rdf.models import load_champion, predict, reset_to_batch_champion
from rdf.spark import write_partitioned


def forecast_at(spark: SparkSession, origin: str, model: PipelineModel, version: str) -> pd.DataFrame:
    """Score origin+1..origin+HORIZON for every scope series using only data up to origin."""
    start = (date.fromisoformat(origin) - timedelta(days=HISTORY_NEEDED_DAYS + 7)).isoformat()
    sales = sales_source(spark).where(F.col("sale_date") >= F.to_date(F.lit(start)))
    feats = build_features(spark, add_future_spine(spark, sales, origin), anchor=origin)
    future = feats.where(F.col("sale_date") > F.to_date(F.lit(origin)))
    fc = (predict(model, future, version)
          .select("item_id", "store_id", "cat_id", "dept_id", "sale_date", "yhat", "price")
          .toPandas())
    fc["origin"] = pd.Timestamp(origin).date()
    fc["model_version"] = version
    return fc


def policy_from_forecast(fc: pd.DataFrame, series_error: pd.DataFrame) -> pd.DataFrame:
    agg = (fc.groupby(["item_id", "store_id", "cat_id", "dept_id"], observed=True)
             .agg(mu_daily=("yhat", "mean"), price=("price", "last")).reset_index())
    agg = agg.merge(series_error[["item_id", "store_id", "sigma_daily"]], on=["item_id", "store_id"], how="left")
    # Series with no backtest error (new items) fall back to a Poisson-like spread.
    agg["sigma_daily"] = agg["sigma_daily"].fillna(agg["mu_daily"].clip(lower=0) ** 0.5)
    pol = inventory.build_policy(agg)
    pol["origin"] = fc["origin"].iat[0]
    return pol


def publish_plan(spark: SparkSession, fc: pd.DataFrame, pol: pd.DataFrame):
    """Store forecasts and policy in Hive (history) and in the serving layer (latest)."""
    db = C.HIVE_DB
    write_partitioned(spark, spark.createDataFrame(fc), f"{db}.forecasts", "origin")
    spark.createDataFrame(pol).write.mode("overwrite").saveAsTable(f"{db}.inventory_policy")
    serving.write(fc, "forecasts", append=True, key=["item_id", "store_id", "sale_date", "origin"])
    serving.write(pol, "inventory_policy")


def replan_at_history_end(spark: SparkSession) -> tuple[str, int]:
    """Restore the starting point of a replay: batch champion, and only the plan made at HISTORY_END."""
    reset_to_batch_champion()          # a model trained on streamed days would leak them into this plan
    version, model = load_champion()
    fc = forecast_at(spark, C.HISTORY_END, model, version)
    pol = policy_from_forecast(fc, serving.read("series_error"))
    spark.sql(f"DROP TABLE IF EXISTS {C.HIVE_DB}.forecasts")
    serving.write(fc.iloc[:0], "forecasts")
    publish_plan(spark, fc, pol)
    return version, len(pol)
