"""Forecast refresh and replenishment policy, shared by the batch pipeline and the stream."""
from datetime import date, timedelta

import pandas as pd
from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rdf import config as C
from rdf import inventory, serving
from rdf.features import HISTORY_NEEDED_DAYS, add_future_spine, build_features, sales_source
from rdf.models import predict


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
    spark.createDataFrame(fc).write.mode("append").saveAsTable(f"{db}.forecasts")
    spark.createDataFrame(pol).write.mode("overwrite").saveAsTable(f"{db}.inventory_policy")
    serving.write(fc, "forecasts", append=True, key=["item_id", "store_id", "sale_date", "origin"])
    serving.write(pol, "inventory_policy")
