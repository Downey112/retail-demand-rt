"""Forecasting models: MLlib global models plus per-series statistical models via applyInPandas."""
import json
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rdf import config as C
from rdf.features import FEATURES

REGISTRY = C.MODELS / "registry.json"


def mllib_pipeline(kind: str) -> Pipeline:
    assembler = VectorAssembler(inputCols=FEATURES, outputCol="features", handleInvalid="keep")
    if kind == "gbt":
        est = GBTRegressor(labelCol="y", maxIter=60, maxDepth=6, stepSize=0.1,
                           subsamplingRate=0.5, featureSubsetStrategy="0.7", seed=C.SEED)
    elif kind == "rf":
        est = RandomForestRegressor(labelCol="y", numTrees=40, maxDepth=10, subsamplingRate=0.3,
                                    featureSubsetStrategy="onethird", seed=C.SEED)
    else:
        raise ValueError(kind)
    return Pipeline(stages=[assembler, est])


def fit(kind: str, train: DataFrame, sample: float | None = None) -> PipelineModel:
    data = train.where(F.col("y").isNotNull() & (F.col("stockout") == 0))
    if sample:
        data = data.sample(fraction=sample, seed=C.SEED)
    return mllib_pipeline(kind).fit(data)


def predict(model: PipelineModel, df: DataFrame, name: str) -> DataFrame:
    return (model.transform(df)
            .withColumn("yhat", F.greatest(F.col("prediction"), F.lit(0.0)))
            .drop("features", "prediction")
            .withColumn("model", F.lit(name)))


# ---------- per-series statistical models (run with groupBy().applyInPandas) ----------

STAT_SCHEMA = "item_id string, store_id string, origin date, sale_date date, yhat double, model string"


def sba_forecast(y: np.ndarray, alpha: float = 0.1) -> float:
    """Syntetos-Boylan approximation of Croston's method: flat per-day rate for intermittent demand."""
    nz = np.flatnonzero(y > 0)
    if len(nz) == 0:
        return 0.0
    z, p, last = y[nz[0]], float(nz[0] + 1), nz[0]
    for i in nz[1:]:
        z = z + alpha * (y[i] - z)
        p = p + alpha * ((i - last) - p)
        last = i
    return float((1 - alpha / 2) * z / p)


def make_stat_forecaster(origins: list[str], horizon: int, arima_ids: set | None):
    """Returns a pandas function: for each origin, fit on history up to it and forecast `horizon` days."""
    def run(pdf: pd.DataFrame) -> pd.DataFrame:
        from statsmodels.tsa.arima.model import ARIMA
        pdf = pdf.sort_values("sale_date")
        item, store = pdf["item_id"].iat[0], pdf["store_id"].iat[0]
        dates = pd.to_datetime(pdf["sale_date"])
        out = []
        for o in origins:
            o_ts = pd.Timestamp(o)
            hist = pdf[dates <= o_ts]
            if hist.empty:
                continue
            series = (hist.set_index(pd.to_datetime(hist["sale_date"]))["units"].astype(float)
                      .asfreq("D", fill_value=0.0))
            fdates = pd.date_range(o_ts + pd.Timedelta(days=1), periods=horizon)
            base = {"item_id": item, "store_id": store, "origin": o_ts.date()}
            sba = sba_forecast(series.values[-365:])
            ma28 = float(series.values[-28:].mean())
            for d in fdates:
                out.append({**base, "sale_date": d.date(), "yhat": sba, "model": "sba_croston"})
                out.append({**base, "sale_date": d.date(), "yhat": ma28, "model": "ma28"})
            if arima_ids is not None and f"{item}|{store}" in arima_ids:
                yhat = _arima(series.iloc[-365:], fdates, ARIMA)
                for d, v in zip(fdates, yhat):
                    out.append({**base, "sale_date": d.date(), "yhat": float(v), "model": "arima"})
        return pd.DataFrame(out, columns=["item_id", "store_id", "origin", "sale_date", "yhat", "model"])
    return run


def _dow_dummies(idx):
    return pd.get_dummies(idx.dayofweek, prefix="d", drop_first=True).astype(float).set_axis(idx)


def _arima(series: pd.Series, fdates, ARIMA) -> np.ndarray:
    """ARIMA(1,0,1) with day-of-week regressors; falls back to the recent mean if fitting fails."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            exog = _dow_dummies(series.index)
            res = ARIMA(series, exog=exog, order=(1, 0, 1)).fit(method_kwargs={"maxiter": 50})
            fc = res.forecast(len(fdates), exog=_dow_dummies(fdates).reindex(columns=exog.columns, fill_value=0.0))
        return np.clip(np.asarray(fc), 0, None)
    except Exception:
        return np.full(len(fdates), max(series.iloc[-28:].mean(), 0.0))


# ---------- simple file-based model registry ----------

def load_registry() -> dict:
    if REGISTRY.exists():
        return json.loads(REGISTRY.read_text())
    return {"champion": None, "versions": []}


def register(model: PipelineModel, kind: str, metrics: dict, trained_through: str, promote: bool) -> str:
    reg = load_registry()
    version = f"{kind}-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}"
    path = C.MODELS / version
    model.write().overwrite().save(path.as_uri())
    reg["versions"].append({"version": version, "kind": kind, "path": str(path), "metrics": metrics,
                            "trained_through": trained_through, "created": time.time()})
    if promote or reg["champion"] is None:
        reg["champion"] = version
    C.MODELS.mkdir(parents=True, exist_ok=True)
    tmp = REGISTRY.with_suffix(".tmp")
    tmp.write_text(json.dumps(reg, indent=2))
    tmp.replace(REGISTRY)
    return version


def load_champion() -> tuple[str, PipelineModel]:
    reg = load_registry()
    v = next(x for x in reg["versions"] if x["version"] == reg["champion"])
    return v["version"], PipelineModel.load(Path(v["path"]).as_uri())
