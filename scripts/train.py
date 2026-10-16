"""Feature build, backtest, champion selection, final fit and the first replenishment plan.

Backtest design: models are fitted on data up to the first of four weekly origins at the end of
history and evaluated on the 28 days after it, each day forecast from its own weekly origin
(all features lag >= 7, so every prediction only uses data available at its origin).
"""
import argparse
import random
import time
from datetime import date, timedelta

import pandas as pd
from pyspark.sql import functions as F

from rdf import config as C
from rdf import serving
from rdf.features import build_features, sales_source
from rdf.metrics import summarise
from rdf.models import STAT_SCHEMA, fit, load_champion, make_stat_forecaster, predict, register
from rdf.planning import forecast_at, policy_from_forecast, publish_plan, replan_at_history_end
from rdf.spark import get_spark


def log(t0, msg):
    print(f"[{time.time() - t0:6.0f}s] {msg}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sample", type=float, default=0.3, help="share of training rows for MLlib models")
    ap.add_argument("--arima-series", type=int, default=300, help="series sampled for the ARIMA comparison")
    ap.add_argument("--plan-only", action="store_true", help="re-run only the plan with the current champion")
    args = ap.parse_args()

    t0 = time.time()
    spark = get_spark("train", memory="4g")
    db = C.HIVE_DB
    if args.plan_only:
        version, n = replan_at_history_end(spark)
        log(t0, f"plan re-published with {version}: {n:,} policies")
        return
    end = date.fromisoformat(C.HISTORY_END)
    origins = [(end - timedelta(days=7 * k)).isoformat() for k in range(C.VALIDATION_ORIGINS, 0, -1)]
    first_origin = origins[0]
    feat_start = (date.fromisoformat(C.TRAIN_START) - timedelta(days=380)).isoformat()

    # 1. Features (Spark SQL) -> Hive
    sales = sales_source(spark).where(F.col("sale_date") >= F.to_date(F.lit(feat_start)))
    feats = build_features(spark, sales).where(F.col("sale_date") >= F.to_date(F.lit(C.TRAIN_START)))
    feats.write.mode("overwrite").partitionBy("store_id").saveAsTable(f"{db}.features")
    feats = spark.table(f"{db}.features")
    log(t0, f"features: {feats.count():,} rows")

    # RMSSE scale per series: mean squared one-step change before the first origin.
    from pyspark.sql import Window
    w = Window.partitionBy("item_id", "store_id").orderBy("sale_date")
    scale = (sales.where(F.col("sale_date") <= F.to_date(F.lit(first_origin)))
             .withColumn("d", F.col("units") - F.lag("units").over(w))
             .groupBy("item_id", "store_id").agg(F.avg(F.col("d") ** 2).alias("scale")))

    train = feats.where(F.col("sale_date") <= F.to_date(F.lit(first_origin)))
    test = (feats.where(F.col("sale_date") > F.to_date(F.lit(first_origin)))
            .withColumn("origin", F.date_add(F.to_date(F.lit(first_origin)),
                        (F.floor((F.datediff("sale_date", F.to_date(F.lit(first_origin))) - 1) / 7) * 7).cast("int"))))

    # 2. MLlib global models
    preds = []
    for kind in ["gbt", "rf"]:
        m = fit(kind, train, sample=args.sample)
        preds.append(predict(m, test, kind).select("item_id", "store_id", "origin", "sale_date", "y", "yhat", "model"))
        log(t0, f"{kind} fitted")
    preds.append(test.select("item_id", "store_id", "origin", "sale_date", "y",
                             F.col("lag_7").alias("yhat"), F.lit("seasonal_naive").alias("model")))

    # 3. Per-series statistical models with applyInPandas (SBA/Croston, MA28 on all; ARIMA on a sample)
    series = sorted(f"{r.item_id}|{r.store_id}" for r in feats.select("item_id", "store_id").distinct().collect())
    random.Random(C.SEED).shuffle(series)
    arima_ids = set(series[: args.arima_series])
    stat_hist_start = (date.fromisoformat(first_origin) - timedelta(days=400)).isoformat()
    stat = (sales.where(F.col("sale_date").between(F.to_date(F.lit(stat_hist_start)), F.to_date(F.lit(origins[-1]))))
            .select("item_id", "store_id", "sale_date", "units")
            .repartition(48, "item_id", "store_id")
            .groupBy("item_id", "store_id")
            .applyInPandas(make_stat_forecaster(origins, C.HORIZON, arima_ids), schema=STAT_SCHEMA))
    actual = test.select("item_id", "store_id", "sale_date", "y")
    preds.append(stat.join(actual, ["item_id", "store_id", "sale_date"]).select("item_id", "store_id", "origin", "sale_date", "y", "yhat", "model"))

    allp = preds[0]
    for p in preds[1:]:
        allp = allp.unionByName(p)
    allp = allp.join(scale, ["item_id", "store_id"], "left")
    allp.write.mode("overwrite").saveAsTable(f"{db}.backtest_predictions")
    bt = spark.table(f"{db}.backtest_predictions").toPandas()
    log(t0, f"backtest predictions: {len(bt):,} rows")

    # 4. Metrics: all scope series, and the ARIMA sample where every model is compared like for like
    full = summarise(bt[bt.model != "arima"]).assign(subset="all_series")
    key = bt["item_id"] + "|" + bt["store_id"]
    sample = summarise(bt[key.isin(arima_ids)]).assign(subset=f"arima_sample_{len(arima_ids)}")
    metrics = pd.concat([full, sample], ignore_index=True)
    print(metrics.to_string(index=False))
    spark.createDataFrame(metrics).write.mode("overwrite").saveAsTable(f"{db}.model_metrics")
    serving.write(metrics, "model_metrics")

    # 5. Champion among deployable global models; refit on all history and register.
    ml = full[full.model.isin(["gbt", "rf"])].sort_values("wape")
    best = ml.iloc[0]
    champion = fit(best.model, feats, sample=args.sample)
    version = register(champion, best.model, {k: float(best[k]) for k in ["wape", "rmse", "rmsse", "bias"]},
                       trained_through=C.HISTORY_END, promote=True)
    log(t0, f"champion {version} (backtest WAPE {best.wape:.3f})")

    # 6. Per-series forecast error of the champion feeds the safety stock.
    err = (bt[bt.model == best.model].assign(e=lambda d: d.y - d.yhat)
           .groupby(["item_id", "store_id"]).agg(sigma_daily=("e", "std"), mae=("e", lambda x: x.abs().mean()))
           .reset_index())
    spark.createDataFrame(err).write.mode("overwrite").saveAsTable(f"{db}.series_error")
    serving.write(err, "series_error")

    # 7. First plan at the end of history: next-7-day forecast and replenishment policy.
    fc = forecast_at(spark, C.HISTORY_END, champion, version)
    pol = policy_from_forecast(fc, err)
    spark.sql(f"DROP TABLE IF EXISTS {db}.forecasts")
    serving.write(fc.iloc[:0], "forecasts")
    publish_plan(spark, fc, pol)
    log(t0, f"plan published: {len(fc):,} forecasts, {len(pol):,} policies")


if __name__ == "__main__":
    main()
