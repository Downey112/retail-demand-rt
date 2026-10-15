"""Spark Structured Streaming processor: Kafka -> bronze -> silver/gold -> serving.

One streaming query reads all three topics. Each micro-batch (foreachBatch):
  * validates events against their contract; bad records go to a quarantine folder,
  * lands raw events in bronze parquet (the HDFS landing zone),
  * refreshes the intraday view: units sold so far today vs forecast, estimated stock on hand,
    and real-time stock-out alerts,
  * when every scope store has closed a day AND the day reconciles with the control totals in the
    day-close message, runs the end-of-day cycle: silver daily sales (Hive), forecast accuracy,
    inventory health and alerts, a refreshed 7-day forecast and replenishment policy, and every
    RETRAIN_EVERY_DAYS a challenger model trained in a background thread (champion/challenger).
"""
import argparse
import json
import threading
import time
import traceback
from datetime import date, timedelta

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

from rdf import config as C
from rdf import inventory, serving
from rdf.features import build_features, sales_source
from rdf.metrics import wape
from rdf.models import fit, load_champion, load_registry, predict, register
from rdf.planning import forecast_at, policy_from_forecast, publish_plan
from rdf.spark import get_spark

S = lambda: StringType()
D = lambda: DoubleType()
SCHEMAS = {
    C.TOPIC_POS: StructType([StructField(n, t()) for n, t in [
        ("event_type", S), ("txn_id", S), ("store_id", S), ("item_id", S), ("qty", D), ("unit_price", D),
        ("channel", S), ("business_date", S), ("event_time", S), ("emitted_at", D)]]),
    C.TOPIC_INVENTORY: StructType([StructField(n, t()) for n, t in [
        ("event_type", S), ("store_id", S), ("item_id", S), ("business_date", S), ("event_time", S),
        ("qty", D), ("on_hand", D), ("on_order", D), ("lost_units", D), ("emitted_at", D)]]),
    C.TOPIC_STORE_EVENTS: StructType([StructField(n, t()) for n, t in [
        ("event_type", S), ("store_id", S), ("business_date", S), ("event_time", S),
        ("pos_lines", lambda: LongType()), ("pos_units", D), ("stock_counts", lambda: LongType()), ("emitted_at", D)]]),
}
CONTRACTS = {
    C.TOPIC_POS: "store_id IS NOT NULL AND item_id IS NOT NULL AND txn_id IS NOT NULL AND business_date IS NOT NULL AND qty > 0",
    C.TOPIC_INVENTORY: "store_id IS NOT NULL AND item_id IS NOT NULL AND business_date IS NOT NULL AND on_hand >= 0",
    C.TOPIC_STORE_EVENTS: "store_id IS NOT NULL AND business_date IS NOT NULL AND pos_lines >= 0",
}
BRONZE = {t: C.LAKE_BRONZE / t.replace("-", "_") for t in SCHEMAS}
STATE_FILE = C.LAKE / "_state" / "processor_state.json"


class Processor:
    def __init__(self, spark):
        self.spark = spark
        self.state = {"closes": {}, "closed_days": [], "last_retrain": C.HISTORY_END, "alerted": []}
        if STATE_FILE.exists():
            self.state.update(json.loads(STATE_FILE.read_text()))
        self.version, self.model = load_champion()
        self.series_error = serving.read("series_error")
        self.retrain_thread: threading.Thread | None = None
        self.lock = threading.Lock()

    # ---------------- helpers ----------------
    def save_state(self):
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps({**self.state, "alerted": sorted(self.alerted)}))
        tmp.replace(STATE_FILE)

    def bronze(self, topic, day=None) -> DataFrame | None:
        if not BRONZE[topic].exists():
            return None
        df = self.spark.read.parquet(BRONZE[topic].as_uri())
        return df.where(F.col("business_date") == day) if day else df

    def reload_champion_if_changed(self):
        reg = load_registry()
        if reg["champion"] != self.version:
            with self.lock:
                self.version, self.model = load_champion()
            print(f"  champion reloaded: {self.version}", flush=True)

    # ---------------- micro-batch ----------------
    def process(self, batch: DataFrame, batch_id: int):
        t0 = time.time()
        batch = batch.persist()
        n_rows = batch.count()
        if n_rows == 0:
            batch.unpersist()
            return
        latency = []
        for topic, schema in SCHEMAS.items():
            raw = batch.where(F.col("topic") == topic)
            parsed = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"),
                                 F.col("partition").alias("_partition"), F.col("offset").alias("_offset"),
                                 F.col("timestamp").alias("_kafka_ts"))
                      .select("e.*", "_partition", "_offset", "_kafka_ts")
                      .withColumn("_batch_id", F.lit(batch_id)))
            good = parsed.where(CONTRACTS[topic])
            bad = parsed.where(f"NOT ({CONTRACTS[topic]}) OR ({CONTRACTS[topic]}) IS NULL")
            good.write.mode("append").partitionBy("business_date").parquet(BRONZE[topic].as_uri())
            n_bad = bad.count()
            if n_bad:
                bad.write.mode("append").parquet((C.LAKE_BRONZE / "_quarantine" / topic).as_uri())
                print(f"  quarantined {n_bad} {topic} events", flush=True)
            lat = good.agg(F.max(F.unix_timestamp() - F.col("emitted_at")).alias("lag")).first()["lag"]
            if lat is not None:
                latency.append(float(lat))
            if topic == C.TOPIC_STORE_EVENTS:
                for r in good.where("event_type = 'day_close'").collect():
                    self.state["closes"][f"{r.store_id}|{r.business_date}"] = {
                        "pos_lines": r.pos_lines, "pos_units": r.pos_units, "stock_counts": r.stock_counts}
        batch.unpersist()

        pos = self.bronze(C.TOPIC_POS)
        seen = set() if pos is None else set(pos.select("business_date").distinct().toPandas()["business_date"])
        open_days = sorted(seen - set(self.state["closed_days"]))
        # Close days strictly in order; the first day that does not reconcile yet gets the intraday view.
        for day in open_days:
            if self.day_reconciled(day):
                self.end_of_day(day)
            else:
                self.intraday(day)
                break
        self.save_state()
        serving.write(pd.DataFrame([{
            "batch_id": batch_id, "processed_at": pd.Timestamp.now(tz="UTC"), "rows": n_rows,
            "seconds": time.time() - t0, "max_event_age_s": max(latency) if latency else np.nan,
            "champion": self.version, "days_closed": len(self.state["closed_days"]),
            "retraining": bool(self.retrain_thread and self.retrain_thread.is_alive())}]),
            "pipeline_health", append=True)
        print(f"batch {batch_id}: {n_rows:,} events in {time.time() - t0:.1f}s", flush=True)

    def day_reconciled(self, day) -> bool:
        closes = {s: self.state["closes"].get(f"{s}|{day}") for s in C.SCOPE_STORES}
        if any(v is None for v in closes.values()):
            return False
        got = (self.bronze(C.TOPIC_POS, day).dropDuplicates(["txn_id"])
               .groupBy("store_id").agg(F.count("*").alias("lines"), F.sum("qty").alias("units")).toPandas()
               .set_index("store_id"))
        counts = (self.bronze(C.TOPIC_INVENTORY, day).where("event_type = 'stock_count'")
                  .dropDuplicates(["store_id", "item_id"]).groupBy("store_id").count().toPandas().set_index("store_id")["count"])
        for s, exp in closes.items():
            lines = int(got["lines"].get(s, 0))
            units = float(got["units"].get(s, 0.0))
            stock = int(counts.get(s, 0))
            if lines != exp["pos_lines"] or abs(units - exp["pos_units"]) > 1e-6 or stock != exp["stock_counts"]:
                print(f"  {day} {s}: waiting ({lines}/{exp['pos_lines']} lines, {stock}/{exp['stock_counts']} stock counts)", flush=True)
                return False
        return True

    # ---------------- intraday (every micro-batch) ----------------
    def intraday(self, day):
        sold = (self.bronze(C.TOPIC_POS, day).dropDuplicates(["txn_id"])
                .withColumn("hour", F.substring("event_time", 12, 2).cast("int"))
                .groupBy("store_id", "item_id", "hour", "channel").agg(F.sum("qty").alias("units"))
                .toPandas())
        fc = serving.read("forecasts")
        if fc.empty:
            return
        fc["sale_date"] = fc["sale_date"].astype(str)
        fc = (fc[fc["sale_date"] == day].sort_values("origin").groupby(["item_id", "store_id"]).tail(1)
              [["item_id", "store_id", "dept_id", "yhat"]])
        sku = sold.groupby(["store_id", "item_id"], as_index=False)["units"].sum()
        sku = fc.merge(sku, on=["store_id", "item_id"], how="left").fillna({"units": 0.0})

        # Stock position: last end-of-day count, plus today's receipts, minus units sold so far.
        inv = self.bronze(C.TOPIC_INVENTORY).where(
            F.col("business_date") >= (date.fromisoformat(day) - timedelta(days=1)).isoformat()).toPandas()
        prev = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
        opening = inv[(inv.event_type == "stock_count") & (inv.business_date == prev)].drop_duplicates(["store_id", "item_id"], keep="last")
        receipts = inv[(inv.event_type == "receipt") & (inv.business_date == day)].groupby(["store_id", "item_id"])["qty"].sum()
        sku = sku.merge(opening[["store_id", "item_id", "on_hand"]], on=["store_id", "item_id"], how="left")
        sku["receipts"] = sku.set_index(["store_id", "item_id"]).index.map(receipts).fillna(0.0)
        sku["on_hand_est"] = (sku["on_hand"].fillna(0) + sku["receipts"] - sku["units"]).clip(lower=0)
        sku["business_date"] = day
        serving.write(sku, "intraday_sku")

        hourly = sold.groupby(["store_id", "hour", "channel"], as_index=False)["units"].sum()
        hourly["business_date"] = day
        serving.write(hourly, "intraday_hourly")

        # Real-time alert: a SKU that the forecast expects to sell has just run out on the shelf.
        out = sku[(sku["on_hand_est"] <= 0) & (sku["yhat"] >= 0.5) & (sku["on_hand"].fillna(0) > 0)]
        new = [f"{day}|{r.store_id}|{r.item_id}" for r in out.itertuples() if f"{day}|{r.store_id}|{r.item_id}|rt" not in self.alerted]
        if new:
            rows = out.assign(key=day + "|" + out.store_id + "|" + out.item_id)
            rows = rows[rows.key.isin(new)]
            self.emit_alerts(pd.DataFrame({
                "business_date": day, "store_id": rows.store_id, "item_id": rows.item_id, "dept_id": rows.dept_id,
                "alert": "STOCKOUT_INTRADAY", "severity": "critical",
                "detail": [f"sold out during the day; forecast {y:.1f} units today" for y in rows.yhat]}))
            self.alerted.update(k + "|rt" for k in new)

    @property
    def alerted(self) -> set:
        if not isinstance(self.state["alerted"], set):
            self.state["alerted"] = set(self.state["alerted"])
        return self.state["alerted"]

    def emit_alerts(self, alerts: pd.DataFrame):
        if alerts.empty:
            return
        alerts = alerts.assign(raised_at=pd.Timestamp.now(tz="UTC"))
        serving.write(alerts, "alerts", append=True)
        print(f"  {len(alerts)} alerts ({', '.join(alerts.alert.unique())})", flush=True)

    # ---------------- end of day ----------------
    def end_of_day(self, day):
        t0 = time.time()
        spark, db = self.spark, C.HIVE_DB
        pos = (self.bronze(C.TOPIC_POS, day).dropDuplicates(["txn_id"])
               .groupBy("store_id", "item_id").agg(F.sum("qty").alias("units"),
                                                   F.sum(F.when(F.col("channel") == "ecom", F.col("qty")).otherwise(0)).alias("ecom_units")))
        counts = (self.bronze(C.TOPIC_INVENTORY, day).where("event_type = 'stock_count'")
                  .dropDuplicates(["store_id", "item_id"]).select("store_id", "item_id", "on_hand", "on_order", "lost_units"))
        silver = (counts.join(pos, ["store_id", "item_id"], "left").fillna(0.0, subset=["units", "ecom_units"])
                  .withColumn("sale_date", F.to_date(F.lit(day)))
                  .withColumn("stockout", ((F.col("lost_units") > 0) | ((F.col("on_hand") <= 0) & (F.col("units") == 0))).cast("int"))
                  .withColumn("units", F.col("units").cast("int")))
        silver.select("item_id", "store_id", "sale_date", "units", "ecom_units", "stockout", "on_hand", "lost_units") \
            .write.mode("append").saveAsTable(f"{db}.sales_stream")
        day_pd = silver.toPandas()

        # Forecast accuracy for this day, by how far ahead the forecast was made.
        fc = serving.read("forecasts")
        fc["sale_date"] = fc["sale_date"].astype(str)
        fc_day = fc[fc["sale_date"] == day].merge(day_pd[["store_id", "item_id", "units", "stockout"]], on=["store_id", "item_id"])
        fc_day["horizon"] = (pd.to_datetime(fc_day["sale_date"]) - pd.to_datetime(fc_day["origin"])).dt.days
        acc = (fc_day.groupby(["store_id", "horizon"])
               .apply(lambda g: pd.Series({"actual": g.units.sum(), "forecast": g.yhat.sum(), "wape": wape(g.units, g.yhat)}),
                      include_groups=False).reset_index())
        acc["business_date"] = day
        serving.write(acc, "accuracy_daily", append=True, key=["business_date", "store_id", "horizon"])
        h1 = fc_day[fc_day["horizon"] == fc_day["horizon"].min()]
        dept = (h1.groupby(["store_id", "dept_id"], as_index=False).agg(actual=("units", "sum"), forecast=("yhat", "sum")))
        dept["business_date"] = day
        serving.write(dept, "demand_vs_forecast", append=True, key=["business_date", "store_id", "dept_id"])
        serving.write(h1[["sale_date", "store_id", "item_id", "dept_id", "units", "yhat", "stockout"]]
                      .rename(columns={"sale_date": "business_date"}), "sku_actual_vs_forecast", append=True,
                      key=["business_date", "store_id", "item_id"])

        # Demand anomaly: a department running far from its forecast.
        dept["dev"] = dept["actual"] / dept["forecast"].clip(lower=1) - 1
        anomalies = dept[(dept["dev"].abs() > 0.25) & (dept["forecast"] > 50)]
        self.emit_alerts(pd.DataFrame({
            "business_date": day, "store_id": anomalies.store_id, "item_id": None, "dept_id": anomalies.dept_id,
            "alert": "DEMAND_ANOMALY", "severity": "warning",
            "detail": [f"actual {a:,.0f} vs forecast {f:,.0f} ({d:+.0%})" for a, f, d in zip(anomalies.actual, anomalies.forecast, anomalies.dev)]}))

        # New forecast and policy from this day's close (the simulator orders against it tomorrow).
        self.reload_champion_if_changed()
        with self.lock:
            model, version = self.model, self.version
        new_fc = forecast_at(spark, day, model, version)
        pol = policy_from_forecast(new_fc, self.series_error)
        publish_plan(spark, new_fc, pol)

        # Inventory health at close, against the new policy.
        h = day_pd.merge(pol[["store_id", "item_id", "dept_id", "price", "mu_daily", "reorder_point", "order_up_to", "service_level"]],
                         on=["store_id", "item_id"], how="inner")
        h["days_of_cover"], h["status"] = inventory.health(h["on_hand"], h["reorder_point"], h["mu_daily"])
        h["days_of_cover"] = h["days_of_cover"].replace(np.inf, np.nan)
        h["business_date"] = day
        serving.write(h, "inventory_health")
        summary = (h.groupby(["store_id", "status"]).size().unstack(fill_value=0)
                   .reindex(columns=["stockout", "below_rop", "overstock", "healthy"], fill_value=0))
        g = h.assign(value=h.on_hand * h.price).groupby("store_id")
        summary["units_sold"] = g["units"].sum()
        summary["lost_units"] = g["lost_units"].sum()
        summary["inventory_value"] = g["value"].sum()
        summary["fill_rate"] = summary["units_sold"] / (summary["units_sold"] + summary["lost_units"]).clip(lower=1)
        summary = summary.reset_index()
        summary["business_date"] = day
        serving.write(summary, "inventory_daily", append=True, key=["business_date", "store_id"])
        risk = h[(h["status"] == "stockout") & (h["mu_daily"] >= 0.5)]
        self.emit_alerts(pd.DataFrame({
            "business_date": day, "store_id": risk.store_id, "item_id": risk.item_id, "dept_id": risk.dept_id,
            "alert": "STOCKOUT", "severity": "critical",
            "detail": [f"0 on hand at close, {o:.0f} on order, forecast {m:.1f}/day" for o, m in zip(risk.on_order, risk.mu_daily)]}))

        self.state["closed_days"].append(day)
        print(f"  day {day} closed in {time.time() - t0:.0f}s: WAPE(h=1) {wape(h1.units, h1.yhat):.3f}, "
              f"fill rate {summary.units_sold.sum() / max(summary.units_sold.sum() + summary.lost_units.sum(), 1):.3f}", flush=True)

        days_since = (date.fromisoformat(day) - date.fromisoformat(self.state["last_retrain"])).days
        if days_since >= C.RETRAIN_EVERY_DAYS and not (self.retrain_thread and self.retrain_thread.is_alive()):
            self.state["last_retrain"] = day
            self.retrain_thread = threading.Thread(target=self.retrain, args=(day,), daemon=True)
            self.retrain_thread.start()

    # ---------------- periodic retraining (background thread) ----------------
    def retrain(self, day):
        """Challenger trained through day-7, compared with the champion on the last 7 streamed days."""
        try:
            t0 = time.time()
            self.spark.sparkContext.setLocalProperty("spark.scheduler.pool", "retrain")
            cutoff = (date.fromisoformat(day) - timedelta(days=7)).isoformat()
            # One year of training rows keeps a retrain inside the weekly cycle on a laptop.
            train_start = (date.fromisoformat(cutoff) - timedelta(days=365)).isoformat()
            start = (date.fromisoformat(train_start) - timedelta(days=380)).isoformat()
            sales = sales_source(self.spark).where(F.col("sale_date").between(F.to_date(F.lit(start)), F.to_date(F.lit(day))))
            feats = build_features(self.spark, sales, anchor=cutoff).where(F.col("sale_date") >= F.to_date(F.lit(train_start))).persist()
            train = feats.where(F.col("sale_date") <= F.to_date(F.lit(cutoff)))
            test = feats.where(F.col("sale_date") > F.to_date(F.lit(cutoff))).where(F.col("stockout") == 0)
            reg = load_registry()
            kind = next(v["kind"] for v in reg["versions"] if v["version"] == reg["champion"])
            challenger = fit(kind, train, sample=0.3)
            with self.lock:
                champ_model, champ_version = self.model, self.version
            cols = ["item_id", "store_id", "sale_date", "y", "yhat"]
            ch = predict(challenger, test, "challenger").select(*cols).toPandas()
            cp = predict(champ_model, test, champ_version).select(*cols).toPandas()
            feats.unpersist()
            w_ch, w_cp = wape(ch.y, ch.yhat), wape(cp.y, cp.yhat)
            promote = w_ch < w_cp
            version = register(challenger, kind, {"wape_last7": w_ch, "champion_wape_last7": w_cp},
                               trained_through=cutoff, promote=promote)
            serving.write(pd.DataFrame([{
                "business_date": day, "challenger": version, "champion": champ_version, "challenger_wape": w_ch,
                "champion_wape": w_cp, "promoted": promote, "seconds": time.time() - t0}]), "model_events", append=True)
            print(f"  retrain {version}: WAPE {w_ch:.3f} vs champion {w_cp:.3f} -> "
                  f"{'promoted' if promote else 'kept champion'} ({time.time() - t0:.0f}s)", flush=True)
        except Exception:
            traceback.print_exc()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--trigger-seconds", type=int, default=10)
    ap.add_argument("--reset", action="store_true", help="clear stream outputs and start from the earliest offsets")
    args = ap.parse_args()
    import shutil
    if args.reset:
        for p in [C.LAKE_BRONZE, C.CHECKPOINTS / "stream", STATE_FILE.parent]:
            shutil.rmtree(p, ignore_errors=True)
        for name in ["intraday_sku", "intraday_hourly", "accuracy_daily", "demand_vs_forecast", "sku_actual_vs_forecast",
                     "alerts", "inventory_health", "inventory_daily", "pipeline_health", "model_events"]:
            (C.LAKE_SERVING / f"{name}.parquet").unlink(missing_ok=True)

    spark = get_spark("stream_processor", memory="4g", cores=6, shuffle_partitions=8)
    spark.conf.set("spark.scheduler.mode", "FAIR")
    if args.reset:
        spark.sql(f"DROP TABLE IF EXISTS {C.HIVE_DB}.sales_stream")
    proc = Processor(spark)
    stream = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", C.KAFKA_BOOTSTRAP)
              .option("subscribe", ",".join(SCHEMAS))
              .option("startingOffsets", "earliest")
              .option("maxOffsetsPerTrigger", 200_000)
              .option("failOnDataLoss", "false")
              .load())
    q = (stream.writeStream.foreachBatch(proc.process)
         .option("checkpointLocation", (C.CHECKPOINTS / "stream").as_uri())
         .trigger(processingTime=f"{args.trigger_seconds} seconds")
         .start())
    print("stream processor running", flush=True)
    q.awaitTermination()


if __name__ == "__main__":
    main()
