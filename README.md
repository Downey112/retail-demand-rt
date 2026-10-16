# Real-time retail demand forecasting (Kafka · Spark Streaming · Hive · MLlib)

This project forecasts demand at SKU-store level and keeps the forecast current as sales stream in. It runs on Walmart's **M5** data (3,049 products, 10 stores, 1,941 days). The modelled, streamed and optimised scope is the four California stores, which hold 12,196 SKU-store series.

History up to 2016-05-22 sits in an "operational" retail database and is batch-imported into a data lake. The **28 held-out days after that are never loaded in batch**. A store simulator replays them through Kafka as point-of-sale transactions, inventory movements and end-of-day closes. A Spark Structured Streaming job turns that stream into:

* sales landed in the lake (bronze) and daily sales in Hive (silver),
* a live view of units sold today against the forecast, with real-time stock-out alerts,
* when a day closes: forecast accuracy, inventory health, a refreshed 7-day forecast and a new replenishment policy (safety stock, reorder point, EOQ), which the simulated stores order against the next day,
* every 7 days, a challenger model retrained in the background and promoted only if it beats the champion on the latest week.

A Streamlit dashboard reads the serving layer and refreshes every 5 seconds. The finished replay is also published as a **Power BI report** ([powerbi/RetailDemand.pbip](powerbi/RetailDemand.pbip)) and as a **web dashboard** with the same layout: **<https://downey112.github.io/retail-demand-rt/dashboard/>**. See [docs/power_bi.md](docs/power_bi.md) for the design and the semantic model.

**Web dashboard** (click to open):

[![Web dashboard: fill rate, lost units, stock-outs, forecast WAPE and bias over the last 7 days, daily fill-rate and actual-vs-forecast trends, lost units by store, accuracy by department and the weekly retrain log](docs/dashboard_preview.png)](https://downey112.github.io/retail-demand-rt/dashboard/)

**Power BI report** (overview page in Power BI Desktop):

![Power BI report: KPI cards with 7-day comparisons, daily fill rate, actual vs next-day forecast, lost units by store, forecast accuracy by department and the retrain log](docs/powerbi_preview.png)

## Architecture

```mermaid
flowchart LR
  subgraph Sources
    DB[(Retail ops DB<br/>SQLite, history)]
    SIM[Store simulator<br/>28 held-out days]
  end
  subgraph Kafka["Kafka 3.9 (KRaft)"]
    T1[pos-transactions]
    T2[inventory-updates]
    T3[store-events<br/>day_close + control totals]
  end
  subgraph Lake["Data lake (local FS as HDFS) + Hive metastore"]
    RAW[raw/ JDBC import]
    BR[bronze/ raw events]
    HV[(Hive: sales_history, sales_stream,<br/>features, forecasts, inventory_policy...)]
    SV[serving/ parquet]
    MR[models/ registry]
  end
  DB -- "Spark JDBC, split by store<br/>(Sqoop replacement)" --> RAW --> HV
  SIM --> T1 & T2 & T3
  T1 & T2 & T3 --> SS[Spark Structured Streaming<br/>foreachBatch]
  SS --> BR
  SS -- end of day --> HV
  HV --> ML[Spark SQL features + MLlib<br/>RF / GBT, ARIMA & Croston via applyInPandas]
  ML --> MR
  SS -- "forecast, policy, health, alerts" --> SV
  SV -- latest policy --> SIM
  SV --> ST[Streamlit live dashboard]
  SV --> PBI[Power BI]
```

| Brief | What runs here | Why |
|---|---|---|
| Kafka ingests POS, e-commerce and inventory streams | Apache Kafka 3.9.1, single broker in KRaft mode, three topics | real broker, no mock |
| Sqoop loads historical batch data into HDFS | Spark JDBC import with one split per store, plus a row-count and unit-sum reconciliation (Sqoop's `--validate`) | Sqoop was retired to the Apache Attic in 2021, and Spark JDBC is what replaced it |
| HDFS data lake | local filesystem through Hadoop's `file://` FileSystem API, laid out as raw / bronze / warehouse / serving | no Docker or WSL on this machine (Windows 11 Home, 7.8 GB RAM); the code is unchanged apart from the path scheme |
| Hive tables | Spark's built-in Hive metastore (embedded Derby) and Hive-managed Parquet tables | real Hive catalog. Embedded Derby allows one Spark process at a time, so dashboards read the serving files instead |
| Spark SQL features | window functions: lags, rolling means and spread, zero share, department-store level, price and markdown, SNAP, events | [rdf/features.py](rdf/features.py) |
| ARIMA, RF, GBT in Spark MLlib | MLlib `RandomForestRegressor` and `GBTRegressor` (global models); ARIMA (statsmodels) and Croston/SBA per series, distributed with `groupBy().applyInPandas` | MLlib has no ARIMA, so per-series models run as distributed pandas UDFs |
| EOQ, newsvendor, reorder points | newsvendor critical ratio → service level; negative-binomial lead-time demand → reorder point and safety stock; EOQ order quantity | [rdf/inventory.py](rdf/inventory.py) |
| Live dashboard and alerts | Streamlit dashboard plus an alert feed (real-time stock-outs, close-of-day stock-outs, demand anomalies), CSV export and optional webhook | [app.py](app.py) |

## Design decisions worth knowing

**No look-ahead anywhere.** Every lag feature for day *t* uses sales up to *t*−7. Each row is also assigned a forecast origin *o* on a weekly grid, with horizon *h* = *t*−*o* in 1..7 and "as of origin" features: the last value, 7- and 28-day means ending at *o*, and days since the last sale. One model therefore forecasts 1 to 7 days ahead and still uses yesterday's sales for tomorrow, without copying each training row seven times. A Spark test perturbs future sales and checks that no feature moves ([tests/test_features_spark.py](tests/test_features_spark.py)).

**The day closes only when the data reconciles.** Kafka keeps order within a partition, not across topics, so a `day_close` event can arrive before the last sales of that day. Each close message carries control totals (POS lines and units per store), and the processor acts on a day only once the bronze data matches them. Days close strictly in order.

**Reorder points for intermittent demand.** Most M5 SKU-days are zero. A normal approximation of lead-time demand gives negative or inflated reorder points for slow movers, so lead-time demand is negative binomial: its mean is the forecast and its variance comes from the model's own backtest error. The service level is the newsvendor critical ratio, using margin as the underage cost and holding plus spoilage/markdown as the overage cost, clipped to 80-99%.

**Censored sales.** Recorded sales are capped by stock. The simulator records lost units when a shelf runs empty, and retraining excludes stock-out days so the model doesn't learn that empty shelves mean no demand.

**Retraining never blocks the stream.** The challenger trains in a background thread on the same SparkSession (FAIR scheduler pool) on data up to day−7. It is compared with the champion on the last 7 streamed days, which both models have never seen, and promoted only if its WAPE is lower. The registry is a JSON file, and the stream reloads the champion when it changes.

**Serving files are swapped atomically** (write to temp, then `os.replace`, retried while Windows holds a reader's lock), so the dashboard never reads a half-written file.

## Results

See [docs/results.md](docs/results.md) for the full tables.

<!-- RESULTS -->
**Backtest (last 28 days of training data, 341,488 SKU-store-days).** Random forest is the champion with WAPE 0.710, just ahead of GBT (0.713) and a 28-day moving average (0.723). SBA/Croston scores 0.737 and seasonal naive 0.868. Plain baselines come close because most series are intermittent. On a 300-series sample, ARIMA wins (0.651 vs 0.672 for the random forest), but it does not scale to 12,196 series.

**Live replay (28 days streamed through Kafka at 60 s per business day).**

* Next-day forecast, SKU level: WAPE **0.725**, bias **-6.3%**, close to the backtest.
* Fill rate **99.14%**: 519,047 units sold, 4,521 lost to stock-outs. By week: 99.9%, 98.8%, 98.7%, 99.2%. Week 1 runs on simulated opening stock. Weekdays 99.3%, weekends 98.8%.
* 149 micro-batches, median batch 8.9 s, 95th-percentile event age 79 s.
* Alerts: 1,352 end-of-day stock-outs, 564 intraday stock-outs, 93 demand anomalies.
* Four weekly retrains. Each challenger was within 0.006 WAPE of the champion but never better, so the champion was kept every time.

![replay summary](docs/replay_summary.png)

## Limitations (stated, not hidden)

* **Synthetic parts.** M5 has no stock levels, costs, transactions or e-commerce channel. Opening stock, the cost assumptions ([rdf/config.py](rdf/config.py)), the split of daily units into POS lines and the 15% online share are simulated. The demand itself is real.
* **Single machine.** The cluster pieces (HDFS, a multi-broker Kafka, a Hive metastore service, Spark on YARN or Kubernetes) are replaced by single-node equivalents. The code would move to a cluster by changing paths and the Spark master, not the logic.
* **MLlib vs LightGBM.** MLlib's tree models have no Poisson or Tweedie loss and no native categorical splits, which is a real handicap on zero-heavy retail data. The M5 winners used LightGBM. MLlib is used here because the brief asks for distributed training.
* **At-least-once landing.** `foreachBatch` can replay a micro-batch after a crash. Bronze can then hold duplicates, but every downstream step de-duplicates on `txn_id`, so silver and gold are exactly-once in effect.
* **Idempotent close.** The end-of-day writes are safe to repeat: Hive `sales_stream` and `forecasts` are partitioned by day and each close overwrites its own partition, so a crash mid-close and a replayed batch leave no duplicates.
* **Reorder point ignores the weekly pattern.** The policy uses the *average* daily forecast times the lead time. Weekend demand runs well above that average, so Saturdays and Sundays (2 of 7 days, 30% of units sold) account for about 45% of lost sales. Summing the day-by-day forecast over the lead time would fix this and is the obvious next step. Week 1 of the replay also runs on generous simulated opening stock, so the results report fill rate by week.
* **Data source.** The official M5 files need Kaggle competition rules accepted. This run used the public MIT-licensed parquet mirror `marcozanotti/m5-competition-dataset-parquet` (same data, long format, leading zeros before each product's launch trimmed).

## Run it

Requirements: Python 3.13, a portable JDK 17, `winutils.exe` and `hadoop.dll` (Hadoop 3.3.6 build) in `%HADOOP_HOME%\bin`, and Kafka 3.9.1. The paths are in [rdf/config.py](rdf/config.py).

```bash
python -m venv .venv && .venv/Scripts/pip install -e . pyspark==4.0.4 confluent-kafka pandas pyarrow statsmodels scipy streamlit plotly matplotlib tabulate pytest
kaggle datasets download marcozanotti/m5-competition-dataset-parquet -p data/raw/m5 --unzip

python scripts/build_retail_db.py        # operational DB (history only)      ~7 min
python scripts/ingest_batch.py           # Spark JDBC -> lake -> Hive          ~4 min
python scripts/train.py                  # features, backtest, champion, plan  ~25 min

powershell -File scripts/kafka.ps1 -Format              # terminal 1: broker
python scripts/stream_processor.py --reset              # terminal 2: Spark streaming (restores the starting plan)
python scripts/store_simulator.py --seconds-per-day 90 --wait-for-plan   # terminal 3
streamlit run app.py                                    # terminal 4: dashboard

python scripts/report.py                 # after the replay: docs/results.md + figure
python scripts/export_bi.py              # then: CSVs for Power BI + data for the web dashboard
python -m pytest                         # unit tests + Spark leakage test
```

Each replay is one `stream_processor.py --reset` plus one simulator run. The reset clears the stream outputs, drops models trained during an earlier replay, re-publishes the end-of-history plan and starts reading Kafka after any earlier replay's events. The simulator waits until the reset is ready, so the two can start in either order. On a 6-core, 8 GB laptop the end-of-day cycle takes about 2 minutes (about 5 while a weekly retrain runs in the background), so with `--wait-for-plan` a full 28-day replay takes about 1.5 to 2 hours at `--seconds-per-day` 90 or less. The processor defaults (`--driver-memory 3g`, `--retrain-sample 0.15`) are sized for 8 GB of RAM; leave the dashboard closed during the run if memory is tight.
