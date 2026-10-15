# Connecting Power BI to the serving layer

The stream processor keeps the serving layer in `lake/serving/` as small Parquet files. Each one is replaced atomically, so a refresh never reads a half-written file. Streamlit reads these files, and Power BI can read them too.

| File | Grain | Use |
|---|---|---|
| `demand_vs_forecast.parquet` | day × store × department | actual units vs next-day forecast |
| `accuracy_daily.parquet` | day × store × horizon (1-7) | WAPE by lead time |
| `sku_actual_vs_forecast.parquet` | day × SKU × store | drill-through |
| `inventory_daily.parquet` | day × store | stock-outs, below-ROP and overstock counts, fill rate, inventory value |
| `inventory_health.parquet` | SKU × store (latest close) | on hand, reorder point, days of cover, status |
| `inventory_policy.parquet` | SKU × store (latest) | service level, safety stock, reorder point, EOQ |
| `alerts.parquet` | one row per alert | alert feed |
| `intraday_sku.parquet`, `intraday_hourly.parquet` | current business day | live view |
| `model_metrics.parquet`, `model_events.parquet` | model | backtest and retraining history |

## Steps (Power BI Desktop)

1. **Get data → Parquet**, then paste the full path of a file, e.g. `C:\Users\courts\retail-demand-rt\lake\serving\demand_vs_forecast.parquet`.
2. Repeat for the files you need. Give each query a `DateFromText(business_date)` date column, and relate the facts to a shared Date table and Store table.
3. Suggested measures:
   ```DAX
   Actual Units = SUM ( demand_vs_forecast[actual] )
   Forecast Units = SUM ( demand_vs_forecast[forecast] )
   Forecast Bias % = DIVIDE ( [Forecast Units], [Actual Units] ) - 1
   Fill Rate = DIVIDE ( SUM ( inventory_daily[units_sold] ),
                        SUM ( inventory_daily[units_sold] ) + SUM ( inventory_daily[lost_units] ) )
   WAPE (h) = DIVIDE ( SUMX ( accuracy_daily, accuracy_daily[wape] * accuracy_daily[actual] ),
                       SUM ( accuracy_daily[actual] ) )
   ```
4. For a near-live report, turn on **automatic page refresh** (Power BI Desktop: Format → Page refresh). For local files this works through a DirectQuery-capable source. With plain Import mode, refresh on a schedule or by hand.

In a production lakehouse the same tables would be Hive/Delta tables behind a Spark Thrift / Databricks SQL endpoint, and Power BI would connect with the Spark connector in DirectQuery. Here the embedded Derby metastore allows only one Spark process at a time, so the Parquet serving files stand in for that endpoint.
