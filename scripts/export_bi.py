"""Export the replay's serving tables as small CSVs for the Power BI report and the web dashboard.

    python scripts/export_bi.py

Writes powerbi/data/*.csv (the Power BI semantic model reads these) and
docs/dashboard/data.json (the same numbers for the static web dashboard).
Run it after the replay; it only reads lake/serving.
"""
import json
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SERVING = ROOT / "lake" / "serving"
OUT = ROOT / "powerbi" / "data"
WEB = ROOT / "docs" / "dashboard" / "data.json"


def read(name):
    return pd.read_parquet(SERVING / f"{name}.parquet")


def main():
    OUT.mkdir(parents=True, exist_ok=True)

    # Forecast fact at date x store x dept. WAPE needs SKU-level absolute error,
    # so it is summed here before aggregating away the SKU.
    sku = read("sku_actual_vs_forecast")
    sku["abs_err"] = (sku["units"] - sku["yhat"]).abs()
    forecast = (
        sku.groupby(["business_date", "store_id", "dept_id"], as_index=False)
        .agg(units=("units", "sum"), forecast=("yhat", "sum"), abs_err=("abs_err", "sum"),
             sku_days=("item_id", "size"), stockout_skus=("stockout", "sum"))
    )
    forecast["forecast"] = forecast["forecast"].round(3)
    forecast["abs_err"] = forecast["abs_err"].round(3)

    # Inventory fact at date x store.
    inv = read("inventory_daily")[
        ["business_date", "store_id", "units_sold", "lost_units", "stockout",
         "below_rop", "overstock", "healthy", "inventory_value"]
    ].rename(columns={"stockout": "skus_stockout", "below_rop": "skus_below_rop",
                      "overstock": "skus_overstock", "healthy": "skus_healthy"})

    alerts = read("alerts")[["business_date", "store_id", "dept_id", "item_id", "alert", "severity", "detail"]]
    events = read("model_events")
    events["challenger_wape"] = events["challenger_wape"].round(4)
    events["champion_wape"] = events["champion_wape"].round(4)
    events["seconds"] = events["seconds"].round(0).astype(int)
    backtest = read("model_metrics").round(6)
    pipeline = read("pipeline_health")[["batch_id", "processed_at", "rows", "seconds", "max_event_age_s"]].copy()
    pipeline["processed_at"] = pipeline["processed_at"].dt.strftime("%Y-%m-%d %H:%M:%S")
    pipeline[["seconds", "max_event_age_s"]] = pipeline[["seconds", "max_event_age_s"]].round(2)

    tables = {"fact_forecast": forecast, "fact_inventory": inv, "alerts": alerts,
              "model_events": events, "backtest": backtest, "pipeline": pipeline}
    for name, df in tables.items():
        df.to_csv(OUT / f"{name}.csv", index=False)
        print(f"{name}: {len(df):,} rows")

    # Cross-check against docs/results.md so the dashboard and the report agree.
    wape = forecast["abs_err"].sum() / forecast["units"].sum()
    fill = inv["units_sold"].sum() / (inv["units_sold"].sum() + inv["lost_units"].sum())
    print(f"check: WAPE {wape:.3f}, fill rate {fill:.4f}")

    WEB.parent.mkdir(parents=True, exist_ok=True)
    web = {name: json.loads(df.to_json(orient="split", index=False))
           for name, df in tables.items() if name != "pipeline"}
    web["pipeline"] = {"batches": len(pipeline),
                       "median_seconds": float(pipeline["seconds"].median()),
                       "p95_event_age_s": float(pipeline["max_event_age_s"].quantile(0.95))}
    WEB.write_text(json.dumps(web, separators=(",", ":")), encoding="utf-8")
    print(f"web data: {WEB.stat().st_size // 1024} KB")


if __name__ == "__main__":
    main()
