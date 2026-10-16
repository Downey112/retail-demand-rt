"""Summarise a finished replay from the serving layer into docs/results.md and figures."""
import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

from rdf import config as C
from rdf import serving

OUT = C.ROOT / "docs"
BLUE, ORANGE, GREY = "#2a78d6", "#eb6834", "#898781"


def style(ax, title, ylabel=None):
    ax.set_title(title, loc="left", fontsize=11, color="#0b0b0b")
    ax.spines[["top", "right"]].set_visible(False)
    ax.spines[["left", "bottom"]].set_color("#c3c2b7")
    ax.tick_params(colors="#52514e", labelsize=8)
    ax.grid(axis="y", color="#e1e0d9", lw=0.6)
    if ylabel:
        ax.set_ylabel(ylabel, color="#52514e", fontsize=9)


def main():
    dvf = serving.read("demand_vs_forecast")
    acc = serving.read("accuracy_daily")
    inv = serving.read("inventory_daily")
    alerts = serving.read("alerts")
    mm = serving.read("model_metrics")
    me = serving.read("model_events")
    sku = serving.read("sku_actual_vs_forecast")
    ph = serving.read("pipeline_health")

    fig, axes = plt.subplots(1, 3, figsize=(15, 3.8))
    g = dvf.groupby("business_date")[["actual", "forecast"]].sum().reset_index()
    x = pd.to_datetime(g.business_date)
    axes[0].plot(x, g.actual, color=BLUE, lw=2, marker="o", ms=4, label="Actual units")
    axes[0].plot(x, g.forecast, color=ORANGE, lw=2, ls="--", marker="D", ms=4, label="Next-day forecast")
    axes[0].legend(frameon=False, fontsize=8)
    axes[0].set_ylim(bottom=0)
    style(axes[0], "Daily demand vs forecast, 4 CA stores (live replay)", "units")
    axes[0].tick_params(axis="x", rotation=30)

    h = acc.assign(err=acc.wape * acc.actual).groupby("horizon")[["err", "actual"]].sum()
    axes[1].bar(h.index, h.err / h.actual, color=BLUE, width=0.6)
    for i, v in zip(h.index, h.err / h.actual):
        axes[1].text(i, v + 0.01, f"{v:.0%}", ha="center", fontsize=8, color="#52514e")
    axes[1].set_xlabel("days between forecast and sale", fontsize=9, color="#52514e")
    style(axes[1], "SKU-level WAPE by lead time", "WAPE")

    i = inv.groupby("business_date")[["units_sold", "lost_units", "stockout", "below_rop"]].sum().reset_index()
    xi = pd.to_datetime(i.business_date)
    axes[2].plot(xi, i.stockout, color="#d03b3b", lw=2, marker="o", ms=4, label="Out of stock")
    axes[2].plot(xi, i.below_rop, color="#ec835a", lw=2, marker="s", ms=4, label="Below reorder point")
    axes[2].legend(frameon=False, fontsize=8)
    axes[2].set_ylim(bottom=0)
    style(axes[2], "Inventory status at close (SKU-stores)", "count")
    axes[2].tick_params(axis="x", rotation=30)
    fig.tight_layout()
    fig.savefig(OUT / "replay_summary.png", dpi=150)

    total_sold, total_lost = inv.units_sold.sum(), inv.lost_units.sum()
    fill = lambda d: d.units_sold.sum() / (d.units_sold.sum() + d.lost_units.sum())
    day_no = (pd.to_datetime(inv.business_date) - pd.to_datetime(inv.business_date).min()).dt.days
    weekly = inv.groupby(day_no // 7 + 1).apply(fill)
    weekend = pd.to_datetime(inv.business_date).dt.dayofweek >= 5
    h1 = sku.copy()
    live_wape = (h1.units - h1.yhat).abs().sum() / h1.units.sum()
    live_bias = h1.yhat.sum() / h1.units.sum() - 1
    lines = [
        "# Replay results",
        "",
        f"Replay of {dvf.business_date.nunique()} held-out days ({dvf.business_date.min()} to {dvf.business_date.max()}), "
        f"stores {', '.join(sorted(dvf.store_id.unique()))}.",
        "",
        "## Backtest (last 4 weeks of history)",
        "",
        mm.round(3).to_markdown(index=False),
        "",
        "## Live replay",
        "",
        f"* Next-day forecast, SKU level: WAPE **{live_wape:.3f}**, bias **{live_bias:+.1%}** "
        f"over {len(h1):,} SKU-store-days.",
        f"* Units sold {total_sold:,.0f}, lost to stock-outs {total_lost:,.0f}: fill rate **{total_sold / (total_sold + total_lost):.2%}**.",
        f"* Fill rate by replay week: {', '.join(f'week {w} {v:.1%}' for w, v in weekly.items())}; "
        f"weekdays {fill(inv[~weekend]):.1%}, weekends {fill(inv[weekend]):.1%}. Week 1 runs on the simulated "
        f"opening stock; later weeks show the policy on its own.",
        f"* SKU-stores out of stock at close: mean {inv.groupby('business_date').stockout.sum().mean():.0f} per day "
        f"(of {serving.read('inventory_policy').shape[0]:,}).",
        f"* Alerts raised: " + ", ".join(f"{k} {v:,}" for k, v in alerts.alert.value_counts().items()) + ".",
        f"* Micro-batches: {len(ph):,}; median batch time {ph.seconds.median():.1f}s; "
        f"95th percentile event age when processed {ph.max_event_age_s.quantile(0.95):.0f}s.",
        "",
        "## Retraining events",
        "",
        me.round(3).to_markdown(index=False) if not me.empty else "None.",
        "",
        "![replay summary](replay_summary.png)",
    ]
    (OUT / "results.md").write_text("\n".join(lines), encoding="utf-8")
    print("\n".join(lines))


if __name__ == "__main__":
    main()
