"""Live dashboard: demand vs forecast, inventory health and alerts, read from the serving layer.

Run with: streamlit run app.py   (refreshes itself every few seconds while the stream runs)
"""
import json
import os
import urllib.request

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from rdf import serving

st.set_page_config(page_title="Live Retail Demand", layout="wide")

INK, INK2, MUTED, GRID = "#0b0b0b", "#52514e", "#898781", "#e1e0d9"
ACTUAL, FORECAST, ECOM = "#2a78d6", "#eb6834", "#1baf7a"
STATUS = {"stockout": "#d03b3b", "below_rop": "#ec835a", "overstock": "#fab219", "healthy": "#0ca30c"}
STATUS_LABEL = {"stockout": "Stock-out", "below_rop": "Below reorder point", "overstock": "Overstock (>60 days)", "healthy": "Healthy"}
SEVERITY_ICON = {"critical": "🔴", "warning": "🟠", "info": "🔵"}

st.markdown("""
<style>
  .block-container {padding-top: 1.6rem; max-width: 1400px;}
  [data-testid="stMetricValue"] {font-size: 1.7rem;}
  h1 {font-size: 1.6rem !important; margin-bottom: 0 !important;}
  .sub {color: #52514e; font-size: 0.92rem; margin-bottom: 0.8rem;}
</style>""", unsafe_allow_html=True)


def layout(fig, title, height=300, ytitle=None):
    fig.update_layout(
        title=dict(text=title, font=dict(size=15, color=INK), x=0, xanchor="left"),
        height=height, margin=dict(l=8, r=8, t=44, b=8), plot_bgcolor="#fcfcfb", paper_bgcolor="#fcfcfb",
        font=dict(color=INK2, size=12), hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.0, xanchor="right", x=1, font=dict(size=12)))
    fig.update_xaxes(showgrid=False, linecolor="#c3c2b7", tickfont=dict(color=MUTED))
    fig.update_yaxes(gridcolor=GRID, zeroline=False, tickfont=dict(color=MUTED), title=ytitle, rangemode="tozero")
    return fig


def load():
    names = ["pipeline_health", "demand_vs_forecast", "accuracy_daily", "intraday_hourly", "intraday_sku",
             "inventory_daily", "inventory_health", "alerts", "model_metrics", "model_events", "inventory_policy"]
    return {n: serving.read(n) for n in names}


st.markdown("<h1>Live retail demand: forecast vs actual</h1>", unsafe_allow_html=True)
st.markdown('<div class="sub">Walmart M5, California stores. The 28 held-out days are replayed through Kafka; '
            'Spark Structured Streaming updates forecasts, replenishment policy and alerts as each day closes.</div>',
            unsafe_allow_html=True)

data0 = load()
all_stores = sorted(data0["inventory_policy"]["store_id"].unique()) if not data0["inventory_policy"].empty else []
c1, c2 = st.columns([3, 1])
stores = c1.multiselect("Stores", all_stores, default=all_stores, label_visibility="collapsed")
live = c2.toggle("Live refresh (5 s)", value=True)


@st.fragment(run_every="5s" if live else None)
def body():
    d = load()
    sel = lambda df: df[df["store_id"].isin(stores)] if not df.empty and "store_id" in df else df
    ph = d["pipeline_health"]

    # ---- status strip
    if ph.empty:
        st.info("Waiting for the stream processor. Start Kafka, `scripts/stream_processor.py` and `scripts/store_simulator.py`.")
    else:
        last = ph.iloc[-1]
        age = (pd.Timestamp.now(tz="UTC") - pd.to_datetime(last["processed_at"], utc=True)).total_seconds()
        intr = d["intraday_sku"]
        day = intr["business_date"].iat[0] if not intr.empty else "-"
        st.caption(f"Business day in progress: **{day}** · days closed: **{int(last['days_closed'])}/28** · "
                   f"last micro-batch {age:.0f}s ago ({int(last['rows']):,} events, {last['seconds']:.1f}s, "
                   f"oldest event {last['max_event_age_s']:.0f}s) · champion model **{last['champion']}**"
                   + (" · retraining challenger…" if last.get("retraining") else ""))

    # ---- KPI row
    intr, dvf, inv_day, acc = sel(d["intraday_sku"]), sel(d["demand_vs_forecast"]), sel(d["inventory_daily"]), sel(d["accuracy_daily"])
    k = st.columns(6)
    if not intr.empty:
        sold, fc = intr["units"].sum(), intr["yhat"].sum()
        k[0].metric("Units sold today so far", f"{sold:,.0f}", f"forecast for full day {fc:,.0f}", delta_color="off")
        k[1].metric("SKUs sold out today", f"{int(((intr.on_hand_est <= 0) & (intr.on_hand.fillna(0) > 0)).sum()):,}")
    if not acc.empty:
        last_day = acc["business_date"].max()
        a = acc[(acc.business_date == last_day) & (acc.horizon == 1)]
        if not a.empty:
            k[2].metric(f"Forecast error (WAPE) {last_day}", f"{(a.wape * a.actual).sum() / max(a.actual.sum(), 1):.1%}",
                        help="Next-day forecast, absolute error over actual units, all SKUs in the selected stores.")
    if not inv_day.empty:
        last_inv = inv_day[inv_day.business_date == inv_day.business_date.max()]
        fill = last_inv.units_sold.sum() / max(last_inv.units_sold.sum() + last_inv.lost_units.sum(), 1)
        k[3].metric("Fill rate (last close)", f"{fill:.1%}", help="Units sold / (sold + lost to stock-outs)")
        k[4].metric("SKUs out of stock", f"{int(last_inv.stockout.sum()):,}", f"{int(last_inv.below_rop.sum()):,} below reorder point", delta_color="off")
        k[5].metric("Inventory value on hand", f"${last_inv.inventory_value.sum() / 1e3:,.0f}k")

    # ---- charts row 1
    left, right = st.columns([3, 2])
    with left:
        if not dvf.empty:
            g = dvf.groupby("business_date", as_index=False)[["actual", "forecast"]].sum().sort_values("business_date")
            fig = go.Figure()
            fig.add_scatter(x=g.business_date, y=g.actual, name="Actual units", mode="lines+markers",
                            line=dict(color=ACTUAL, width=2), marker=dict(size=8))
            fig.add_scatter(x=g.business_date, y=g.forecast, name="Next-day forecast", mode="lines+markers",
                            line=dict(color=FORECAST, width=2, dash="dash"), marker=dict(size=8, symbol="diamond"))
            st.plotly_chart(layout(fig, "Daily demand vs forecast", ytitle="units"), width="stretch")
        else:
            st.info("Demand vs forecast appears after the first day closes.")
    with right:
        hr = sel(d["intraday_hourly"])
        if not hr.empty:
            h = hr.groupby(["hour", "channel"], as_index=False)["units"].sum()
            fig = go.Figure()
            for ch, col, name in [("store", ACTUAL, "In store"), ("ecom", ECOM, "E-commerce")]:
                x = h[h.channel == ch]
                fig.add_bar(x=x.hour, y=x.units, name=name, marker=dict(color=col, line=dict(color="#fcfcfb", width=2)))
            fig.update_layout(barmode="stack")
            st.plotly_chart(layout(fig, f"Today by hour ({hr.business_date.iat[0]})", ytitle="units"), width="stretch")

    # ---- charts row 2
    left, right = st.columns(2)
    with left:
        if not inv_day.empty:
            g = inv_day.groupby("business_date", as_index=False)[["stockout", "below_rop", "overstock"]].sum()
            fig = go.Figure()
            for s in ["stockout", "below_rop", "overstock"]:
                fig.add_scatter(x=g.business_date, y=g[s], name=STATUS_LABEL[s], mode="lines+markers",
                                line=dict(color=STATUS[s], width=2), marker=dict(size=8))
            st.plotly_chart(layout(fig, "Inventory health at close (SKU-store count)", ytitle="SKU-stores"), width="stretch")
    with right:
        if not acc.empty:
            g = (acc.assign(err=acc.wape * acc.actual).groupby("horizon", as_index=False)[["err", "actual"]].sum())
            g["wape"] = g.err / g.actual
            fig = go.Figure(go.Bar(x=g.horizon, y=g.wape, marker=dict(color=ACTUAL, cornerradius=4),
                                   text=[f"{v:.0%}" for v in g.wape], textposition="outside", textfont=dict(color=INK2),
                                   hovertemplate="%{x} days ahead: WAPE %{y:.1%}<extra></extra>"))
            fig.update_xaxes(title="days between forecast and sale", dtick=1)
            fig.update_yaxes(tickformat=".0%")
            st.plotly_chart(layout(fig, "Forecast error by lead time (live days)", ytitle="WAPE"), width="stretch")

    # ---- alerts and attention list
    left, right = st.columns([3, 2])
    with left:
        st.markdown("**Alerts**")
        al = sel(d["alerts"])
        if al.empty:
            st.caption("No alerts yet.")
        else:
            al = al.sort_values("raised_at", ascending=False)
            kinds = st.multiselect("Alert types", sorted(al.alert.unique()), default=sorted(al.alert.unique()), key="kinds")
            view = al[al.alert.isin(kinds)].head(300).assign(
                severity=lambda x: x.severity.map(lambda s: f"{SEVERITY_ICON.get(s, '')} {s}"))
            st.dataframe(view[["business_date", "severity", "alert", "store_id", "dept_id", "item_id", "detail"]],
                         hide_index=True, width="stretch", height=300)
            hook = os.environ.get("ALERT_WEBHOOK_URL")
            if hook and st.button("Send critical alerts to webhook"):
                crit = view[view.severity.str.contains("critical")].head(50)
                body_ = json.dumps({"text": "\n".join(f"{r.business_date} {r.store_id} {r.item_id}: {r.alert} - {r.detail}"
                                                      for r in crit.itertuples())}).encode()
                urllib.request.urlopen(urllib.request.Request(hook, body_, {"Content-Type": "application/json"}), timeout=10)
                st.success(f"Sent {len(crit)} alerts")
            st.download_button("Download alerts (CSV)", al.to_csv(index=False), "alerts.csv")
    with right:
        st.markdown("**Needs attention at last close**")
        ih = sel(d["inventory_health"])
        if not ih.empty:
            att = ih[ih.status.isin(["stockout", "below_rop"])].sort_values("mu_daily", ascending=False)
            att = att.assign(status=att.status.map(STATUS_LABEL))
            st.dataframe(att[["store_id", "item_id", "status", "on_hand", "on_order", "reorder_point", "mu_daily", "days_of_cover"]]
                         .rename(columns={"mu_daily": "forecast/day"}).round(1),
                         hide_index=True, width="stretch", height=300)

    # ---- model section
    with st.expander("Models: backtest and retraining"):
        mm, me = d["model_metrics"], d["model_events"]
        if not mm.empty:
            st.markdown("Backtest on the last 4 weeks of history (weekly origins, 1-7 days ahead). "
                        "ARIMA is compared on a random sample of series, so every model is also scored on that sample.")
            st.dataframe(mm.round(3), hide_index=True, width="stretch")
        if not me.empty:
            st.markdown("Champion / challenger retraining during the stream")
            st.dataframe(me.round(3), hide_index=True, width="stretch")


body()
