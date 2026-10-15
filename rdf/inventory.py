"""Operational research layer: service level (newsvendor), reorder point, safety stock, EOQ.

Lead-time demand is modelled as negative binomial (mean and variance matched to the forecast and
its backtest error) rather than normal: most M5 SKU-days are zero, and a normal approximation
gives negative or badly inflated reorder points for slow movers.
"""
import numpy as np
import pandas as pd
from scipy import stats

from rdf import config as C

# Overage cost beyond holding: annual share of unit cost lost to spoilage, markdown or obsolescence.
OBSOLESCENCE = {"FOODS": 0.60, "HOBBIES": 0.20, "HOUSEHOLD": 0.10}


def service_level(price, cat_id, cycle_days):
    """Newsvendor critical ratio Cu / (Cu + Co), clipped to the allowed range.

    Cu: margin lost on a unit of unmet demand. Co: cost of a unit left over, i.e. carried
    (holding plus spoilage/obsolescence) for one replenishment cycle."""
    price = np.asarray(price, float)
    unit_cost = price * (1 - C.GROSS_MARGIN)
    cu = price * C.GROSS_MARGIN
    obs = np.array([OBSOLESCENCE.get(c, 0.20) for c in np.asarray(cat_id)])
    co = unit_cost * (C.HOLDING_RATE_ANNUAL + obs) * np.asarray(cycle_days, float) / 365
    cr = np.divide(cu, cu + co, out=np.full_like(cu, C.SERVICE_LEVEL_CAP[0]), where=(cu + co) > 0)
    return np.clip(cr, *C.SERVICE_LEVEL_CAP)


def eoq(daily_demand, price):
    annual = np.asarray(daily_demand, float) * 365
    h = np.maximum(np.asarray(price, float) * (1 - C.GROSS_MARGIN) * C.HOLDING_RATE_ANNUAL, 1e-6)
    return np.maximum(np.ceil(np.sqrt(2 * annual * C.ORDER_COST / h)), 1)


def reorder_point(mu_daily, sigma_daily, level, lead_time=C.LEAD_TIME_DAYS):
    """Negative-binomial quantile of lead-time demand at the target service level."""
    m = np.asarray(mu_daily, float) * lead_time
    v = np.asarray(sigma_daily, float) ** 2 * lead_time
    v = np.maximum(v, m * 1.05 + 1e-9)            # NB needs variance > mean (over-dispersion)
    p = np.clip(np.divide(m, v, out=np.ones_like(m), where=v > 0), 1e-9, 1 - 1e-9)
    n = np.maximum(m * p / (1 - p), 1e-9)
    rop = stats.nbinom.ppf(np.asarray(level, float), n, p)
    return np.where(m <= 1e-9, 0.0, rop)


def build_policy(df: pd.DataFrame) -> pd.DataFrame:
    """df needs item_id, store_id, cat_id, mu_daily (forecast), sigma_daily (forecast error), price."""
    out = df.copy()
    out["price"] = out["price"].fillna(out["price"].median()).clip(lower=0.01)
    out["eoq"] = eoq(out["mu_daily"], out["price"])
    cycle = np.where(out["mu_daily"] > 0, out["eoq"] / out["mu_daily"].clip(lower=1e-6), 365).clip(max=365)
    out["service_level"] = service_level(out["price"], out["cat_id"], cycle)
    out["reorder_point"] = reorder_point(out["mu_daily"], out["sigma_daily"], out["service_level"])
    out["safety_stock"] = (out["reorder_point"] - out["mu_daily"] * C.LEAD_TIME_DAYS).clip(lower=0)
    out["order_up_to"] = out["reorder_point"] + out["eoq"]
    return out


def health(on_hand, reorder_point, mu_daily, overstock_days=60):
    """Classify stock position: stockout, below_rop, overstock or healthy."""
    on_hand, rop, mu = (np.asarray(a, float) for a in (on_hand, reorder_point, mu_daily))
    cover = np.where(mu > 0, on_hand / np.maximum(mu, 1e-9), np.inf)
    status = np.select(
        [on_hand <= 0, on_hand <= rop, cover > overstock_days],
        ["stockout", "below_rop", "overstock"], default="healthy")
    return cover, status
