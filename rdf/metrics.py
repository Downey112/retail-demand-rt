"""Forecast accuracy metrics (pandas). WAPE is the headline: it weights SKUs by volume."""
import numpy as np
import pandas as pd


def wape(y, yhat):
    y, yhat = np.asarray(y, float), np.asarray(yhat, float)
    return np.abs(y - yhat).sum() / max(np.abs(y).sum(), 1e-9)


def rmse(y, yhat):
    y, yhat = np.asarray(y, float), np.asarray(yhat, float)
    return float(np.sqrt(np.mean((y - yhat) ** 2)))


def bias(y, yhat):
    """Total forecast over total actual minus 1 (positive = over-forecast)."""
    y, yhat = np.asarray(y, float), np.asarray(yhat, float)
    return yhat.sum() / max(y.sum(), 1e-9) - 1


def rmsse(df: pd.DataFrame, y="y", yhat="yhat", scale="scale", key=("item_id", "store_id")):
    """Mean over series of RMSE scaled by the in-sample one-step naive RMSE (the M5 scaling)."""
    d = df[df[scale] > 0]
    per = d.assign(se=(d[y] - d[yhat]) ** 2).groupby(list(key), observed=True).agg(mse=("se", "mean"), scale=(scale, "first"))
    return float(np.sqrt(per["mse"] / per["scale"]).mean())


def summarise(df: pd.DataFrame, model_col="model", y="y", yhat="yhat"):
    rows = []
    for m, g in df.groupby(model_col, observed=True):
        rows.append({
            "model": m,
            "wape": wape(g[y], g[yhat]),
            "rmse": rmse(g[y], g[yhat]),
            "rmsse": rmsse(g, y, yhat) if "scale" in g else np.nan,
            "bias": bias(g[y], g[yhat]),
            "n": len(g),
        })
    return pd.DataFrame(rows).sort_values("wape").reset_index(drop=True)
