"""Serving layer: small parquet files that Streamlit and Power BI read.

Files are written to a temp name and swapped in with os.replace, so a reader never sees a half
written file. On Windows the swap fails while a reader holds the file open, so it retries.
"""
import os
import time

import pandas as pd

from rdf import config as C


def write(pdf: pd.DataFrame, name: str, append: bool = False, key: list[str] | None = None):
    C.LAKE_SERVING.mkdir(parents=True, exist_ok=True)
    path = C.LAKE_SERVING / f"{name}.parquet"
    if append and path.exists():
        old = pd.read_parquet(path)
        pdf = pd.concat([old, pdf], ignore_index=True)
        if key:
            pdf = pdf.drop_duplicates(subset=key, keep="last")
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    pdf.to_parquet(tmp, index=False)
    for attempt in range(20):
        try:
            os.replace(tmp, path)
            return path
        except PermissionError:
            time.sleep(0.25 * (attempt + 1))
    raise PermissionError(f"could not replace {path}")


def read(name: str) -> pd.DataFrame:
    path = C.LAKE_SERVING / f"{name}.parquet"
    for attempt in range(10):
        try:
            return pd.read_parquet(path)
        except FileNotFoundError:
            return pd.DataFrame()
        except (PermissionError, OSError):
            time.sleep(0.2)
    return pd.DataFrame()
