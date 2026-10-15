"""Build the 'operational retail database' (SQLite) that the batch import reads from.

It holds everything up to HISTORY_END. The 28 days after that are kept out of the database:
they reach the platform only through the Kafka stream.
"""
import sqlite3
import time

import pandas as pd

from rdf import config as C


def main():
    t0 = time.time()
    C.RETAIL_DB.unlink(missing_ok=True)
    con = sqlite3.connect(C.RETAIL_DB)
    con.execute("PRAGMA journal_mode=OFF")
    con.execute("PRAGMA synchronous=OFF")

    sales = pd.read_parquet(C.RAW_M5 / "m5_train.parquet")
    uid = sales["unique_id"].astype(str)
    sales = pd.DataFrame({
        "item_id": uid.str[:-5],
        "store_id": uid.str[-4:],
        "sale_date": sales["ds"].astype(str),
        "units": sales["y"].astype("int32"),
    })
    assert sales["sale_date"].max() == C.HISTORY_END

    items = sales[["item_id"]].drop_duplicates()
    items["dept_id"] = items["item_id"].str.rsplit("_", n=1).str[0]
    items["cat_id"] = items["item_id"].str.split("_").str[0]
    stores = pd.DataFrame({"store_id": sorted(sales["store_id"].unique())})
    stores["state_id"] = stores["store_id"].str[:2]
    cal = pd.read_csv(C.RAW_M5 / "m5_xregs.csv")
    prices = pd.read_csv(C.RAW_M5 / "m5_sell_prices.csv")

    for name, df in [("items", items), ("stores", stores), ("calendar", cal), ("sell_prices", prices)]:
        df.to_sql(name, con, index=False)
        print(f"{name}: {len(df):,} rows")

    con.execute("CREATE TABLE daily_sales (item_id TEXT, store_id TEXT, sale_date TEXT, units INTEGER)")
    for store, part in sales.groupby("store_id", sort=True):
        con.executemany("INSERT INTO daily_sales VALUES (?,?,?,?)", part.itertuples(index=False, name=None))
        con.commit()
        print(f"daily_sales {store}: {len(part):,} rows ({time.time() - t0:.0f}s)")
    con.execute("CREATE INDEX ix_sales_store ON daily_sales(store_id)")
    con.commit()
    con.close()
    print(f"done in {time.time() - t0:.0f}s -> {C.RETAIL_DB}")


if __name__ == "__main__":
    main()
