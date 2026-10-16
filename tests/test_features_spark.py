"""Leakage test: changing sales on or after day t-6 must not change any feature for day t."""
import os
import sys
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from rdf import config as C


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("JAVA_HOME", str(C.JAVA_HOME))
    os.environ.setdefault("HADOOP_HOME", str(C.HADOOP_HOME))
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PATH"] = f"{C.HADOOP_HOME / 'bin'};{os.environ['PATH']}"
    from pyspark.sql import SparkSession
    s = (SparkSession.builder.master("local[2]").appName("test")
         .config("spark.sql.shuffle.partitions", 2).config("spark.ui.enabled", "false").getOrCreate())
    yield s
    s.stop()


def _frames(spark, units, anchor):
    start = date(2015, 1, 1)
    days = [start + timedelta(days=i) for i in range(len(units))]
    sales = pd.DataFrame({"item_id": "FOODS_1_001", "store_id": "CA_1", "sale_date": days,
                          "units": units, "stockout": 0})
    cal = pd.DataFrame({"date": days, "wm_yr_wk": [11500 + i // 7 for i in range(len(days))],
                        "snap_CA": 0, "snap_TX": 0, "snap_WI": 0, "event_type_1": "", "event_type_2": ""})
    prices = pd.DataFrame({"store_id": "CA_1", "item_id": "FOODS_1_001",
                           "wm_yr_wk": sorted(set(cal.wm_yr_wk)), "sell_price": 2.0})
    items = pd.DataFrame({"item_id": ["FOODS_1_001"], "dept_id": ["FOODS_1"], "cat_id": ["FOODS"]})
    sales, items, cal, prices = [spark.createDataFrame(x) for x in (sales, items, cal, prices)]
    return sales, anchor, items, cal, prices


def test_no_feature_uses_data_after_its_origin(spark):
    """Perturb sales from day p on. A row's features may change only if its origin or t-7 is >= p."""
    from rdf.features import FEATURES, build_features

    rng = np.random.default_rng(0)
    base = rng.poisson(3, 120).astype(int)
    p = 100
    changed = base.copy()
    changed[p:] += 50
    anchor = (date(2015, 1, 1) + timedelta(days=90)).isoformat()      # day index 90 is an origin
    a = build_features(spark, *_frames(spark, base, anchor)).toPandas().sort_values("sale_date").reset_index(drop=True)
    b = build_features(spark, *_frames(spark, changed, anchor)).toPandas().sort_values("sale_date").reset_index(drop=True)
    idx = np.arange(len(a))
    origin = idx - a["horizon"].to_numpy().astype(int)
    assert set(a["horizon"].unique()) == set(range(1, 8))
    assert (origin[90 + 1: 90 + 8] == 90).all()
    safe = (origin < p) & (idx - 7 < p)
    pd.testing.assert_frame_equal(a.loc[safe, FEATURES], b.loc[safe, FEATURES])
    # the perturbation does reach rows whose origin is on or after p
    assert not np.allclose(a.loc[~safe, "o_rm_7"].to_numpy()[-20:], b.loc[~safe, "o_rm_7"].to_numpy()[-20:])


def test_write_partitioned_replaces_only_its_partition(spark, tmp_path):
    """Re-running a day after a crash must replace that day, keep the others, and never wipe a legacy table."""
    from rdf.spark import write_partitioned
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS wp_test LOCATION '{(tmp_path / 'wp').as_uri()}'")
    t = "wp_test.sales"
    day = lambda d, units: spark.createDataFrame(pd.DataFrame({"sale_date": [d] * 2, "item_id": ["a", "b"], "units": units}))
    try:
        write_partitioned(spark, day("2016-06-18", [1, 2]), t, "sale_date")      # first write creates the table
        write_partitioned(spark, day("2016-06-19", [3, 4]), t, "sale_date")
        write_partitioned(spark, day("2016-06-19", [5, 6]), t, "sale_date")      # re-run of the same day
        got = spark.table(t).toPandas().sort_values(["sale_date", "item_id"])
        assert got.groupby("sale_date").units.sum().to_dict() == {"2016-06-18": 3, "2016-06-19": 11}
        assert len(got) == 4

        day("2016-06-18", [1, 2]).write.saveAsTable("wp_test.legacy")             # unpartitioned
        with pytest.raises(RuntimeError, match="--reset"):
            write_partitioned(spark, day("2016-06-19", [3, 4]), "wp_test.legacy", "sale_date")
        assert spark.table("wp_test.legacy").count() == 2
    finally:
        spark.sql("DROP DATABASE IF EXISTS wp_test CASCADE")
