import numpy as np
import pandas as pd
import pytest

from rdf import inventory
from rdf.metrics import bias, rmsse, wape
from rdf.models import sba_forecast
from rdf.simulate import Ledger, line_times, split_into_lines


def test_split_conserves_units():
    rng = np.random.default_rng(0)
    for units in [0, 1, 2, 7, 55]:
        lines = split_into_lines(units, rng)
        assert sum(lines) == units
        assert all(q >= 1 for q in lines)


def test_line_times_within_opening_hours():
    t = line_times(1000, np.random.default_rng(1))
    assert t.min() >= 8 * 3600 and t.max() < 22 * 3600
    assert np.all(np.diff(t) >= 0)


def test_ledger_caps_sales_and_receives_after_lead_time():
    led = Ledger(on_hand=5, lead_time=3)
    sold, lost = led.sell(8)
    assert (sold, lost, led.on_hand) == (5, 3, 0)
    qty = led.review(day=0, reorder_point=4, order_up_to=10)
    assert qty == 10 and led.on_order == 10
    assert led.receive(2) == 0
    assert led.receive(3) == 10 and led.on_hand == 10 and led.on_order == 0
    assert led.review(day=3, reorder_point=4, order_up_to=10) == 0   # above reorder point: no order


def test_metrics():
    y, yhat = np.array([0, 2, 4.0]), np.array([1, 2, 2.0])
    assert wape(y, yhat) == pytest.approx(3 / 6)
    assert bias(y, yhat) == pytest.approx(5 / 6 - 1)
    df = pd.DataFrame({"item_id": ["a"] * 3, "store_id": ["s"] * 3, "y": y, "yhat": yhat, "scale": 4.0})
    assert rmsse(df) == pytest.approx(np.sqrt(np.mean((y - yhat) ** 2) / 4))


def test_sba_flat_rate():
    y = np.array([0, 0, 3, 0, 0, 3, 0, 0, 3], float)
    f = sba_forecast(y, alpha=0.1)
    assert 0.8 < f < 1.0          # true rate is 1/day, SBA shrinks it by (1 - alpha/2)
    assert sba_forecast(np.zeros(10)) == 0.0


def test_reorder_point_is_integer_nonnegative_and_monotone():
    mu = np.array([0.0, 0.1, 1.0, 10.0])
    sigma = np.array([0.0, 0.5, 1.5, 4.0])
    low = inventory.reorder_point(mu, sigma, np.full(4, 0.8))
    high = inventory.reorder_point(mu, sigma, np.full(4, 0.99))
    assert low[0] == 0 and np.all(low >= 0) and np.all(low == np.round(low))
    assert np.all(high >= low)
    # fast mover: close to the normal approximation
    assert abs(high[3] - (30 + 2.326 * 4 * np.sqrt(3))) < 4


def test_service_level_ordering():
    lvl = inventory.service_level([5.0, 5.0], ["FOODS", "HOUSEHOLD"], [40, 40])
    assert lvl[0] < lvl[1]       # perishables carry a higher overage cost


def test_eoq_formula():
    q = inventory.eoq([1.0], [10.0])       # D=365, K=ORDER_COST, h=10*0.7*0.25
    from rdf import config as C
    assert q[0] == np.ceil(np.sqrt(2 * 365 * C.ORDER_COST / 1.75))


def test_health_status():
    cover, status = inventory.health([0, 2, 500, 20], [3, 3, 3, 3], [1, 1, 1, 1])
    assert list(status) == ["stockout", "below_rop", "overstock", "healthy"]
