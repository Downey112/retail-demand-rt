"""Pure functions behind the store simulator (kept separate so they can be unit tested)."""
import numpy as np

OPEN_HOUR, CLOSE_HOUR = 8, 22


def split_into_lines(units: int, rng: np.random.Generator) -> list[int]:
    """Split a SKU's daily units into POS line quantities (mostly 1, sometimes 2-4). Sums to units."""
    out, left = [], int(units)
    while left > 0:
        q = int(min(left, rng.geometric(0.65)))   # geometric >= 1, mean ~1.5
        out.append(q)
        left -= q
    return out


def line_times(n: int, rng: np.random.Generator) -> np.ndarray:
    """Seconds after midnight for n sales, with a lunch and an evening peak."""
    peaks = rng.choice(3, size=n, p=[0.3, 0.3, 0.4])
    centre = np.array([11.0, 13.0, 18.0])[peaks]
    hours = np.clip(rng.normal(centre, 2.0), OPEN_HOUR, CLOSE_HOUR - 1e-3)
    return np.sort((hours * 3600).astype(int))


class Ledger:
    """Inventory ledger for one SKU-store: sales capped at stock on hand, orders arrive after lead time."""

    def __init__(self, on_hand: float, lead_time: int):
        self.on_hand = float(on_hand)
        self.lead_time = lead_time
        self.pipeline: list[tuple[int, float]] = []   # (arrival_day_index, qty)

    @property
    def on_order(self) -> float:
        return sum(q for _, q in self.pipeline)

    def receive(self, day: int) -> float:
        arrived = sum(q for d, q in self.pipeline if d <= day)
        self.pipeline = [(d, q) for d, q in self.pipeline if d > day]
        self.on_hand += arrived
        return arrived

    def sell(self, demand: float) -> tuple[float, float]:
        sold = min(demand, self.on_hand)
        self.on_hand -= sold
        return sold, demand - sold

    def review(self, day: int, reorder_point: float, order_up_to: float) -> float:
        """Continuous-review (s, S): if position <= s, order up to S."""
        position = self.on_hand + self.on_order
        if position <= reorder_point:
            qty = float(np.ceil(order_up_to - position))
            if qty > 0:
                self.pipeline.append((day + self.lead_time, qty))
                return qty
        return 0.0
