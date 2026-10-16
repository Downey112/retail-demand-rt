"""Store simulator: replays the 28 held-out M5 days into Kafka on an accelerated clock.

For every simulated day it
  1. receives replenishment orders that are due (inventory-updates: receipt),
  2. sells the actual M5 demand, capped at stock on hand, as POS line items spread over opening
     hours (pos-transactions; ~15% flagged as e-commerce, a synthetic split),
  3. reviews stock against the latest policy Spark published and places orders
     (inventory-updates: order_placed), posts end-of-day stock counts (inventory-updates:
     stock_count), and closes the day per store with control totals (store-events: day_close).

The day-close message carries line and unit counts so the stream processor can confirm it has
every sale for that day before acting on it.
"""
import argparse
import json
import time
import uuid

import numpy as np
import pandas as pd
from confluent_kafka import Producer

from rdf import config as C
from rdf import serving, streaming
from rdf.simulate import CLOSE_HOUR, OPEN_HOUR, Ledger, line_times, split_into_lines


def load_demand(stores):
    d = pd.read_parquet(C.RAW_M5 / "m5_test.parquet")
    uid = d["unique_id"].astype(str)
    d = pd.DataFrame({"item_id": uid.str[:-5], "store_id": uid.str[-4:],
                      "date": pd.to_datetime(d["ds"]).dt.date.astype(str), "units": d["y"].astype(int)})
    d = d[d["store_id"].isin(stores)]
    cal = pd.read_csv(C.RAW_M5 / "m5_xregs.csv", usecols=["date", "wm_yr_wk"])
    prices = pd.read_csv(C.RAW_M5 / "m5_sell_prices.csv")
    prices = prices[prices["store_id"].isin(stores) & prices["wm_yr_wk"].isin(cal.loc[cal["date"].isin(d["date"]), "wm_yr_wk"])]
    d = d.merge(cal, on="date").merge(prices, on=["store_id", "item_id", "wm_yr_wk"], how="left")
    return d


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seconds-per-day", type=float, default=60.0)
    ap.add_argument("--days", type=int, default=C.STREAM_DAYS)
    ap.add_argument("--wait-for-plan", action="store_true",
                    help="before each day, wait until Spark has published the plan for the previous day")
    args = ap.parse_args()

    stores = C.SCOPE_STORES
    rng = np.random.default_rng(C.SEED)
    producer = Producer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "linger.ms": 50,
                         "acks": "all", "enable.idempotence": True, "compression.type": "lz4"})

    def send(topic, key, payload):
        payload["emitted_at"] = time.time()
        while True:
            try:
                producer.produce(topic, key=key.encode(), value=json.dumps(payload).encode())
                break
            except BufferError:
                producer.poll(0.5)
        producer.poll(0)

    demand = load_demand(stores)
    days = sorted(demand["date"].unique())[: args.days]
    if not streaming.claim_ready():
        print("waiting for stream_processor.py --reset to prepare a new replay", flush=True)
        while not streaming.claim_ready():
            time.sleep(2)
    policy = serving.read("inventory_policy")
    if policy.empty or str(policy["origin"].astype(str).max()) != C.HISTORY_END:
        raise SystemExit(f"Expected the plan made at {C.HISTORY_END}. Run scripts/train.py first.")
    pol = policy.set_index(["item_id", "store_id"])
    ledgers = {}
    for (item, store), r in pol.iterrows():
        start = r.reorder_point + rng.uniform(0.3, 1.0) * r.eoq      # staggered opening stock
        ledgers[(item, store)] = Ledger(np.ceil(start), C.LEAD_TIME_DAYS)

    # Opening stock position as of the end of history.
    for (item, store), led in ledgers.items():
        send(C.TOPIC_INVENTORY, f"{store}|{item}", {
            "event_type": "stock_count", "store_id": store, "item_id": item, "business_date": C.HISTORY_END,
            "event_time": f"{C.HISTORY_END}T23:00:00", "qty": 0.0, "on_hand": led.on_hand,
            "on_order": 0.0, "lost_units": 0.0})
    producer.flush(30)
    print(f"simulating {len(days)} days x {len(stores)} stores, {len(ledgers):,} SKU-store ledgers", flush=True)

    for di, day in enumerate(days):
        if args.wait_for_plan and di > 0:
            prev = days[di - 1]
            while str(serving.read("inventory_policy").get("origin", pd.Series([""])).astype(str).max()) < prev:
                time.sleep(1)
        policy = serving.read("inventory_policy")
        if not policy.empty:
            pol = policy.set_index(["item_id", "store_id"])
        day_start = time.time()
        today = demand[demand["date"] == day]
        lines, totals = [], {s: {"pos_lines": 0, "pos_units": 0.0} for s in stores}

        # 1. receipts at opening
        for (item, store), led in ledgers.items():
            got = led.receive(di)
            if got:
                send(C.TOPIC_INVENTORY, f"{store}|{item}", {
                    "event_type": "receipt", "store_id": store, "item_id": item, "business_date": day,
                    "event_time": f"{day}T{OPEN_HOUR - 1:02d}:30:00", "qty": got, "on_hand": led.on_hand,
                    "on_order": led.on_order, "lost_units": 0.0})

        # 2. sales: actual demand capped at stock, split into timed POS lines
        lost = {}
        for r in today.itertuples(index=False):
            led = ledgers.get((r.item_id, r.store_id))
            if led is None:
                continue
            sold, short = led.sell(r.units)
            lost[(r.item_id, r.store_id)] = short
            qtys = split_into_lines(int(sold), rng)
            for q, t in zip(qtys, line_times(len(qtys), rng)):
                lines.append((t, r.store_id, r.item_id, q, r.sell_price))
        lines.sort(key=lambda x: x[0])
        span = args.seconds_per_day * 0.8
        open_s, close_s = OPEN_HOUR * 3600, CLOSE_HOUR * 3600
        for i, (t, store, item, q, price) in enumerate(lines):
            if i % 200 == 0:   # pace the replay to the simulated clock
                due = day_start + (t - open_s) / (close_s - open_s) * span
                if due > time.time():
                    time.sleep(due - time.time())
            channel = "ecom" if rng.random() < C.ECOM_SHARE else "store"
            send(C.TOPIC_POS, f"{store}|{item}", {
                "event_type": "sale", "txn_id": uuid.uuid4().hex[:16], "store_id": store, "item_id": item,
                "qty": float(q), "unit_price": None if pd.isna(price) else float(price), "channel": channel,
                "business_date": day, "event_time": f"{day}T{t // 3600:02d}:{t % 3600 // 60:02d}:{t % 60:02d}"})
            totals[store]["pos_lines"] += 1
            totals[store]["pos_units"] += q
        producer.flush(60)

        # 3. end of day: replenishment review, stock counts, day close
        orders = 0
        for (item, store), led in ledgers.items():
            if (item, store) in pol.index:
                p = pol.loc[(item, store)]
                qty = led.review(di, p.reorder_point, p.order_up_to)
                if qty:
                    orders += 1
                    send(C.TOPIC_INVENTORY, f"{store}|{item}", {
                        "event_type": "order_placed", "store_id": store, "item_id": item, "business_date": day,
                        "event_time": f"{day}T{CLOSE_HOUR:02d}:30:00", "qty": qty, "on_hand": led.on_hand,
                        "on_order": led.on_order, "lost_units": 0.0})
            send(C.TOPIC_INVENTORY, f"{store}|{item}", {
                "event_type": "stock_count", "store_id": store, "item_id": item, "business_date": day,
                "event_time": f"{day}T23:00:00", "qty": 0.0, "on_hand": led.on_hand, "on_order": led.on_order,
                "lost_units": float(lost.get((item, store), 0.0))})
        producer.flush(60)
        counts = {s: sum(1 for (_, st) in ledgers if st == s) for s in stores}
        for s in stores:
            send(C.TOPIC_STORE_EVENTS, s, {
                "event_type": "day_close", "store_id": s, "business_date": day,
                "event_time": f"{day}T23:59:00", **totals[s], "stock_counts": counts[s]})
        producer.flush(30)
        units = sum(v["pos_units"] for v in totals.values())
        lost_total = sum(lost.values())
        print(f"{day}: {len(lines):,} POS lines, {units:,.0f} units sold, {lost_total:,.0f} lost, {orders:,} orders "
              f"({time.time() - day_start:.0f}s)", flush=True)
        rest = day_start + args.seconds_per_day - time.time()
        if rest > 0:
            time.sleep(rest)


if __name__ == "__main__":
    main()
