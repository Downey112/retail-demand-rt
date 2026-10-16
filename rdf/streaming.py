"""Kafka topics and the hand-off between a stream processor reset and the store simulator."""
import json

from rdf import config as C

TOPICS = {C.TOPIC_POS: 4, C.TOPIC_INVENTORY: 4, C.TOPIC_STORE_EVENTS: 1}   # topic -> partitions
# Written by `stream_processor.py --reset` once it is ready; the simulator waits for it and claims it, so each
# replay is one reset plus one simulator run, and events from an earlier replay are never read again.
READY_FILE = C.LAKE / "_state" / "replay_ready.json"


def ensure_topics():
    from confluent_kafka.admin import AdminClient, NewTopic
    admin = AdminClient({"bootstrap.servers": C.KAFKA_BOOTSTRAP})
    existing = set(admin.list_topics(timeout=10).topics)
    new = [NewTopic(t, num_partitions=n, replication_factor=1) for t, n in TOPICS.items() if t not in existing]
    for f in admin.create_topics(new).values() if new else []:
        f.result()


def end_offsets() -> dict:
    """Current end offset of every partition, in Spark's startingOffsets JSON shape."""
    from confluent_kafka import Consumer, TopicPartition
    consumer = Consumer({"bootstrap.servers": C.KAFKA_BOOTSTRAP, "group.id": "rdf-offsets", "enable.auto.commit": False})
    try:
        return {t: {str(p): consumer.get_watermark_offsets(TopicPartition(t, p), timeout=10)[1] for p in range(n)}
                for t, n in TOPICS.items()}
    finally:
        consumer.close()


def mark_ready(offsets: dict):
    READY_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = READY_FILE.with_suffix(".tmp")
    tmp.write_text(json.dumps({"history_end": C.HISTORY_END, "offsets": offsets}))
    tmp.replace(READY_FILE)


def claim_ready() -> bool:
    try:
        READY_FILE.unlink()
        return True
    except (FileNotFoundError, PermissionError):     # PermissionError: briefly locked on Windows; try again
        return False
