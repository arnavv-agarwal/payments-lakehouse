"""Reconcile the CDC topics in Kafka against the Postgres source tables.

For each table, replay its topic from the beginning and keep the latest event
per primary key (a delete event removes the key). Then compare with the table:
the same set of keys, and the same updated_at for every key. A trigger bumps
updated_at on every UPDATE, so a matching updated_at means Kafka holds the
latest version of that row.

Also reports events by operation and re-delivered duplicates: Debezium is
at-least-once, so after a crash some events can appear twice.

Run it while the generator is stopped and the connector has caught up;
otherwise rows that changed in the last few seconds show up as stale.

    python -m tools.reconcile_cdc
"""
import os
import sys
import time
import uuid
from collections import Counter
from datetime import datetime

from confluent_kafka import Consumer, KafkaException, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from generator.db import connect

TOPIC_PREFIX = "payments.public"
TABLES = {
    "merchants": "merchant_id",
    "customers": "customer_id",
    "payment_methods": "payment_method_id",
    "payments": "payment_id",
    "refunds": "refund_id",
}


def replay_topic(consumer, deserialize, topic, pk):
    """Read a topic up to its current end; return latest updated_at per key and stats."""
    partitions = consumer.list_topics(topic, timeout=10).topics[topic].partitions
    ends, assignment = {}, []
    for p in partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=10)
        if high > low:
            ends[p] = high
            assignment.append(TopicPartition(topic, p, low))
    consumer.assign(assignment)

    latest, ops, seen, duplicates = {}, Counter(), set(), 0
    key_ctx = SerializationContext(topic, MessageField.KEY)
    value_ctx = SerializationContext(topic, MessageField.VALUE)
    pending = set(ends)
    last_progress = time.monotonic()
    while pending:
        # consume() returns once it has num_messages OR the timeout passes, so a
        # short timeout keeps small topics from waiting for a full batch.
        batch = consumer.consume(num_messages=10_000, timeout=0.5)
        if not batch:
            if time.monotonic() - last_progress > 15:
                raise TimeoutError(f"{topic}: no messages for 15 s before reaching the end offsets")
            continue
        last_progress = time.monotonic()
        for msg in batch:
            if msg.error():
                raise KafkaException(msg.error())
            if msg.offset() >= ends[msg.partition()]:
                continue  # arrived after we took the end offsets
            key = deserialize(msg.key(), key_ctx)[pk]
            if msg.value() is None:
                ops["tombstone"] += 1  # lets log compaction drop the key later
                continue
            event = deserialize(msg.value(), value_ctx)
            op = event["op"]
            ops[op] += 1
            identity = (key, event["source"]["lsn"], op)
            if identity in seen:
                duplicates += 1
            seen.add(identity)
            if op == "d":
                latest.pop(key, None)
            else:
                latest[key] = datetime.fromisoformat(event["after"]["updated_at"])
        positions = consumer.position([TopicPartition(topic, p) for p in pending])
        pending = {tp.partition for tp in positions if tp.offset < ends[tp.partition]}
    consumer.unassign()
    return latest, ops, duplicates


def main():
    registry = SchemaRegistryClient({"url": os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")})
    deserialize = AvroDeserializer(registry)
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": f"reconcile-{uuid.uuid4().hex[:8]}",  # required, but we never commit
        "enable.auto.commit": False,
        # "localhost" resolves to IPv6 first, but Docker publishes the port on IPv4 only.
        "broker.address.family": "v4",
    })

    header = (f"{'table':16} {'postgres':>9} {'kafka':>9} {'missing':>8} {'extra':>6} "
              f"{'stale':>6} {'dupes':>6}  events")
    print(header)
    print("-" * len(header))
    all_ok = True
    with connect() as conn:
        for table, pk in TABLES.items():
            started = time.monotonic()
            source = dict(conn.execute(f"SELECT {pk}, updated_at FROM {table}").fetchall())
            kafka, ops, duplicates = replay_topic(consumer, deserialize, f"{TOPIC_PREFIX}.{table}", pk)

            missing = source.keys() - kafka.keys()   # in Postgres, never arrived
            extra = kafka.keys() - source.keys()     # deleted in Postgres, delete never arrived
            stale = [k for k in source.keys() & kafka.keys() if source[k] != kafka[k]]
            ok = not (missing or extra or stale)
            all_ok &= ok
            events = " ".join(f"{op}={n}" for op, n in sorted(ops.items()))
            print(f"{table:16} {len(source):>9} {len(kafka):>9} {len(missing):>8} {len(extra):>6} "
                  f"{len(stale):>6} {duplicates:>6}  {events}  "
                  f"{'OK' if ok else 'MISMATCH'} ({time.monotonic() - started:.1f}s)")
    consumer.close()
    print("\nop codes: r = snapshot read, c = insert, u = update, d = delete")
    print("RESULT:", "PASS, Kafka matches Postgres" if all_ok else "FAIL")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
