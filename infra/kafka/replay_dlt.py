#!/usr/bin/env python3
"""Replays messages from order-events.dlt back onto order-events, after you've fixed the cause.

Copies key, value bytes and original headers unchanged (the dlt-* diagnostic headers are
dropped), so the stats worker processes them as if they had just arrived. The projector
deduplicates on event id, so replaying something twice is harmless.

Ordering: a replayed event lands *after* everything already on its partition. That's fine for
the stats projection (it only adds counts), but think twice before replaying into a consumer
that keeps "latest state" per order.

Requires: pip install confluent-kafka
Run as the Kafka admin user (the app users aren't allowed to write to order-events from here):

    KAFKA_ADMIN_PASSWORD=... python3 infra/kafka/replay_dlt.py --dry-run
    KAFKA_ADMIN_PASSWORD=... python3 infra/kafka/replay_dlt.py --reason processing-failed
"""
from __future__ import annotations

import argparse
import os
import sys
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition

REPO = Path(__file__).resolve().parent.parent.parent
SOURCE, TARGET = "order-events.dlt", "order-events"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--bootstrap", default=os.environ.get("KAFKA_BOOTSTRAP", "localhost:19092,localhost:19093,localhost:19094"))
    parser.add_argument("--user", default=os.environ.get("KAFKA_USER", "admin"))
    parser.add_argument("--password", default=os.environ.get("KAFKA_ADMIN_PASSWORD"))
    parser.add_argument("--ca-file", default=str(REPO / "infra/kafka/secrets/public/ca.crt"))
    parser.add_argument("--reason", help="only replay messages with this dlt-reason (e.g. processing-failed)")
    parser.add_argument("--dry-run", action="store_true", help="list what would be replayed")
    args = parser.parse_args()

    if not args.password:
        print("Set KAFKA_ADMIN_PASSWORD or pass --password.", file=sys.stderr)
        return 1

    security = {
        "bootstrap.servers": args.bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": args.user,
        "sasl.password": args.password,
        "ssl.ca.location": args.ca_file,
    }

    # A throwaway group that never commits: reading the DLT doesn't remove anything from it.
    consumer = Consumer({**security, "group.id": f"dlt-replay-{uuid.uuid4()}",
                         "enable.auto.commit": False, "auto.offset.reset": "earliest"})
    producer = Producer({**security, "acks": "all", "enable.idempotence": True})

    # Read every partition up to its current end, then stop.
    metadata = consumer.list_topics(SOURCE, timeout=10)
    if metadata.topics[SOURCE].error:
        raise KafkaException(metadata.topics[SOURCE].error)
    partitions = [TopicPartition(SOURCE, p, 0) for p in metadata.topics[SOURCE].partitions]
    ends = {tp.partition: consumer.get_watermark_offsets(tp, timeout=10)[1] for tp in partitions}
    remaining = {p for p, end in ends.items() if end > 0}
    consumer.assign([TopicPartition(SOURCE, p, 0) for p in remaining])

    replayed = skipped = 0
    while remaining:
        msg = consumer.poll(5.0)
        if msg is None:
            break
        if msg.error():
            raise KafkaException(msg.error())

        headers = dict((k, v) for k, v in (msg.headers() or []))
        reason = (headers.get("dlt-reason") or b"").decode()
        if args.reason and reason != args.reason:
            skipped += 1
        else:
            error = (headers.get("dlt-error") or b"").decode()
            print(f"{'would replay' if args.dry_run else 'replaying'} p{msg.partition()}@{msg.offset()} "
                  f"key={msg.key().decode() if msg.key() else None} reason={reason} error={error[:120]}")
            if not args.dry_run:
                original_headers = [(k, v) for k, v in (msg.headers() or []) if not k.startswith("dlt-")]
                producer.produce(TARGET, key=msg.key(), value=msg.value(), headers=original_headers)
                producer.poll(0)
            replayed += 1

        if msg.offset() + 1 >= ends[msg.partition()]:
            remaining.discard(msg.partition())

    if not args.dry_run:
        undelivered = producer.flush(30)
        if undelivered:
            print(f"{undelivered} message(s) not acknowledged; re-run to retry.", file=sys.stderr)
            return 1
    consumer.close()

    print(f"{'Would replay' if args.dry_run else 'Replayed'} {replayed} message(s); skipped {skipped}.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
