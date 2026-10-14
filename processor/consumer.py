"""Idempotent Kafka consumer: otel.raw.v1 -> normalize -> Postgres.

Delivery model: at-least-once from Kafka (manual offset commits, committed
only after a message's events are durably persisted or dead-lettered).
Idempotency is enforced at the application layer via the unique `event_id`
column (`ON CONFLICT (event_id) DO NOTHING`), not via Kafka exactly-once
semantics -- replaying already-processed messages (e.g. after a consumer
group offset reset) re-inserts zero rows rather than duplicating events.

Malformed messages (bad JSON, missing/invalid span fields) are produced to
the DLQ topic and their offset is still committed, so a poison pill cannot
block the partition. Transient DB errors are retried with backoff; if
retries are exhausted, the exception propagates and the process exits
without committing that message's offset -- a process restart (Docker's
restart policy) resumes from the last successfully committed offset, so a
DB outage cannot silently drop events.
"""

from __future__ import annotations

import json
import logging
import os

from confluent_kafka import Consumer, KafkaException, Producer
from sqlalchemy import Engine
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import OperationalError
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from services.common.db import build_engine
from processor.models import Event
from processor.schema import NormalizedEvent, NormalizationError, normalize_span
from services.common.otlp_json import iter_spans

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("pulseops.processor")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:19092")
RAW_TOPIC = os.environ.get("KAFKA_RAW_TELEMETRY_TOPIC", "otel.raw.v1")
DLQ_TOPIC = os.environ.get("KAFKA_DLQ_TOPIC", "otel.dlq.v1")
GROUP_ID = os.environ.get("KAFKA_CONSUMER_GROUP", "pulseops-event-normalizer")


def build_consumer() -> Consumer:
    return Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            # Shorter than librdkafka's 45s default so a stopped/crashed
            # consumer's membership expires quickly -- otherwise tooling
            # like `kafka-consumer-groups --reset-offsets` has to wait out
            # the full session timeout before the group leaves "Stable".
            "session.timeout.ms": 10000,
            "heartbeat.interval.ms": 3000,
        }
    )


def build_producer() -> Producer:
    return Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})


def send_to_dlq(producer: Producer, raw_value: bytes, reason: str, source_topic: str, partition: int, offset: int) -> None:
    producer.produce(
        DLQ_TOPIC,
        value=raw_value,
        headers={
            "dlq_reason": reason.encode()[:1000],
            "source_topic": source_topic.encode(),
            "source_partition": str(partition).encode(),
            "source_offset": str(offset).encode(),
        },
    )
    producer.flush(5)


def _event_row(e: NormalizedEvent, partition: int, offset: int) -> dict:
    return {
        "event_id": e.event_id,
        "schema_version": e.schema_version,
        "service": e.service,
        "event_timestamp": e.event_timestamp,
        "trace_id": e.trace_id,
        "span_id": e.span_id,
        "parent_span_id": e.parent_span_id,
        "event_type": e.event_type,
        "duration_ms": e.duration_ms,
        "status": e.status,
        "severity": e.severity,
        "attributes": e.attributes,
        "raw_kafka_partition": partition,
        "raw_kafka_offset": offset,
    }


@retry(
    reraise=True,
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=0.5, max=10),
    retry=retry_if_exception_type(OperationalError),
)
def persist_events(engine: Engine, events: list[NormalizedEvent], partition: int, offset: int) -> int:
    """Idempotent bulk upsert. Returns the number of NEW rows actually inserted.

    Uses RETURNING rather than cursor.rowcount: psycopg3's rowcount for a
    multi-row INSERT ... ON CONFLICT DO NOTHING is not reliably the count of
    rows actually inserted (observed -1 / ambiguous values in practice), but
    rows skipped by the conflict simply don't appear in RETURNING, so
    counting returned rows is exact.
    """
    if not events:
        return 0
    stmt = pg_insert(Event).values([_event_row(e, partition, offset) for e in events])
    stmt = stmt.on_conflict_do_nothing(index_elements=["event_id"]).returning(Event.id)
    with engine.begin() as conn:
        result = conn.execute(stmt)
        return len(result.fetchall())


def decode_and_normalize(raw_value: bytes) -> list[NormalizedEvent]:
    record = json.loads(raw_value)
    return [normalize_span(span) for span in iter_spans(record)]


def run() -> None:
    engine = build_engine()
    consumer = build_consumer()
    producer = build_producer()
    consumer.subscribe([RAW_TOPIC])
    logger.info("normalizer started: group=%s topic=%s -> dlq=%s", GROUP_ID, RAW_TOPIC, DLQ_TOPIC)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())

            partition, offset, raw_value = msg.partition(), msg.offset(), msg.value()

            try:
                events = decode_and_normalize(raw_value)
            except (json.JSONDecodeError, NormalizationError, KeyError, TypeError, ValueError) as exc:
                logger.warning("malformed message partition=%s offset=%s: %s", partition, offset, exc)
                send_to_dlq(producer, raw_value, str(exc), msg.topic(), partition, offset)
                consumer.commit(message=msg, asynchronous=False)
                continue

            inserted = persist_events(engine, events, partition, offset)
            logger.info(
                "partition=%s offset=%s spans=%d inserted=%d duplicates=%d",
                partition, offset, len(events), inserted, len(events) - inserted,
            )
            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        producer.flush(5)
