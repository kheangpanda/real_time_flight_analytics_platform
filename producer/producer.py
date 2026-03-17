"""
producer/producer.py
━━━━━━━━━━━━━━━━━━━━
OpenSky Network → Apache Kafka producer.

• Polls the OpenSky REST API every POLL_INTERVAL seconds.
• Transforms raw state-vector arrays into clean JSON records.
• Publishes each record as a separate Kafka message keyed by icao24.
• Implements exponential-back-off retry for both API calls and Kafka delivery.
"""
from __future__ import annotations

import json
import logging
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional

import requests
from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

# ─────────────────────────────────────────────────────────────────────────────
# Configuration (from environment)
# ─────────────────────────────────────────────────────────────────────────────
KAFKA_BROKER       = os.getenv("KAFKA_BROKER", "localhost:9092")
OPENSKY_API_URL    = os.getenv("OPENSKY_API_URL", "https://opensky-network.org/api/states/all")
KAFKA_TOPIC        = "flight_stream"
POLL_INTERVAL      = int(os.getenv("POLL_INTERVAL_SEC", "10"))
REQUEST_TIMEOUT    = int(os.getenv("REQUEST_TIMEOUT_SEC", "30"))
MAX_RETRIES        = int(os.getenv("MAX_RETRIES", "5"))
BACKOFF_BASE       = float(os.getenv("BACKOFF_BASE", "2.0"))

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("flight-producer")


# ─────────────────────────────────────────────────────────────────────────────
# Kafka topic bootstrap
# ─────────────────────────────────────────────────────────────────────────────
def ensure_topic(broker: str, topic: str, partitions: int = 3) -> None:
    """Create the Kafka topic if it does not already exist."""
    admin = AdminClient({"bootstrap.servers": broker})
    existing = admin.list_topics(timeout=10).topics
    if topic in existing:
        logger.info("Topic '%s' already exists.", topic)
        return

    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=1)
    fs = admin.create_topics([new_topic])
    for t, f in fs.items():
        try:
            f.result()
            logger.info("Topic '%s' created successfully.", t)
        except Exception as exc:  # noqa: BLE001
            if "TOPIC_ALREADY_EXISTS" in str(exc):
                logger.info("Topic '%s' already exists (race).", t)
            else:
                logger.error("Failed to create topic '%s': %s", t, exc)
                raise


# ─────────────────────────────────────────────────────────────────────────────
# Delivery report callback
# ─────────────────────────────────────────────────────────────────────────────
def _delivery_report(err: Optional[Exception], msg: Any) -> None:
    if err is not None:
        logger.warning("Delivery failed for key %s: %s", msg.key(), err)
    else:
        logger.debug(
            "Delivered icao24=%s → topic=%s partition=%d offset=%d",
            msg.key().decode() if msg.key() else "N/A",
            msg.topic(),
            msg.partition(),
            msg.offset(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Single-field safe extractors
# ─────────────────────────────────────────────────────────────────────────────
def _f(lst: list, idx: int) -> Optional[float]:
    try:
        v = lst[idx]
        return float(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None


def _i(lst: list, idx: int) -> Optional[int]:
    try:
        v = lst[idx]
        return int(v) if v is not None else None
    except (IndexError, TypeError, ValueError):
        return None


def _s(lst: list, idx: int) -> Optional[str]:
    try:
        v = lst[idx]
        return str(v).strip() if v is not None else None
    except (IndexError, TypeError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# State vector → dict
# ─────────────────────────────────────────────────────────────────────────────
def _classify_altitude(alt: Optional[float]) -> str:
    if alt is None:
        return "unknown"
    if alt <= 0:
        return "ground"
    if alt < 1_000:
        return "low"
    if alt < 6_000:
        return "medium"
    if alt < 12_000:
        return "high"
    return "very_high"


def _classify_region(lon: Optional[float], lat: Optional[float]) -> str:
    if lon is None or lat is None:
        return "unknown"
    if lat > 60:
        return "arctic"
    if lat < -60:
        return "antarctic"
    if -30 < lon < 60 and 0 < lat < 60:
        return "europe_africa"
    if 60 < lon < 150 and 0 < lat < 60:
        return "asia_pacific"
    if -180 < lon < -30 and 0 < lat < 60:
        return "north_america"
    if -90 < lon < -30 and -60 < lat < 0:
        return "south_america"
    return "other"


def state_to_record(state: list, event_ts: int) -> Dict[str, Any]:
    lon  = _f(state, 5)
    lat  = _f(state, 6)
    baro = _f(state, 7)
    vel  = _f(state, 9)

    return {
        "event_id":        str(uuid.uuid4()),
        "icao24":          _s(state, 0),
        "callsign":        _s(state, 1),
        "origin_country":  _s(state, 2),
        "time_position":   _i(state, 3),
        "last_contact":    _i(state, 4),
        "longitude":       lon,
        "latitude":        lat,
        "baro_altitude":   baro,
        "geo_altitude":    _f(state, 13),
        "on_ground":       bool(state[8]) if len(state) > 8 and state[8] is not None else None,
        "velocity":        vel,
        "velocity_kmh":    round(vel * 3.6, 2) if vel is not None else None,
        "true_track":      _f(state, 10),
        "vertical_rate":   _f(state, 11),
        "squawk":          _s(state, 14),
        "spi":             bool(state[15]) if len(state) > 15 and state[15] is not None else None,
        "position_source": _i(state, 16),
        "altitude_level":  _classify_altitude(baro),
        "flight_region":   _classify_region(lon, lat),
        "event_timestamp": event_ts,
    }


# ─────────────────────────────────────────────────────────────────────────────
# OpenSky API fetch with retry
# ─────────────────────────────────────────────────────────────────────────────
def fetch_states(session: requests.Session) -> Optional[Dict[str, Any]]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = session.get(OPENSKY_API_URL, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 429:
                wait = BACKOFF_BASE ** attempt
                logger.warning("Rate-limited (429). Sleeping %.1fs…", wait)
                time.sleep(wait)
                continue
            logger.warning("API returned HTTP %d", resp.status_code)
        except requests.exceptions.Timeout:
            logger.warning("API request timed out (attempt %d/%d)", attempt, MAX_RETRIES)
        except requests.exceptions.ConnectionError as exc:
            logger.warning("Connection error (attempt %d/%d): %s", attempt, MAX_RETRIES, exc)
        except Exception as exc:  # noqa: BLE001
            logger.error("Unexpected API error: %s", exc)

        if attempt < MAX_RETRIES:
            wait = BACKOFF_BASE ** attempt
            logger.info("Retrying in %.1fs…", wait)
            time.sleep(wait)

    logger.error("All %d API attempts exhausted.", MAX_RETRIES)
    return None


# ─────────────────────────────────────────────────────────────────────────────
# Kafka producer factory with retry
# ─────────────────────────────────────────────────────────────────────────────
def create_producer() -> Producer:
    conf = {
        "bootstrap.servers": KAFKA_BROKER,
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 1000,
        "compression.type": "snappy",
        "batch.size": 32768,
        "linger.ms": 50,
        "max.in.flight.requests.per.connection": 5,
        "enable.idempotence": "true",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            p = Producer(conf)
            logger.info("Kafka producer connected to %s", KAFKA_BROKER)
            return p
        except KafkaException as exc:
            wait = BACKOFF_BASE ** attempt
            logger.warning(
                "Kafka connection failed (attempt %d/%d): %s — retrying in %.1fs",
                attempt, MAX_RETRIES, exc, wait,
            )
            time.sleep(wait)
    raise RuntimeError(f"Cannot connect to Kafka broker at {KAFKA_BROKER}")


# ─────────────────────────────────────────────────────────────────────────────
# Main loop
# ─────────────────────────────────────────────────────────────────────────────
def main() -> None:
    logger.info("Starting Flight Analytics Producer")
    logger.info("  Kafka broker : %s", KAFKA_BROKER)
    logger.info("  OpenSky URL  : %s", OPENSKY_API_URL)
    logger.info("  Poll interval: %ds", POLL_INTERVAL)

    # Wait for Kafka to be ready
    time.sleep(15)

    ensure_topic(KAFKA_BROKER, KAFKA_TOPIC)
    producer = create_producer()
    session  = requests.Session()
    session.headers.update({"User-Agent": "FlightAnalyticsPlatform/1.0"})

    poll_count = 0
    while True:
        cycle_start = time.monotonic()
        poll_count += 1

        logger.info("=== Poll cycle #%d ===", poll_count)

        data = fetch_states(session)
        if data is None:
            logger.warning("No data — skipping cycle.")
        else:
            states      = data.get("states") or []
            event_ts    = data.get("time", int(time.time()))
            published   = 0
            skipped     = 0

            for state in states:
                if state is None or state[0] is None:
                    skipped += 1
                    continue
                try:
                    record = state_to_record(state, event_ts)
                    payload = json.dumps(record, default=str).encode("utf-8")
                    key = (record["icao24"] or "unknown").encode("utf-8")
                    producer.produce(
                        KAFKA_TOPIC,
                        key=key,
                        value=payload,
                        callback=_delivery_report,
                    )
                    published += 1
                except Exception as exc:  # noqa: BLE001
                    logger.error("Error serialising/producing record: %s", exc)
                    skipped += 1

            producer.flush()
            logger.info(
                "Published %d records, skipped %d (total states: %d)",
                published, skipped, len(states),
            )

        elapsed = time.monotonic() - cycle_start
        sleep_for = max(0.0, POLL_INTERVAL - elapsed)
        logger.debug("Cycle took %.2fs — sleeping %.2fs", elapsed, sleep_for)
        time.sleep(sleep_for)


if __name__ == "__main__":
    main()
