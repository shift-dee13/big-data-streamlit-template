"""
Kafka Producer - Real Weather Data from the Open-Meteo API
==========================================================

Polls the free Open-Meteo forecast API (https://open-meteo.com, no API key needed)
for current weather in several cities and publishes one Kafka message per
(city, metric) reading.

Message schema (JSON):
{
    "timestamp":    "2026-11-02T07:15:00Z",   # ISO-8601 UTC time the reading was fetched
    "value":        29.4,                     # numeric measurement
    "metric_type":  "temperature",            # temperature | humidity | pressure | ...
    "sensor_id":    "om-manila",              # one virtual "sensor" per city
    "location":     "Manila",
    "unit":         "°C",
    "latitude":     14.5995,
    "longitude":    120.9842,
    "observed_at":  "2026-11-02T07:15",       # model time reported by the API
    "weather_code": 3,
    "weather_desc": "Overcast",
    "source":       "open-meteo"
}

Usage:
    python producer.py                      # stream forever (every 60 s)
    python producer.py --interval 30        # custom poll interval
    python producer.py --once --dry-run     # fetch once and print, no Kafka needed
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import config

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError, NoBrokersAvailable
except ImportError:  # allows --dry-run without kafka-python installed
    KafkaProducer = None
    KafkaError = NoBrokersAvailable = Exception


def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}", flush=True)


class OpenMeteoClient:
    """Small HTTP client for the Open-Meteo forecast API with retries."""

    def __init__(self, cities: List[dict], timeout: int = 15):
        self.cities = cities
        self.timeout = timeout
        self.session = requests.Session()
        retry = Retry(total=4, backoff_factor=1.5,
                      status_forcelist=(429, 500, 502, 503, 504),
                      allowed_methods=("GET",))
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def fetch_current(self) -> List[dict]:
        """Fetch current conditions for all cities in ONE request.

        Returns a list with one API result per city (same order as self.cities).
        """
        params = {
            "latitude": ",".join(str(c["lat"]) for c in self.cities),
            "longitude": ",".join(str(c["lon"]) for c in self.cities),
            "current": ",".join(list(config.METRICS.keys()) + ["weather_code"]),
            "timezone": "GMT",
            "wind_speed_unit": "kmh",
        }
        resp = self.session.get(config.OPEN_METEO_URL, params=params, timeout=self.timeout)
        resp.raise_for_status()
        payload = resp.json()
        # A single location returns an object, several locations return a list
        results = payload if isinstance(payload, list) else [payload]
        if len(results) != len(self.cities):
            raise ValueError(f"Expected {len(self.cities)} results, got {len(results)}")
        for r in results:
            if r.get("error"):
                raise ValueError(f"API error: {r.get('reason')}")
        return results


def to_messages(api_results: List[dict], cities: List[dict], fetched_at: datetime) -> List[Dict[str, Any]]:
    """Turn raw API results into flat Kafka messages (one per city and metric)."""
    ts = fetched_at.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    messages = []
    for city, result in zip(cities, api_results):
        current = result.get("current") or {}
        code = current.get("weather_code")
        desc, _ = config.describe_weather(code)
        for api_var, meta in config.METRICS.items():
            value = current.get(api_var)
            if value is None:
                continue  # variable missing for this location - skip it
            try:
                value = float(value)
            except (TypeError, ValueError):
                continue
            messages.append({
                "timestamp": ts,
                "value": round(value, 2),
                "metric_type": meta["metric_type"],
                "sensor_id": config.sensor_id_for(city["name"]),
                "location": city["name"],
                "unit": meta["unit"],
                "latitude": city["lat"],
                "longitude": city["lon"],
                "observed_at": current.get("time"),
                "weather_code": code,
                "weather_desc": desc,
                "source": "open-meteo",
            })
    return messages


class StreamingDataProducer:
    """Kafka producer that streams real weather readings from Open-Meteo."""

    def __init__(self, bootstrap_servers: str, topic: str, cities: List[dict], dry_run: bool = False):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.cities = cities
        self.dry_run = dry_run
        self.api = OpenMeteoClient(cities)
        self.producer = None
        self.sent_total = 0
        self.failed_total = 0
        self._running = True
        if not dry_run:
            self.producer = self._connect()

    def _connect(self, attempts: int = 20, delay: int = 5):
        """Connect to Kafka, retrying so the producer can start before the broker."""
        if KafkaProducer is None:
            log("ERROR: kafka-python is not installed. Run: pip install -r requirements.txt")
            sys.exit(1)
        for attempt in range(1, attempts + 1):
            try:
                producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers.split(","),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8"),
                    acks="all",
                    retries=5,
                    linger_ms=50,
                )
                log(f"Connected to Kafka at {self.bootstrap_servers}, topic '{self.topic}'")
                return producer
            except NoBrokersAvailable:
                log(f"Kafka not reachable ({attempt}/{attempts}) - retrying in {delay}s...")
                time.sleep(delay)
        log("ERROR: could not connect to Kafka. Is the broker running?")
        sys.exit(1)

    def send_messages(self, messages: List[Dict[str, Any]]) -> int:
        """Send a batch of messages. Key = sensor_id keeps each city's order in one partition."""
        if self.dry_run:
            for m in messages:
                print(json.dumps(m, ensure_ascii=False))
            return len(messages)
        futures = [self.producer.send(self.topic, key=m["sensor_id"], value=m) for m in messages]
        self.producer.flush(timeout=30)
        ok = 0
        for f in futures:
            try:
                f.get(timeout=10)
                ok += 1
            except KafkaError as e:
                self.failed_total += 1
                log(f"Kafka send error: {e}")
        return ok

    def poll_once(self) -> int:
        fetched_at = datetime.now(timezone.utc)
        results = self.api.fetch_current()
        messages = to_messages(results, self.cities, fetched_at)
        sent = self.send_messages(messages)
        self.sent_total += sent
        temps = ", ".join(
            f"{m['location']} {m['value']}{m['unit']}" for m in messages if m["metric_type"] == "temperature"
        )
        log(f"Sent {sent} readings for {len(self.cities)} cities | {temps}")
        return sent

    def produce_stream(self, interval: int, duration: int | None = None, once: bool = False):
        log(f"Streaming Open-Meteo data every {interval}s for: {', '.join(c['name'] for c in self.cities)}")
        start = time.time()
        backoff = interval
        while self._running:
            cycle_start = time.time()
            try:
                self.poll_once()
                backoff = interval
            except requests.RequestException as e:
                backoff = min(backoff * 2, 600)
                log(f"API request failed: {e}. Next attempt in {backoff}s")
            except (ValueError, KeyError) as e:
                backoff = min(backoff * 2, 600)
                log(f"Bad API response: {e}. Next attempt in {backoff}s")
            if once or (duration and time.time() - start >= duration):
                break
            # sleep in small steps so Ctrl+C / docker stop is handled quickly
            wait_until = cycle_start + backoff
            while self._running and time.time() < wait_until:
                time.sleep(0.5)
        self.close()

    def stop(self, *_):
        log("Stop signal received, shutting down...")
        self._running = False

    def close(self):
        if self.producer:
            try:
                self.producer.flush(timeout=10)
                self.producer.close()
            except Exception as e:
                log(f"Error closing producer: {e}")
        log(f"Producer stopped. Sent {self.sent_total} messages ({self.failed_total} failed).")


def parse_arguments():
    p = argparse.ArgumentParser(description="Stream real weather data from Open-Meteo into Kafka")
    p.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS,
                   help=f"Kafka brokers (default: {config.KAFKA_BOOTSTRAP_SERVERS})")
    p.add_argument("--topic", default=config.KAFKA_TOPIC, help=f"Kafka topic (default: {config.KAFKA_TOPIC})")
    p.add_argument("--interval", type=int, default=config.PRODUCER_INTERVAL_SECONDS,
                   help="Seconds between API polls (default: %(default)s)")
    p.add_argument("--cities", default=None,
                   help="Cities as 'Name:lat:lon,Name:lat:lon' (default: 5 Philippine cities)")
    p.add_argument("--duration", type=int, default=None, help="Stop after N seconds (default: run forever)")
    p.add_argument("--once", action="store_true", help="Fetch and send a single batch, then exit")
    p.add_argument("--dry-run", action="store_true", help="Print messages instead of sending to Kafka")
    return p.parse_args()


def main():
    args = parse_arguments()
    cities = config.parse_cities(args.cities) if args.cities else config.CITIES
    if args.interval < 15:
        log("Interval below 15s would exceed Open-Meteo's free limits; using 15s.")
        args.interval = 15

    producer = StreamingDataProducer(args.bootstrap_servers, args.topic, cities, dry_run=args.dry_run)
    signal.signal(signal.SIGINT, producer.stop)
    signal.signal(signal.SIGTERM, producer.stop)
    producer.produce_stream(args.interval, args.duration, args.once)


if __name__ == "__main__":
    main()
