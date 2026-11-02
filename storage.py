"""
MongoDB query layer for the dashboard.

All heavy lifting (filtering, grouping, time bucketing) is pushed down to MongoDB
aggregation pipelines so the dashboard stays fast as the data grows.
Every function returns a pandas DataFrame with UTC-aware timestamps.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Iterable, Optional

import pandas as pd
from pymongo import DESCENDING, MongoClient

import config

_clients: dict = {}


def get_client(uri: str) -> MongoClient:
    if uri not in _clients:
        _clients[uri] = MongoClient(uri, serverSelectionTimeoutMS=3000, connectTimeoutMS=3000)
    return _clients[uri]


def get_db(uri: str, db: str):
    return get_client(uri)[db]


def ping(uri: str) -> tuple[bool, str]:
    try:
        get_client(uri).admin.command("ping")
        return True, "connected"
    except Exception as e:  # pragma: no cover - depends on environment
        return False, str(e).split(",")[0][:150]


def _utc(df: pd.DataFrame, cols: Iterable[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], utc=True)
    return df


def _match(start: Optional[datetime], end: Optional[datetime], cities, metrics, time_field="timestamp") -> dict:
    q: dict = {}
    if start or end:
        q[time_field] = {}
        if start:
            q[time_field]["$gte"] = start
        if end:
            q[time_field]["$lte"] = end
    if cities:
        q["location"] = {"$in": list(cities)}
    if metrics:
        q["metric_type"] = {"$in": list(metrics)}
    return q


# ---------------------------------------------------------------------------
# Overview helpers
# ---------------------------------------------------------------------------
def collection_stats(uri: str, db: str) -> dict:
    d = get_db(uri, db)
    raw = d[config.RAW_COLLECTION]
    total = raw.estimated_document_count()
    first = raw.find_one({}, {"timestamp": 1}, sort=[("timestamp", 1)])
    last = raw.find_one({}, {"timestamp": 1}, sort=[("timestamp", -1)])
    return {
        "total_records": total,
        "agg_windows": d[config.AGG_COLLECTION].estimated_document_count(),
        "first": pd.Timestamp(first["timestamp"], tz="UTC") if first else None,
        "last": pd.Timestamp(last["timestamp"], tz="UTC") if last else None,
        "cities": sorted(raw.distinct("location")),
        "metrics": [m for m in config.METRIC_ORDER if m in set(raw.distinct("metric_type"))],
    }


def pipeline_status(uri: str, db: str) -> dict:
    out = {}
    for doc in get_db(uri, db)[config.STATUS_COLLECTION].find():
        if doc.get("last_batch_at"):
            doc["last_batch_at"] = pd.Timestamp(doc["last_batch_at"], tz="UTC")
        out[doc["_id"]] = doc
    return out


def latest_by_city(uri: str, db: str) -> pd.DataFrame:
    """Most recent reading for every (city, metric)."""
    coll = get_db(uri, db)[config.RAW_COLLECTION]
    last = coll.find_one({}, {"timestamp": 1}, sort=[("timestamp", -1)])
    if not last:
        return pd.DataFrame()
    pipeline = [
        # only scan the last day before the newest reading (fast even with a long history)
        {"$match": {"timestamp": {"$gte": last["timestamp"] - timedelta(days=1)}}},
        {"$sort": {"timestamp": -1}},
        {"$group": {"_id": {"location": "$location", "metric_type": "$metric_type"},
                    "doc": {"$first": "$$ROOT"}}},
        {"$replaceRoot": {"newRoot": "$doc"}},
        {"$project": {"_id": 0}},
    ]
    rows = list(coll.aggregate(pipeline, allowDiskUse=True))
    return _utc(pd.DataFrame(rows), ["timestamp", "ingested_at"])


# ---------------------------------------------------------------------------
# Raw data
# ---------------------------------------------------------------------------
RAW_FIELDS = {"_id": 0, "timestamp": 1, "location": 1, "sensor_id": 1, "metric_type": 1, "value": 1,
              "unit": 1, "weather_desc": 1, "observed_at": 1, "quality": 1, "latitude": 1, "longitude": 1}


def query_raw(uri: str, db: str, start=None, end=None, cities=None, metrics=None,
              limit: int = 0) -> pd.DataFrame:
    cur = (get_db(uri, db)[config.RAW_COLLECTION]
           .find(_match(start, end, cities, metrics), RAW_FIELDS)
           .sort("timestamp", DESCENDING))
    if limit:
        cur = cur.limit(limit)
    df = pd.DataFrame(list(cur))
    if df.empty:
        return pd.DataFrame(columns=[c for c in RAW_FIELDS if c != "_id"])
    return _utc(df, ["timestamp"]).sort_values("timestamp").reset_index(drop=True)


def query_recent(uri: str, db: str, minutes: int, cities=None) -> pd.DataFrame:
    start = datetime.now(timezone.utc) - timedelta(minutes=minutes)
    return query_raw(uri, db, start=start, cities=cities)


# ---------------------------------------------------------------------------
# Aggregation
# ---------------------------------------------------------------------------
GRANULARITY = {
    # label: ($dateTrunc unit, binSize)
    "5 minutes": ("minute", 5),
    "15 minutes": ("minute", 15),
    "Hourly": ("hour", 1),
    "6 hours": ("hour", 6),
    "Daily": ("day", 1),
    "Weekly": ("week", 1),
}


GRANULARITY_WIDTH = {"5 minutes": pd.Timedelta(minutes=5), "15 minutes": pd.Timedelta(minutes=15),
                     "Hourly": pd.Timedelta(hours=1), "6 hours": pd.Timedelta(hours=6),
                     "Daily": pd.Timedelta(days=1), "Weekly": pd.Timedelta(weeks=1)}


def suggest_granularity(span) -> str:
    """Pick a period that gives a readable number of points for how much data exists."""
    if span is None:
        return "Hourly"
    hours = span.total_seconds() / 3600
    if hours < 3:
        return "5 minutes"
    if hours < 12:
        return "15 minutes"
    if hours < 24 * 4:
        return "Hourly"
    if hours < 24 * 30:
        return "6 hours"
    return "Daily"


def aggregate(uri: str, db: str, granularity: str, start=None, end=None, cities=None, metrics=None,
              tz: str = "UTC") -> pd.DataFrame:
    """Time-bucketed statistics computed inside MongoDB ($dateTrunc, MongoDB 5.0+)."""
    unit, size = GRANULARITY[granularity]
    pipeline = [
        {"$match": {**_match(start, end, cities, metrics), "quality": {"$ne": "out_of_range"}}},
        {"$group": {
            "_id": {
                "bucket": {"$dateTrunc": {"date": "$timestamp", "unit": unit, "binSize": size, "timezone": tz}},
                "location": "$location", "metric_type": "$metric_type", "unit": "$unit"},
            "avg": {"$avg": "$value"}, "min": {"$min": "$value"}, "max": {"$max": "$value"},
            "stddev": {"$stdDevPop": "$value"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "bucket": "$_id.bucket", "location": "$_id.location",
                      "metric_type": "$_id.metric_type", "unit": "$_id.unit",
                      "avg": {"$round": ["$avg", 2]}, "min": 1, "max": 1,
                      "stddev": {"$round": ["$stddev", 3]}, "count": 1}},
        {"$sort": {"bucket": 1}},
    ]
    df = pd.DataFrame(list(get_db(uri, db)[config.RAW_COLLECTION].aggregate(pipeline, allowDiskUse=True)))
    return _utc(df, ["bucket"])


def summary_stats(uri: str, db: str, start=None, end=None, cities=None, metrics=None) -> pd.DataFrame:
    """Overall statistics per city and metric."""
    pipeline = [
        {"$match": {**_match(start, end, cities, metrics), "quality": {"$ne": "out_of_range"}}},
        {"$group": {"_id": {"location": "$location", "metric_type": "$metric_type", "unit": "$unit"},
                    "avg": {"$avg": "$value"}, "min": {"$min": "$value"}, "max": {"$max": "$value"},
                    "stddev": {"$stdDevPop": "$value"}, "count": {"$sum": 1},
                    "first": {"$min": "$timestamp"}, "last": {"$max": "$timestamp"}}},
        {"$project": {"_id": 0, "location": "$_id.location", "metric_type": "$_id.metric_type",
                      "unit": "$_id.unit", "avg": {"$round": ["$avg", 2]}, "min": 1, "max": 1,
                      "stddev": {"$round": ["$stddev", 3]}, "count": 1, "first": 1, "last": 1}},
        {"$sort": {"metric_type": 1, "location": 1}},
    ]
    df = pd.DataFrame(list(get_db(uri, db)[config.RAW_COLLECTION].aggregate(pipeline, allowDiskUse=True)))
    return _utc(df, ["first", "last"])


def hourly_profile(uri: str, db: str, metric: str, start=None, end=None, cities=None,
                   tz: str = "UTC") -> pd.DataFrame:
    """Average value by local hour of day and city (for the heatmap)."""
    pipeline = [
        {"$match": {**_match(start, end, cities, [metric]), "quality": {"$ne": "out_of_range"}}},
        {"$group": {"_id": {"hour": {"$hour": {"date": "$timestamp", "timezone": tz}}, "location": "$location"},
                    "avg": {"$avg": "$value"}, "count": {"$sum": 1}}},
        {"$project": {"_id": 0, "hour": "$_id.hour", "location": "$_id.location",
                      "avg": {"$round": ["$avg", 2]}, "count": 1}},
        {"$sort": {"hour": 1}},
    ]
    return pd.DataFrame(list(get_db(uri, db)[config.RAW_COLLECTION].aggregate(pipeline, allowDiskUse=True)))


def spark_windows(uri: str, db: str, start=None, end=None, cities=None, metrics=None) -> pd.DataFrame:
    """Windowed aggregates computed in real time by the Spark streaming job."""
    cur = (get_db(uri, db)[config.AGG_COLLECTION]
           .find(_match(start, end, cities, metrics, time_field="window_start"), {"_id": 0})
           .sort("window_start", 1))
    return _utc(pd.DataFrame(list(cur)), ["window_start", "window_end", "updated_at"])
