"""
Shared configuration for the Weather Streaming Pipeline.

Every setting can be overridden with an environment variable (see .env.example),
so the same code runs both locally and inside Docker without edits.

Pipeline:  Open-Meteo API -> Kafka -> Spark Structured Streaming -> MongoDB -> Streamlit
"""

from __future__ import annotations

import os

# ---------------------------------------------------------------------------
# Connection settings
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "weather-stream")

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "weather_pipeline")
RAW_COLLECTION = "weather_raw"            # every cleaned reading written by Spark
AGG_COLLECTION = "weather_agg_windowed"   # Spark windowed aggregates
STATUS_COLLECTION = "pipeline_status"     # Spark heartbeats (last batch, row counts)

# Spark
SPARK_CHECKPOINT_DIR = os.getenv("SPARK_CHECKPOINT_DIR", "./checkpoints")
SPARK_AGG_WINDOW = os.getenv("SPARK_AGG_WINDOW", "5 minutes")
SPARK_WATERMARK = os.getenv("SPARK_WATERMARK", "15 minutes")

# Dashboard
DISPLAY_TIMEZONE = os.getenv("DISPLAY_TIMEZONE", "Asia/Manila")
LIVE_REFRESH_SECONDS = int(os.getenv("LIVE_REFRESH_SECONDS", "15"))

# Producer
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
# Open-Meteo's free tier allows about 10,000 calls/day and counts each location
# as a call. 5 cities polled every 60 s = 7,200 calls/day, safely under the limit.
PRODUCER_INTERVAL_SECONDS = int(os.getenv("PRODUCER_INTERVAL_SECONDS", "60"))

# ---------------------------------------------------------------------------
# Monitored locations ("sensors"). Override with
#   CITIES="Tokyo:35.68:139.69,Seoul:37.57:126.98"
# ---------------------------------------------------------------------------
DEFAULT_CITIES = [
    {"name": "Manila", "lat": 14.5995, "lon": 120.9842},
    {"name": "Quezon City", "lat": 14.6760, "lon": 121.0437},
    {"name": "Baguio", "lat": 16.4023, "lon": 120.5960},
    {"name": "Cebu City", "lat": 10.3157, "lon": 123.8854},
    {"name": "Davao City", "lat": 7.1907, "lon": 125.4553},
]


def parse_cities(spec: str | None) -> list[dict]:
    """Parse 'Name:lat:lon,Name:lat:lon' into a list of city dicts."""
    if not spec:
        return list(DEFAULT_CITIES)
    cities = []
    for chunk in spec.split(","):
        parts = chunk.strip().split(":")
        if len(parts) != 3:
            raise ValueError(f"Invalid city spec '{chunk}'. Use Name:lat:lon")
        cities.append({"name": parts[0].strip(), "lat": float(parts[1]), "lon": float(parts[2])})
    return cities


CITIES = parse_cities(os.getenv("CITIES"))


def sensor_id_for(city_name: str) -> str:
    """Stable sensor id for a city, e.g. 'Quezon City' -> 'om-quezon-city'."""
    return "om-" + "-".join(city_name.lower().split())


# ---------------------------------------------------------------------------
# Metrics taken from the API.  api variable -> metadata
# 'valid' is the physically plausible range Spark uses for data-quality flags.
# ---------------------------------------------------------------------------
METRICS = {
    "temperature_2m":       {"metric_type": "temperature",   "unit": "°C",   "label": "Temperature",   "valid": (-40, 60)},
    "apparent_temperature": {"metric_type": "feels_like",    "unit": "°C",   "label": "Feels Like",    "valid": (-50, 70)},
    "relative_humidity_2m": {"metric_type": "humidity",      "unit": "%",    "label": "Humidity",      "valid": (0, 100)},
    "precipitation":        {"metric_type": "precipitation", "unit": "mm",   "label": "Precipitation", "valid": (0, 500)},
    "pressure_msl":         {"metric_type": "pressure",      "unit": "hPa",  "label": "Pressure",      "valid": (870, 1090)},
    "wind_speed_10m":       {"metric_type": "wind_speed",    "unit": "km/h", "label": "Wind Speed",    "valid": (0, 400)},
    "cloud_cover":          {"metric_type": "cloud_cover",   "unit": "%",    "label": "Cloud Cover",   "valid": (0, 100)},
}

# metric_type -> metadata (handy for the dashboard)
METRIC_INFO = {m["metric_type"]: m for m in METRICS.values()}
METRIC_ORDER = [m["metric_type"] for m in METRICS.values()]

# WMO weather interpretation codes used by Open-Meteo
WEATHER_CODES = {
    0: ("Clear sky", "☀️"), 1: ("Mainly clear", "🌤️"), 2: ("Partly cloudy", "⛅"), 3: ("Overcast", "☁️"),
    45: ("Fog", "🌫️"), 48: ("Rime fog", "🌫️"),
    51: ("Light drizzle", "🌦️"), 53: ("Drizzle", "🌦️"), 55: ("Dense drizzle", "🌧️"),
    56: ("Freezing drizzle", "🌧️"), 57: ("Freezing drizzle", "🌧️"),
    61: ("Slight rain", "🌦️"), 63: ("Rain", "🌧️"), 65: ("Heavy rain", "🌧️"),
    66: ("Freezing rain", "🌧️"), 67: ("Freezing rain", "🌧️"),
    71: ("Slight snow", "🌨️"), 73: ("Snow", "🌨️"), 75: ("Heavy snow", "❄️"), 77: ("Snow grains", "🌨️"),
    80: ("Rain showers", "🌦️"), 81: ("Rain showers", "🌧️"), 82: ("Violent showers", "⛈️"),
    85: ("Snow showers", "🌨️"), 86: ("Snow showers", "❄️"),
    95: ("Thunderstorm", "⛈️"), 96: ("Thunderstorm w/ hail", "⛈️"), 99: ("Thunderstorm w/ hail", "⛈️"),
}


def describe_weather(code) -> tuple[str, str]:
    try:
        return WEATHER_CODES.get(int(code), ("Unknown", "🌡️"))
    except (TypeError, ValueError):
        return ("Unknown", "🌡️")
