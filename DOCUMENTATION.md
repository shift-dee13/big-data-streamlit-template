# Weather Streaming Dashboard: Project Documentation

**Big Data Streaming Application using the MongoDB–Kafka–Spark Pipeline**

| | |
|---|---|
| **Data source** | Open-Meteo Weather API (free, no API key) |
| **Pipeline** | Apache Kafka → Apache Spark Structured Streaming → MongoDB |
| **Application** | Streamlit web dashboard (Overview, Live Monitor, History, Analytics, Export, About) |
| **Repository** | https://github.com/shift-dee13/big-data-streamlit-template |

---

## Table of Contents
1. [Introduction](#1-introduction)
2. [System Architecture](#2-system-architecture)
3. [The API Used: Open-Meteo](#3-the-api-used-open-meteo)
4. [The Pipeline Implemented: MongoDB–Kafka–Spark](#4-the-pipeline-implemented-mongodbkafkaspark)
5. [Why This Pipeline Was Chosen](#5-why-this-pipeline-was-chosen)
6. [The Application: Data Dashboard](#6-the-application-data-dashboard)
7. [Additional Features](#7-additional-features)
8. [Significance of the Data Dashboard](#8-significance-of-the-data-dashboard)
9. [Significance of the Big Data Application](#9-significance-of-the-big-data-application)
10. [How to Run](#10-how-to-run)
11. [Limitations and Future Improvements](#11-limitations-and-future-improvements)
12. [Conclusion](#12-conclusion)

---

## 1. Introduction

Weather affects almost every part of daily life in the Philippines: commuting, farming, business
operations, school and work suspensions, and disaster preparedness. The country is hit by around
20 tropical cyclones a year, so timely and reliable weather information is very valuable.

This project builds a **big data streaming application** that continuously collects real weather
measurements for five Philippine cities (**Manila, Quezon City, Baguio, Cebu City and Davao City**).
It streams them through a scalable data pipeline, processes and stores them, and presents them in
an interactive dashboard with two views:

- a **real-time view** that shows new readings as they arrive (refreshed every 15 seconds), and
- a **historical view** that shows **all collected data**, with filtering, aggregation and export.

### Objectives
1. Ingest real data from a public **Application Programming Interface (API)**.
2. Implement a **MongoDB–Kafka–Spark** streaming pipeline.
3. Monitor the streamed data in a **live dashboard** that updates every 15 seconds.
4. Present **all collected data** in a history view.
5. Add **data aggregation**, an **improved UI/UX**, and **formatted data export** (Excel, PDF, CSV, JSON, PNG, JPG).

---

## 2. System Architecture

```
┌────────────────┐   HTTPS/JSON   ┌──────────────┐  JSON messages  ┌──────────────────┐
│ Open-Meteo API │ ─────────────► │ producer.py  │ ──────────────► │   Apache Kafka   │
│ (5 cities)     │  every 60 s    │ Kafka        │  key=sensor_id  │ topic:           │
└────────────────┘                │ Producer     │                 │ weather-stream   │
                                  └──────────────┘                 │ (3 partitions)   │
                                                                   └───┬──────────┬───┘
                                                 real-time path        │          │  processing path
                                          ┌────────────────────────────┘          ▼
                                          │                     ┌───────────────────────────────┐
                                          │                     │ Spark Structured Streaming    │
                                          │                     │ (spark_streaming.py)          │
                                          │                     │ • parse & validate JSON       │
                                          │                     │ • quality flags               │
                                          │                     │ • 5-min windowed aggregation  │
                                          │                     └───────────────┬───────────────┘
                                          │                                     │ upserts
                                          │                                     ▼
                                          │                     ┌───────────────────────────────┐
                                          │                     │ MongoDB (weather_pipeline)    │
                                          │                     │ • weather_raw                 │
                                          │                     │ • weather_agg_windowed        │
                                          │                     │ • pipeline_status             │
                                          │                     └───────────────┬───────────────┘
                                          ▼                                     ▼
                                  ┌─────────────────────────────────────────────────────────┐
                                  │             Streamlit Dashboard (app.py)                │
                                  │  Live Monitor ◄─ Kafka    History / Analytics / Export  │
                                  │  (every 15 s)              ◄─ MongoDB                   │
                                  └─────────────────────────────────────────────────────────┘
```

| Component | File | Technology |
|---|---|---|
| Data producer | `producer.py` | Python, `requests`, `kafka-python` |
| Message broker | `docker-compose.yml` (service `kafka`) | Apache Kafka 3.8 (KRaft mode) |
| Stream processor | `spark_streaming.py` | Apache Spark Structured Streaming (PySpark) |
| Database | `docker-compose.yml` (service `mongodb`) | MongoDB 7 |
| Query layer | `storage.py` | `pymongo`, MongoDB aggregation pipelines |
| Dashboard | `app.py` | Streamlit, Plotly |
| Export engine | `exports.py` | openpyxl, ReportLab, Matplotlib |
| Configuration | `config.py`, `.env.example` | Environment variables |

The design is a **dual-pipeline (lambda-style) architecture**:
- The **speed layer** (Kafka → Live Monitor) delivers data to the user within seconds.
- The **batch/serving layer** (Kafka → Spark → MongoDB) produces a durable, validated, queryable
  history and pre-computed aggregates.

---

## 3. The API Used: Open-Meteo

### 3.1 Overview
[Open-Meteo](https://open-meteo.com) is an **open-source weather API** that offers free access for
non-commercial use. It combines numerical weather prediction models from national weather services
such as NOAA (United States), DWD (Germany) and JMA (Japan) into high-resolution forecasts and
current conditions.

It was chosen for this project because:

| Reason | Explanation |
|---|---|
| **No API key or account** | Anyone (including the instructor) can run the project immediately |
| **Real, continuously changing data** | Current conditions are refreshed about every 15 minutes |
| **Multi-location requests** | All 5 cities are fetched in a single HTTP request |
| **Rich, numeric variables** | Many measurements per location, suitable for analytics |
| **Simple JSON format** | Easy to parse and convert into the required message schema |
| **Relevance** | Weather data has clear real-world value for the Philippines |

### 3.2 Endpoint and request
```
GET https://api.open-meteo.com/v1/forecast
    ?latitude=14.5995,14.676,16.4023,10.3157,7.1907
    &longitude=120.9842,121.0437,120.596,123.8854,125.4553
    &current=temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,
             pressure_msl,wind_speed_10m,cloud_cover,weather_code
    &timezone=GMT
    &wind_speed_unit=kmh
```

When several coordinates are given, the API returns a **list** with one result per location.

### 3.3 Variables collected

| API variable | `metric_type` | Unit | Meaning |
|---|---|---|---|
| `temperature_2m` | `temperature` | °C | Air temperature 2 m above ground |
| `apparent_temperature` | `feels_like` | °C | Perceived temperature (heat index / wind chill) |
| `relative_humidity_2m` | `humidity` | % | Relative humidity |
| `precipitation` | `precipitation` | mm | Rain / showers in the last interval |
| `pressure_msl` | `pressure` | hPa | Pressure reduced to mean sea level |
| `wind_speed_10m` | `wind_speed` | km/h | Wind speed 10 m above ground |
| `cloud_cover` | `cloud_cover` | % | Total cloud cover |
| `weather_code` | (metadata) | WMO code | Condition, e.g. 0 = clear, 63 = rain, 95 = thunderstorm |

### 3.4 Message schema
The producer flattens each API response into **one Kafka message per city per metric**
(5 cities × 7 metrics = 35 messages per poll). Every message has the required fields plus
metadata:

```json
{
  "timestamp":    "2026-11-02T08:31:00Z",
  "value":        33.2,
  "metric_type":  "temperature",
  "sensor_id":    "om-manila",
  "location":     "Manila",
  "unit":         "°C",
  "latitude":     14.5995,
  "longitude":    120.9842,
  "observed_at":  "2026-11-02T08:30",
  "weather_code": 1,
  "weather_desc": "Mainly clear",
  "source":       "open-meteo"
}
```

| Required field | Implementation |
|---|---|
| `timestamp` (ISO format) | UTC time the reading was fetched, ISO-8601 with `Z` suffix |
| `value` (numeric) | Measurement as a float, rounded to 2 decimals |
| `metric_type` (string) | One of the 7 metric names above |
| `sensor_id` (string) | One virtual "sensor" per city, e.g. `om-quezon-city` |

### 3.5 Rate limits and reliability
- The free tier allows about **10,000 API calls per day**, and each location counts as a call. The
  producer therefore polls every **60 seconds**: 5 locations × 1,440 polls/day ≈ **7,200 calls/day**,
  safely within the limit. The interval can be changed with `--interval` but never goes below 15 s.
- Failed requests (HTTP 429/5xx, timeouts) are **retried automatically with exponential back-off**.
  If the API stays unavailable, the producer waits longer between attempts (up to 10 minutes)
  instead of crashing.
- Malformed or missing values are skipped, so a single bad variable never stops the stream.

---

## 4. The Pipeline Implemented: MongoDB–Kafka–Spark

### 4.1 Stage 1: Ingestion (Producer → Kafka)
`producer.py` is a Kafka producer that:
1. Calls the Open-Meteo API on a fixed interval.
2. Converts the response into JSON messages using the schema above.
3. Publishes them to the Kafka topic **`weather-stream`**.

Key design decisions:
- **Message key = `sensor_id`.** Kafka sends all messages with the same key to the same partition,
  so each city's readings stay in **chronological order**.
- **`acks="all"` and `retries=5`.** A message only counts as sent once the broker has stored it,
  which gives reliable delivery.
- **Connection retries at start-up.** The producer waits for Kafka if the broker is still starting.
- **Graceful shutdown.** On `Ctrl+C` or `docker stop`, pending messages are flushed before exit.
- **`--dry-run` mode.** Prints messages without Kafka, which is useful for testing the API alone.

**Why Kafka?** Kafka is a distributed, append-only commit log. It **decouples** the data source
from the consumers: the producer does not need to know who reads the data, and consumers can
stop, restart or be added without losing messages. The topic keeps data for 7 days and has 3
partitions, so processing can run in parallel.

### 4.2 Stage 2: Processing (Kafka → Spark Structured Streaming)
`spark_streaming.py` subscribes to the topic with Spark Structured Streaming and runs **two
continuous streaming queries**.

**Query 1: clean, validate and store (`weather_raw`)**
1. **Parse** each Kafka message against an explicit schema (`from_json`).
2. **Filter** out malformed records (missing `sensor_id`, `metric_type`, `value` or `timestamp`).
3. **Transform:** convert the ISO timestamp to a Spark timestamp (`event_time`) and round values.
4. **Validate:** add a `quality` flag. A value is `ok` if it lies within a physically plausible
   range for its metric (e.g. humidity 0–100 %, pressure 870–1090 hPa), otherwise `out_of_range`.
5. **Write** each micro-batch (every 10 s) to MongoDB using **idempotent upserts**.

**Query 2: windowed aggregation (`weather_agg_windowed`)**
1. Only `quality = ok` readings are used.
2. Readings are grouped into **5-minute tumbling event-time windows** per city and metric.
3. A **15-minute watermark** lets slightly late data still update its window while bounding the
   memory Spark uses.
4. For each window, Spark computes **average, minimum, maximum, standard deviation and count**.
5. Updated windows are upserted into MongoDB every 15 s (`outputMode("update")`).

**Fault tolerance**
- **Checkpointing:** each query stores its Kafka offsets and state in a checkpoint directory.
  After a restart, Spark resumes exactly where it stopped.
- **`failOnDataLoss=false`:** the job keeps running if old Kafka data has expired.
- **Heartbeats:** every micro-batch writes a status record to `pipeline_status`. The dashboard
  uses it to show whether Spark is alive.

**Why Spark?** Spark Structured Streaming treats a stream as an unbounded table, so the same
DataFrame operations (filter, groupBy, window, aggregate) used in batch processing work on live
data. It scales from one laptop core to a cluster of machines without code changes and has
built-in support for event time, watermarks and exactly-once progress tracking.

### 4.3 Stage 3: Storage (MongoDB)
MongoDB stores the processed data in the database `weather_pipeline`:

| Collection | Content | Written by |
|---|---|---|
| `weather_raw` | Every cleaned reading, with quality flag and Kafka partition and offset | Spark Query 1 |
| `weather_agg_windowed` | 5-minute window statistics per city and metric | Spark Query 2 |
| `pipeline_status` | Spark heartbeats (last batch time, rows, totals) | Spark (both queries) |

- **Deterministic `_id`** (e.g. `om-manila|temperature|2026-11-02T08:31:00+00:00`) with upserts
  means re-processing the same Kafka messages **never creates duplicates**.
- **Indexes** on `timestamp` and on (`location`, `metric_type`, `timestamp`) keep dashboard queries
  fast as the data grows.
- **Aggregation pipelines** (`$match`, `$group`, `$dateTrunc`, `$hour`, `$stdDevPop`) let the
  database compute hourly, daily and weekly statistics itself. Only small results are sent to the
  dashboard.

**Why MongoDB?** The weather messages are JSON documents, and MongoDB stores them natively with
no schema conversion. Its flexible schema allows new metrics or metadata to be added later. It
also supports time-based aggregation and indexing, and scales horizontally with sharding.

### 4.4 Stage 4: Serving (Streamlit Dashboard)
- The **Live Monitor** consumes Kafka **directly** (the real-time path), so it shows data the moment
  the producer publishes it, independent of Spark's processing delay.
- **History, Analytics and Export** query **MongoDB** (the historical path), using the validated data
  and the aggregates that Spark produced.

### 4.5 Error handling across the pipeline

| Failure | How it is handled |
|---|---|
| API down / rate-limited | Automatic retries with back-off; the producer keeps running |
| Kafka not yet started | Producer retries the connection 20 times; Docker health checks order start-up |
| Malformed message | Dropped by Spark's schema and filters; skipped by the live consumer |
| Implausible value | Flagged `out_of_range` and excluded from aggregations |
| Spark restart | Resumes from checkpoints; upserts prevent duplicates |
| Kafka unreachable from dashboard | Live Monitor falls back to the newest MongoDB records |
| MongoDB unreachable | Friendly error message with instructions instead of a crash |
| Filter matches no data | Clear "no records" message |

---

## 5. Why This Pipeline Was Chosen

The project allowed either **MongoDB–Kafka–Spark** or **Hadoop (HDFS)–Kafka–Spark**.
MongoDB–Kafka–Spark was chosen for these reasons:

| Criterion | MongoDB | Hadoop HDFS |
|---|---|---|
| Data format | Stores JSON documents natively, matching the Kafka messages exactly | Stores files (CSV/Parquet); JSON must be converted and batched |
| Query speed for a dashboard | Indexed, millisecond queries on recent data | Designed for large batch scans; high latency for small interactive queries |
| Updates and upserts | Supports upserts, so writes are idempotent and deduplicated | Files are immutable; updating a record means rewriting files |
| Aggregation | Built-in aggregation pipeline (`$group`, `$dateTrunc`) | Needs Spark/Hive jobs for each query |
| Setup | One container | NameNode, DataNode and usually YARN; heavier on a laptop |
| Best use | Operational, interactive, near-real-time applications | Very large, write-once archives and offline batch analytics |

A dashboard needs **frequent, small, low-latency queries** on recent data and **live updates**.
That is MongoDB's strength. HDFS would be a better fit for an archive of many terabytes analyzed
in nightly batch jobs. In a larger production system, the two could be combined: MongoDB for the
dashboard and HDFS or a data lake for long-term cold storage.

---

## 6. The Application: Data Dashboard

The dashboard (`app.py`) is a multi-page Streamlit web application at `http://localhost:8501`.
It is designed to be **easy to use and navigate**.

### 6.1 Navigation and layout
- A **sidebar menu** groups the pages into *Monitor*, *Explore* and *Project*.
- A **System status** panel in the sidebar shows the health of every component: Kafka (messages in
  topic), MongoDB (connected), Spark (time since last batch) and Data (age of the latest reading),
  each with a green, yellow or red indicator.
- A **Refresh now** button and collapsible **Connection settings** (Kafka broker and topic, MongoDB
  URI, display time zone).
- All times are shown in the user's time zone (default: Asia/Manila).

### 6.2 Pages

| Page | Purpose | Main features |
|---|---|---|
| 🏠 **Overview** | Landing page | Status pills, totals (readings, cities, latest reading, collection time), a weather card for each city, shortcuts to other pages, a temperature comparison chart |
| 📡 **Live Monitor** | Real-time monitoring | Consumes **Kafka directly**; **auto-refreshes every 15 s** (adjustable 5–60 s or off); weather cards with ▲/▼ change indicators; live trend chart (15 min to 6 h window); throughput chart (messages/minute); latest message feed with Kafka partition and offset; automatic fallback to MongoDB |
| 🗂️ **History** | All collected data | Shows **all data by default**; filters for time range (1 h, 24 h, 7 d, 30 d, all time, custom dates), city and metric; long or wide table view; interactive charts with a range slider; distribution box plots; quick CSV and Excel download |
| 📊 **Analytics** | Data aggregation | Trends at 15-min, hourly, 6-h, daily or weekly granularity with a min–max band; city comparison (average ± std dev, extremes); hour-of-day heatmap; correlation matrix and temperature vs humidity scatter; Spark 5-minute window results |
| 📥 **Export** | Formatted export | Choose a dataset (raw, aggregated, summary), apply filters, choose a format: **Excel, PDF report, CSV, JSON, PNG, JPG** |
| ℹ️ **About** | Documentation | Architecture diagram and the discussion of the API, pipeline and significance |

### 6.3 Live updates
The Live Monitor uses a Streamlit **fragment** that reruns only the live panel on a timer, so the
rest of the page (filters and settings) does not reload or flicker. On each refresh, it fetches
every new Kafka message since the last update and adds it to a rolling 6-hour buffer for the
session. On first open, it replays recent messages from Kafka so the charts are not empty.

---

## 7. Additional Features

### 7.1 Data aggregation
Aggregation happens at **three levels**:
1. **Spark (streaming):** 5-minute tumbling windows with avg, min, max, std dev and count,
   computed continuously as data arrives.
2. **MongoDB (on demand):** time buckets (15 min to weekly) with `$dateTrunc`, per-city summary
   statistics, and hour-of-day averages with `$hour` in the local time zone.
3. **Dashboard (pandas):** correlation matrix between metrics, per-minute message throughput.

### 7.2 Improved UI/UX
- Grouped sidebar navigation and shortcut cards on the Overview page
- Live health indicators for every pipeline component
- Weather cards with icons, "feels like" temperature and change arrows
- One reusable filter bar on every page, with quick time-range buttons
- Helpful empty states (e.g. "No data yet: start the producer…") instead of errors
- Smooth partial refresh on the live page
- Charts with hover details, range sliders and min–max bands; large selections are automatically
  down-sampled to keep the browser responsive
- Time zone selector; consistent colours and layout

### 7.3 Formatted data export

| Format | Content |
|---|---|
| **Excel (.xlsx)** | Report-info sheet (filters, time range, row count), data sheet and summary sheet; styled header row, frozen header, auto-filter, fitted column widths, date formatting |
| **PDF report** | Title and generation time, applied filters, summary statistics table, charts (hourly temperature, humidity, pressure and wind trends; average temperature by city), and a data sample; page numbers and footer |
| **CSV** | UTF-8 with BOM so Excel shows symbols like "°C" correctly |
| **JSON** | Records with ISO dates |
| **PNG / JPG** | High-resolution chart of the hourly average of a chosen metric |

---

## 8. Significance of the Data Dashboard

1. **Real-time situational awareness.** Current temperature, heat index, humidity, rain, wind and
   pressure for five cities across Luzon, Visayas and Mindanao, all on one screen and refreshed
   every 15 seconds. Users can spot sudden changes, such as a pressure drop or heavy rainfall, as
   they happen.

2. **Public safety and disaster preparedness.** Falling sea-level pressure, rising wind speed and
   heavy precipitation are early signs of tropical storms. Local government units, disaster
   risk-reduction offices and schools can use this kind of monitoring to support decisions about
   suspensions, evacuations and resource deployment.

3. **Health.** The *feels like* temperature (heat index) shows heat stress. In the sample data,
   Quezon City measured 33.7 °C but felt like 37.6 °C. This helps decide when outdoor work, classes
   or events should be limited.

4. **Historical insight and pattern discovery.** Because every reading is stored, users can answer
   questions such as:
   - Which city is the hottest or most humid on average? (City comparison)
   - At what hour of the day is it hottest? (Hour-of-day heatmap)
   - How do metrics relate? Humidity usually falls as temperature rises. (Correlation matrix)
   - How did conditions change over the past week? (Trends)

5. **Comparing regions.** Baguio (highland, about 20 °C) and the lowland cities (about 29–34 °C)
   show clear climate differences. This is useful for tourism, agriculture and energy planning.

6. **Trust and transparency.** The status panel and data-quality flags show users whether the data
   is current and valid, which is essential before acting on it.

7. **Reporting and decision support.** One-click exports to Excel and PDF turn live data into
   documents for meetings, reports or further analysis, with no manual copying.

---

## 9. Significance of the Big Data Application

### 9.1 The "V"s of big data in this project

| Characteristic | How it appears in this project |
|---|---|
| **Volume** | 35 readings per poll → about 50,000 readings per day. Over a year, that is about 18 million documents for just 5 cities; with hundreds of stations or IoT sensors it reaches billions. |
| **Velocity** | Data arrives **continuously**. It must be ingested, processed and displayed within seconds, which is a streaming problem rather than a one-time batch job. |
| **Variety** | Different metrics, units and value ranges, numeric and categorical (weather codes) data, plus metadata, all in semi-structured JSON. |
| **Veracity** | Real-world data can be missing or wrong. Spark validates every record, flags implausible values and drops malformed ones. |
| **Value** | Raw readings are turned into trends, comparisons, patterns and reports that support decisions. |

### 9.2 Why a big data pipeline instead of a simple script
A simple script that calls the API and writes to a file would work for a few readings, but it would
not:
- **scale** to thousands of sensors (Kafka partitions and Spark executors scale horizontally),
- **survive failures** without losing or duplicating data (Kafka retention, Spark checkpoints,
  idempotent upserts),
- **serve multiple consumers** at once (the dashboard and Spark both read the same Kafka topic
  independently), or
- **process data in real time** with event-time windows and late-data handling.

Each technology does one job and can be scaled or replaced on its own. This is the
**loose coupling** that real big data systems rely on.

### 9.3 Real-world applications of the same design
The same architecture is used in industry for:
- **National weather and hazard monitoring:** thousands of automatic weather stations streaming
  readings for flood and typhoon early warnings
- **Smart cities and IoT:** air quality, traffic and energy sensors
- **Agriculture:** rainfall and temperature monitoring for planting and irrigation decisions
- **Transportation and logistics:** rerouting flights, ships and deliveries around bad weather
- **Finance and e-commerce:** real-time transaction streams, fraud detection and live analytics

Replacing the Open-Meteo producer with another data source (for example stock prices, sensor
readings or website clicks) would reuse the whole pipeline with minimal changes.

---

## 10. How to Run

### Option A: Docker (whole pipeline)
```bash
docker compose up -d --build
```
Open **http://localhost:8501**. The Spark UI is at http://localhost:4040.

### Option B: Python in VS Code + Docker for infrastructure (Windows example)
```powershell
docker compose up -d kafka mongodb spark     # Kafka, MongoDB and the Spark job

py -3.12 -m venv .venv                       # Python 3.10–3.13 (not 3.14)
.venv\Scripts\Activate.ps1
pip install -r requirements.txt

python producer.py                           # terminal 1
streamlit run app.py                         # terminal 2
```

Useful commands:

| Command | Purpose |
|---|---|
| `python producer.py --once --dry-run` | Test the API without Kafka |
| `docker compose ps` | Check service health |
| `docker compose logs -f spark` | Watch Spark micro-batches |
| `docker compose stop` / `start` | Pause / resume (data is kept) |
| `docker compose down -v` | Remove everything, including data |

---

## 11. Limitations and Future Improvements

| Limitation | Possible improvement |
|---|---|
| Open-Meteo refreshes current conditions about every 15 minutes, so values repeat between updates | Add more frequent sources (e.g. PAGASA or IoT weather stations) |
| Single-node Kafka, Spark and MongoDB | Run Kafka with replication, a Spark cluster and a MongoDB replica set or sharded cluster |
| Only 5 cities | Add more locations with the `CITIES` setting (within API limits) |
| No alerting | Add threshold alerts (e.g. heat index > 41 °C, wind > 60 km/h) by email or SMS |
| No forecasting | Train a machine-learning model on the history to predict the next hours |
| Local deployment | Deploy to the cloud (e.g. managed Kafka, MongoDB Atlas, Databricks) |

---

## 12. Conclusion

This project implements a complete **big data streaming application** on the
**MongoDB–Kafka–Spark** pipeline:
- **Open-Meteo** provides real, continuously updated weather data through a free API.
- **Kafka** reliably ingests and buffers the stream.
- **Spark Structured Streaming** cleans, validates and aggregates it in real time.
- **MongoDB** stores it for fast interactive queries.

The **Streamlit dashboard** turns the data into information people can act on: live monitoring
every 15 seconds, a complete history, multi-level aggregation, and professional exports. Beyond
the weather use case, the project shows the core principles of modern data engineering:
decoupled components, fault tolerance, scalability and the transformation of raw data into value.
