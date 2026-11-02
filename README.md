# 🌦️ Weather Streaming Dashboard: Open-Meteo → Kafka → Spark → MongoDB → Streamlit

A big-data streaming pipeline that collects **real weather data** from a public API, streams it
through **Apache Kafka**, processes it with **Spark Structured Streaming**, stores it in **MongoDB**,
and presents it in a multi-page **Streamlit** dashboard. The dashboard has a live monitor, the full
history, aggregation analytics and formatted exports.

**Pipeline used: MongoDB-Kafka-Spark**

```
 Open-Meteo API ──► producer.py ──► Kafka topic ──► Spark Structured Streaming ──► MongoDB ──► Streamlit
  (HTTPS/JSON)      (Kafka producer)  weather-stream   clean · validate · aggregate   raw + windows   History / Analytics / Export
                                          │
                                          └──────────── live consumer ───────────────────────────► Live Monitor (every 15 s)
```

---

## 🚀 Quick start (recommended: Docker, one command)

Requirements: [Docker Desktop](https://www.docker.com/products/docker-desktop/).

```bash
git clone https://github.com/shift-dee13/big-data-streamlit-template.git
cd big-data-streamlit-template
docker compose up -d --build
```

Open **http://localhost:8501**. Within about a minute, the first readings appear and every status
light in the sidebar turns green.

| Service | URL / port | What it is |
|---|---|---|
| Dashboard | http://localhost:8501 | Streamlit app |
| Spark UI | http://localhost:4040 | Streaming query progress |
| Kafka | localhost:9092 | Broker (KRaft mode, no ZooKeeper) |
| MongoDB | localhost:27017 | Database `weather_pipeline` |

Useful commands:
```bash
docker compose logs -f producer spark   # watch data flowing
docker compose ps                       # service health
docker compose down                     # stop (data is kept in volumes)
docker compose down -v                  # stop and delete all data
```

## 🛠️ Manual setup (without Docker)

Prerequisites: Python 3.10+, **Java 17** (for Spark), a running Kafka broker on `localhost:9092`
and MongoDB on `localhost:27017`. You can start just Kafka and MongoDB with Docker:
`docker compose up -d kafka mongodb`.

```bash
conda create -n bigdata python=3.10.13
conda activate bigdata
pip install -r requirements.txt

# Terminal 1: producer (Open-Meteo -> Kafka)
python producer.py

# Terminal 2: Spark streaming job (Kafka -> Spark -> MongoDB)
python spark_streaming.py

# Terminal 3: dashboard
streamlit run app.py
```

Producer options:
```bash
python producer.py --once --dry-run          # test the API without Kafka (prints messages)
python producer.py --interval 30             # poll every 30 s
python producer.py --cities "Tokyo:35.68:139.69,Seoul:37.57:126.98"
```

All settings (broker, topic, MongoDB URI, cities, time zone, Spark window and more) can be changed
with environment variables. See [`.env.example`](.env.example).

---

## 🧭 Using the dashboard

The sidebar has page navigation, a **System status** panel (Kafka, MongoDB, Spark, data freshness)
and a **Refresh now** button. Connection settings are in a collapsible section.

| Page | What you can do |
|---|---|
| 🏠 **Overview** | Status of every pipeline component, totals, a weather card for each city, and shortcuts to the other pages |
| 📡 **Live Monitor** | Watch messages arrive **directly from Kafka**. Auto-refreshes every **15 s** (adjustable from 5 to 60 s, or off). Includes weather cards with change arrows, a live trend chart, throughput per minute, and the latest message feed with Kafka partition and offset. Falls back to MongoDB if Kafka is down. |
| 🗂️ **History** | **All collected data** from MongoDB (default: *All time*). Filter by time range (1 h / 24 h / 7 d / 30 d / custom dates), city and metric. Switch between a long and a wide table. Interactive charts with a range slider, plus distribution box plots. |
| 📊 **Analytics** | **Data aggregation:** 15-min / hourly / 6-h / daily / weekly buckets with avg and a min–max band, city comparison (avg ± std dev, extremes), hour-of-day heatmap, correlation matrix, and the **Spark windowed aggregates** |
| 📥 **Export** | Choose a dataset (raw, aggregated or summary), filter it, then download it as **Excel (.xlsx)**, **PDF report**, **CSV**, **JSON**, **PNG** or **JPG** |
| ℹ️ **About** | Architecture diagram and the discussion below |

---

## 📝 Discussion

### 1. API used: Open-Meteo Weather API
[Open-Meteo](https://open-meteo.com) is a free, open-source weather API. It needs **no
registration or API key** and returns JSON over HTTPS. It combines forecast models from national
weather services (NOAA, DWD, JMA, and others) and updates them every 15 minutes.

- **Endpoint:** `GET https://api.open-meteo.com/v1/forecast?latitude=…&longitude=…&current=…`
- **Data collected (`current=`):** temperature, apparent temperature, relative humidity, precipitation,
  mean sea-level pressure, wind speed, cloud cover, and the WMO weather code (e.g. 3 = overcast,
  63 = rain, 95 = thunderstorm).
- **Locations:** Manila, Quezon City, Baguio, Cebu City and Davao City, fetched in a **single request**
  by passing comma-separated coordinates.
- **Rate limits:** the free tier allows about 10,000 calls per day, counting each location. The
  producer polls every 60 s (5 × 1,440 ≈ 7,200 calls/day) and retries failed calls with
  exponential back-off.

Each API response is flattened into one Kafka message per city and metric. Each message has the
required fields `timestamp` (ISO-8601), `value` (numeric), `metric_type` and `sensor_id`, plus
`location`, `unit`, coordinates, the API's own observation time and the weather description.

### 2. Pipeline implemented: MongoDB · Kafka · Spark

| Stage | Component | Responsibility |
|---|---|---|
| Ingestion | `producer.py` → **Kafka** | Publishes JSON readings to the `weather-stream` topic (3 partitions). The message key is `sensor_id`, so each city's readings stay in order. `acks=all` and retries make delivery reliable. Kafka also decouples the API from processing: if Spark restarts, messages wait in the topic. |
| Processing | **Spark Structured Streaming** (`spark_streaming.py`) | **Query 1:** parses JSON against an explicit schema, drops malformed records, converts timestamps, rounds values, and adds a `quality` flag for values outside physically plausible ranges. Writes to MongoDB with idempotent upserts. **Query 2:** windowed aggregation (avg, min, max, std dev, count per city and metric) over **5-minute tumbling event-time windows** with a **15-minute watermark** for late data. Checkpoints let both queries resume where they left off. |
| Storage | **MongoDB** | `weather_raw` holds every cleaned reading. `weather_agg_windowed` holds the Spark windows. `pipeline_status` holds Spark heartbeats. Indexes on time, city and metric. Upserting on a deterministic `_id` means replaying Kafka never creates duplicates. |
| Serving | **Streamlit** (`app.py`) | **Real-time path:** the Live Monitor consumes Kafka directly. **Historical path:** History, Analytics and Export query MongoDB. Aggregations such as `$dateTrunc`, `$group` and `$hour` run inside MongoDB, so only small results reach the browser. |

This is a **dual-pipeline (lambda-style)** architecture. The *speed layer* (Kafka → Live Monitor)
shows data seconds after it is published. The *storage layer* (Kafka → Spark → MongoDB) keeps a
durable, validated, queryable history plus pre-computed aggregates.

**Error handling:** API retries with back-off, Kafka connection retries at start-up, schema
validation and quality flags in Spark, `failOnDataLoss=false` and checkpointing, MongoDB
time-outs, a Kafka-to-MongoDB fallback in the Live Monitor, and friendly messages in every page
when a component is down or a filter matches no data.

### 3. Significance of the data dashboard
- **Situational awareness:** live conditions for several cities on one screen, refreshed every 15
  seconds. This helps disaster-risk teams, farmers, event planners, logistics and commuters,
  especially in a typhoon-prone country like the Philippines.
- **Historical insight:** because every reading is kept, users can spot trends, compare cities,
  find the hottest or most humid hours of the day, and see how metrics relate (humidity usually
  falls as temperature rises).
- **Data you can trust:** Spark validates every record, and the dashboard shows whether each part
  of the pipeline is healthy and how fresh the data is.
- **Reporting and decision support:** one-click exports to Excel, CSV, JSON, a formatted PDF
  report or chart images make the data ready for presentations and further analysis.
- **Scalability:** Kafka partitions, Spark executors and MongoDB can each scale out on their own,
  so the same design would handle thousands of IoT sensors instead of five cities.

---

## ⭐ Additional features
- **Data aggregation:** Spark streaming windows, plus MongoDB time buckets, per-city statistics,
  an hour-of-day profile and a correlation matrix
- **Improved UI/UX:** grouped page navigation, a live status panel, weather cards, a reusable
  filter bar with quick time ranges, an empty state that explains what to start, smooth partial
  refresh (only the live panel reloads), and charts with range sliders and hover details
- **Formatted export:** Excel (styled headers, frozen header row, filters, a report-info sheet and
  a summary sheet), PDF report (summary table, charts, data sample), CSV, JSON, PNG and JPG

## 📁 Project structure
```
app.py               Streamlit dashboard (6 pages)
producer.py          Kafka producer: Open-Meteo API -> Kafka
spark_streaming.py   Spark Structured Streaming: Kafka -> clean/aggregate -> MongoDB
storage.py           MongoDB queries and aggregation pipelines used by the dashboard
exports.py           CSV / JSON / Excel / PDF / PNG / JPG builders
config.py            Shared settings (env-overridable), cities, metrics, weather codes
docker-compose.yml   Kafka + MongoDB + producer + Spark + dashboard
Dockerfile           Image for the producer and dashboard
Dockerfile.spark     Image for the Spark job
requirements.txt     Python dependencies
```

## 🩺 Troubleshooting
| Symptom | Fix |
|---|---|
| Sidebar shows **Kafka 🔴** | `docker compose ps`; Kafka takes about 20 s to become healthy on first start |
| **Spark 🔴 no heartbeat yet** | The first start downloads the Kafka connector (needs internet). Run `docker compose logs -f spark` |
| "No data yet" | Check `docker compose logs producer` for API errors; the first batch appears within about a minute |
| Values change only every ~15 min | Expected. Open-Meteo refreshes its *current* conditions every 15 minutes. New readings still arrive every poll, so the live stream keeps flowing. |
| Running Spark locally fails | Spark 3.5 needs **Java 17**; check with `java -version` |
