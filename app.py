"""
Weather Streaming Dashboard  (Streamlit)
========================================

Pipeline:  Open-Meteo API -> Kafka -> Spark Structured Streaming -> MongoDB -> this dashboard

Pages
  🏠 Overview        system status + current conditions in every city
  📡 Live Monitor    real-time stream straight from Kafka, auto-refreshes every 15 s
  🗂️ History         every reading ever collected (from MongoDB) with filters
  📊 Analytics       aggregations: time buckets, city comparison, hourly profile, correlations,
                     and the windowed aggregates computed by Spark
  📥 Export          CSV / Excel / JSON / PDF report / PNG / JPG
  ℹ️ About           API, pipeline and why the dashboard matters

Run:  streamlit run app.py
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

import config
import exports
import storage

st.set_page_config(page_title="Weather Stream Dashboard", page_icon="🌦️", layout="wide",
                   initial_sidebar_state="expanded")

PALETTE = exports.PALETTE
TIME_RANGES = ["Last 1h", "Last 24h", "Last 7d", "Last 30d", "All time", "Custom"]
RANGE_DELTA = {"Last 1h": timedelta(hours=1), "Last 24h": timedelta(days=1),
               "Last 7d": timedelta(days=7), "Last 30d": timedelta(days=30)}
TIMEZONES = ["Asia/Manila", "Asia/Shanghai", "Asia/Singapore", "Asia/Tokyo", "UTC",
             "Europe/London", "America/New_York", "America/Los_Angeles"]

st.markdown("""
<style>
.block-container {padding-top: 1.6rem;}
.wx-card {border: 1px solid rgba(128,128,128,.25); border-radius: 14px; padding: 14px 16px;
          background: linear-gradient(160deg, rgba(37,99,235,.10), rgba(16,185,129,.06)); height: 100%;}
.wx-city {font-weight: 700; font-size: 1.05rem;}
.wx-desc {opacity: .8; font-size: .9rem; margin-bottom: 4px;}
.wx-temp {font-size: 2.1rem; font-weight: 800; line-height: 1.1;}
.wx-delta-up {color: #EF4444; font-size: .85rem; font-weight: 600;}
.wx-delta-down {color: #2563EB; font-size: .85rem; font-weight: 600;}
.wx-sub {opacity: .75; font-size: .85rem; margin-bottom: 8px;}
.wx-grid {display: grid; grid-template-columns: 1fr 1fr; gap: 4px 10px; font-size: .85rem;}
.wx-time {opacity: .6; font-size: .75rem; margin-top: 8px;}
.status-pill {display:inline-block; padding: 3px 10px; border-radius: 999px; font-size: .8rem;
              font-weight: 600; margin-right: 6px; border: 1px solid rgba(128,128,128,.25);}
.ok {background: rgba(16,185,129,.15);} .warn {background: rgba(234,179,8,.18);} .bad {background: rgba(239,68,68,.15);}
.hint {opacity: .7; font-size: .85rem;}
</style>
""", unsafe_allow_html=True)


# =============================================================================
# Settings (sidebar) - defaults come from config.py / environment variables
# =============================================================================
def settings() -> dict:
    ss = st.session_state
    ss.setdefault("kafka_broker", config.KAFKA_BOOTSTRAP_SERVERS)
    ss.setdefault("kafka_topic", config.KAFKA_TOPIC)
    ss.setdefault("mongo_uri", config.MONGO_URI)
    ss.setdefault("mongo_db", config.MONGO_DB)
    ss.setdefault("tz", config.DISPLAY_TIMEZONE if config.DISPLAY_TIMEZONE in TIMEZONES else "UTC")
    return {k: ss[k] for k in ("kafka_broker", "kafka_topic", "mongo_uri", "mongo_db", "tz")}


def tz() -> ZoneInfo:
    return ZoneInfo(settings()["tz"])


def metric_label(m: str) -> str:
    info = config.METRIC_INFO.get(m)
    return f"{info['label']} ({info['unit']})" if info else m


def unit_of(m: str) -> str:
    return config.METRIC_INFO.get(m, {}).get("unit", "")


def ago(ts) -> str:
    if ts is None or pd.isna(ts):
        return "never"
    secs = (pd.Timestamp.now(tz="UTC") - pd.Timestamp(ts)).total_seconds()
    if secs < 60:
        return f"{max(0, int(secs))}s ago"
    if secs < 3600:
        return f"{int(secs // 60)} min ago"
    if secs < 86400:
        return f"{secs / 3600:.1f} h ago"
    return f"{secs / 86400:.1f} days ago"


def localize(df: pd.DataFrame, cols) -> pd.DataFrame:
    """UTC-aware -> naive local time (for display, charts and exports)."""
    df = df.copy()
    for c in cols:
        if c in df.columns and isinstance(df[c].dtype, pd.DatetimeTZDtype):
            df[c] = df[c].dt.tz_convert(tz()).dt.tz_localize(None)
    return df


# =============================================================================
# Cached data access (short TTLs keep the UI fast but fresh)
# =============================================================================
@st.cache_data(ttl=15, show_spinner=False)
def c_stats(uri, db):
    return storage.collection_stats(uri, db)


@st.cache_data(ttl=10, show_spinner=False)
def c_status(uri, db):
    return storage.pipeline_status(uri, db)


@st.cache_data(ttl=15, show_spinner=False)
def c_latest(uri, db):
    return storage.latest_by_city(uri, db)


@st.cache_data(ttl=30, show_spinner="Loading data from MongoDB...")
def c_raw(uri, db, start, end, cities, metrics):
    return storage.query_raw(uri, db, start, end, cities, metrics)


@st.cache_data(ttl=60, show_spinner="Aggregating in MongoDB...")
def c_agg(uri, db, gran, start, end, cities, metrics, tzname):
    return storage.aggregate(uri, db, gran, start, end, cities, metrics, tzname)


@st.cache_data(ttl=60, show_spinner=False)
def c_summary(uri, db, start, end, cities, metrics):
    return storage.summary_stats(uri, db, start, end, cities, metrics)


@st.cache_data(ttl=60, show_spinner=False)
def c_hourly(uri, db, metric, start, end, cities, tzname):
    return storage.hourly_profile(uri, db, metric, start, end, cities, tzname)


@st.cache_data(ttl=20, show_spinner=False)
def c_spark(uri, db, start, end, cities, metrics):
    return storage.spark_windows(uri, db, start, end, cities, metrics)


@st.cache_data(ttl=10, show_spinner=False)
def c_mongo_ping(uri):
    return storage.ping(uri)


@st.cache_data(ttl=15, show_spinner=False)
def kafka_status(broker: str, topic: str) -> dict:
    try:
        from kafka import KafkaConsumer, TopicPartition
        c = KafkaConsumer(bootstrap_servers=broker.split(","), group_id=None,
                          api_version_auto_timeout_ms=3000)
        try:
            parts = c.partitions_for_topic(topic)
            if not parts:
                return {"ok": True, "topic": False, "messages": 0, "partitions": 0}
            tps = [TopicPartition(topic, p) for p in parts]
            end, begin = c.end_offsets(tps), c.beginning_offsets(tps)
            return {"ok": True, "topic": True, "partitions": len(parts),
                    "messages": int(sum(end[tp] - begin[tp] for tp in tps))}
        finally:
            c.close()
    except Exception as e:
        return {"ok": False, "error": str(e)[:150]}


def load_stats_or_stop() -> dict:
    """Load collection stats; show a friendly help box (and stop the page) if MongoDB is down."""
    s = settings()
    try:
        return c_stats(s["mongo_uri"], s["mongo_db"])
    except Exception as e:
        st.error(f"**Cannot reach MongoDB at `{s['mongo_uri']}`.**\n\n{str(e)[:200]}")
        st.info("Start the stack with `docker compose up -d`, or check the MongoDB URI in "
                "**⚙️ Connection settings** in the sidebar.")
        st.stop()


def empty_state(stats: dict) -> bool:
    if stats["total_records"] == 0:
        st.info("**No data yet.** MongoDB is connected but empty.\n\n"
                "1. Start the producer: `python producer.py`\n"
                "2. Start Spark: `python spark_streaming.py`\n\n"
                "With Docker, `docker compose up -d` runs everything. The first readings arrive "
                "within about a minute.")
        return True
    return False


# =============================================================================
# Reusable filter bar
# =============================================================================
def floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


def filter_bar(key: str, stats: dict, default_range: str = "All time", metrics: bool = True,
               single_metric: bool = False):
    """Time range + cities + metrics. Returns (start_utc, end_utc, cities_tuple, metrics_tuple)."""
    zone = tz()
    with st.container(border=True):
        c1, c2, c3 = st.columns([1.6, 1.2, 1.2])
        with c1:
            rng = st.segmented_control("Time range", TIME_RANGES, default=default_range,
                                       key=f"{key}_range") or default_range
            start = end = None
            if rng in RANGE_DELTA:
                start = floor_minute(datetime.now(timezone.utc)) - RANGE_DELTA[rng]
            elif rng == "Custom":
                first = stats["first"].tz_convert(zone).date()
                last = stats["last"].tz_convert(zone).date()
                picked = st.date_input("Dates", value=(first, last), min_value=first, max_value=last,
                                       key=f"{key}_dates", label_visibility="collapsed")
                if isinstance(picked, (list, tuple)) and len(picked) == 2:
                    d0, d1 = picked
                else:
                    d0 = d1 = picked[0] if isinstance(picked, (list, tuple)) else picked
                start = datetime(d0.year, d0.month, d0.day, tzinfo=zone).astimezone(timezone.utc)
                end = (datetime(d1.year, d1.month, d1.day, tzinfo=zone) + timedelta(days=1)
                       - timedelta(microseconds=1)).astimezone(timezone.utc)
        with c2:
            cities = st.multiselect("Cities", stats["cities"], key=f"{key}_cities", placeholder="All cities")
        with c3:
            sel_metrics = ()
            if single_metric:
                sel_metrics = (st.selectbox("Metric", stats["metrics"], format_func=metric_label,
                                            key=f"{key}_metric"),)
            elif metrics:
                sel_metrics = tuple(st.multiselect("Metrics", stats["metrics"], format_func=metric_label,
                                                   key=f"{key}_metrics", placeholder="All metrics"))
    return start, end, tuple(cities), sel_metrics


def describe_filters(start, end, cities, metrics) -> dict:
    zone = tz()
    fmt = lambda d: d.astimezone(zone).strftime("%Y-%m-%d %H:%M") if d else None  # noqa: E731
    return {
        "Time range": f"{fmt(start) or 'beginning'}  to  {fmt(end) or 'now'}  ({settings()['tz']})",
        "Cities": ", ".join(cities) if cities else "All",
        "Metrics": ", ".join(metric_label(m) for m in metrics) if metrics else "All",
        "Data source": "Open-Meteo API via Kafka -> Spark -> MongoDB",
    }


# =============================================================================
# Small UI components
# =============================================================================
def weather_card(city: str, readings: dict, when, prev_temp=None) -> str:
    def v(m, fmt="{:.1f}"):
        r = readings.get(m)
        return fmt.format(r) if r is not None and not pd.isna(r) else "–"
    desc, icon = config.describe_weather(readings.get("_code"))
    delta = ""
    t = readings.get("temperature")
    if prev_temp is not None and t is not None and abs(t - prev_temp) >= 0.05:
        cls = "wx-delta-up" if t > prev_temp else "wx-delta-down"
        delta = f' <span class="{cls}">{"▲" if t > prev_temp else "▼"} {abs(t - prev_temp):.1f}</span>'
    when_txt = pd.Timestamp(when).tz_convert(tz()).strftime("%b %d, %H:%M") if when is not None else "–"
    return f"""
<div class="wx-card">
  <div class="wx-city">{city}</div>
  <div class="wx-desc">{icon} {desc}</div>
  <div class="wx-temp">{v('temperature')}°C{delta}</div>
  <div class="wx-sub">Feels like {v('feels_like')}°C</div>
  <div class="wx-grid">
    <span>💧 {v('humidity', '{:.0f}')}%</span><span>💨 {v('wind_speed')} km/h</span>
    <span>🧭 {v('pressure', '{:.0f}')} hPa</span><span>🌧️ {v('precipitation')} mm</span>
    <span>☁️ {v('cloud_cover', '{:.0f}')}%</span>
  </div>
  <div class="wx-time">Updated {when_txt}</div>
</div>"""


def city_cards(latest: pd.DataFrame, previous: dict | None = None, per_row: int = 5):
    """Render a grid of weather cards from a long-format 'latest readings' DataFrame."""
    if latest.empty:
        st.caption("No readings yet.")
        return
    cities = sorted(latest["location"].unique())
    for i in range(0, len(cities), per_row):
        cols = st.columns(per_row)
        for col, city in zip(cols, cities[i:i + per_row]):
            sub = latest[latest["location"] == city]
            readings = dict(zip(sub["metric_type"], sub["value"]))
            if "weather_code" in sub.columns and sub["weather_code"].notna().any():
                readings["_code"] = sub["weather_code"].dropna().iloc[0]
            when = sub["timestamp"].max()
            with col:
                st.markdown(weather_card(city, readings, when, (previous or {}).get(city)),
                            unsafe_allow_html=True)


def pill(text: str, level: str) -> str:
    return f'<span class="status-pill {level}">{text}</span>'


def system_status() -> list[tuple[str, str, str]]:
    """[(name, level, detail)] for Kafka, Spark, MongoDB and data freshness."""
    s = settings()
    out = []
    k = kafka_status(s["kafka_broker"], s["kafka_topic"])
    if not k["ok"]:
        out.append(("Kafka", "bad", "broker unreachable"))
    elif not k["topic"]:
        out.append(("Kafka", "warn", "topic not created yet"))
    else:
        out.append(("Kafka", "ok", f"{k['messages']:,} msgs in topic"))

    ok, msg = c_mongo_ping(s["mongo_uri"])
    out.append(("MongoDB", "ok" if ok else "bad", "connected" if ok else "unreachable"))

    if ok:
        try:
            status = c_status(s["mongo_uri"], s["mongo_db"])
            hb = status.get("spark_raw", {}).get("last_batch_at")
            if hb is None:
                out.append(("Spark", "bad", "no heartbeat yet"))
            else:
                age = (pd.Timestamp.now(tz="UTC") - hb).total_seconds()
                out.append(("Spark", "ok" if age < 90 else "warn" if age < 600 else "bad", f"batch {ago(hb)}"))
            last = c_stats(s["mongo_uri"], s["mongo_db"])["last"]
            if last is None:
                out.append(("Data", "bad", "no readings"))
            else:
                age = (pd.Timestamp.now(tz="UTC") - last).total_seconds()
                limit = config.PRODUCER_INTERVAL_SECONDS
                out.append(("Data", "ok" if age < limit * 3 else "warn" if age < limit * 20 else "bad",
                            f"latest {ago(last)}"))
        except Exception:
            out.append(("Spark", "bad", "unknown"))
    else:
        out.append(("Spark", "bad", "unknown (MongoDB down)"))
    return out


ICON = {"ok": "🟢", "warn": "🟡", "bad": "🔴"}


def sidebar():
    s = settings()
    with st.sidebar:
        st.markdown("### System status")
        for name, level, detail in system_status():
            st.markdown(f"{ICON[level]} **{name}** · <span class='hint'>{detail}</span>", unsafe_allow_html=True)
        if st.button("🔄 Refresh now", width="stretch"):
            st.cache_data.clear()
            st.rerun()

        with st.expander("⚙️ Connection settings"):
            st.text_input("Kafka broker", key="kafka_broker")
            st.text_input("Kafka topic", key="kafka_topic")
            st.text_input("MongoDB URI", key="mongo_uri")
            st.text_input("MongoDB database", key="mongo_db")
            st.selectbox("Display time zone", TIMEZONES, key="tz")
        st.caption(f"Times shown in **{s['tz']}** · data: Open-Meteo")


def plot_defaults(fig, height=380, ytitle=None):
    fig.update_layout(height=height, margin=dict(l=10, r=10, t=40, b=10), hovermode="x unified",
                      legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title=None))
    if ytitle:
        fig.update_yaxes(title=ytitle)
    fig.update_xaxes(title=None)
    return fig


# =============================================================================
# Page 1: Overview
# =============================================================================
def page_overview():
    s = settings()
    st.title("🌦️ Weather Streaming Dashboard")
    st.caption("Real weather from the **Open-Meteo API**, streamed through **Kafka**, processed by "
               "**Spark Structured Streaming** and stored in **MongoDB**.")

    st.markdown(" ".join(pill(f"{ICON[l]} {n}: {d}", l) for n, l, d in system_status()), unsafe_allow_html=True)
    st.write("")

    stats = load_stats_or_stop()
    if empty_state(stats):
        return

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Readings stored", f"{stats['total_records']:,}")
    k2.metric("Cities monitored", len(stats["cities"]))
    k3.metric("Latest reading", ago(stats["last"]))
    span = stats["last"] - stats["first"]
    k4.metric("Collecting for", f"{span.days} d {span.seconds // 3600} h" if span.days else
              f"{span.seconds // 3600} h {(span.seconds % 3600) // 60} min")

    st.subheader("Current conditions")
    latest = c_latest(s["mongo_uri"], s["mongo_db"])
    city_cards(latest)

    st.subheader("Where would you like to go?")
    nav = [
        ("pages/live", "Live Monitor", "Watch new readings arrive from Kafka, refreshed every 15 seconds."),
        ("pages/history", "History", "Browse and filter every reading collected so far."),
        ("pages/analytics", "Analytics", "Aggregates, city comparisons, daily patterns and correlations."),
        ("pages/export", "Export", "Download data as CSV, Excel, JSON, a PDF report or chart images."),
    ]
    cols = st.columns(4)
    for col, (page, title, text) in zip(cols, nav):
        with col, st.container(border=True):
            st.page_link(PAGES[page], label=title)
            st.caption(text)

    if not latest.empty:
        st.subheader("Temperature right now")
        t = latest[latest["metric_type"] == "temperature"].sort_values("value", ascending=False)
        fig = px.bar(t, x="location", y="value", text="value", color="location",
                     color_discrete_sequence=PALETTE)
        fig.update_traces(texttemplate="%{text:.1f}°C", textposition="outside", showlegend=False)
        st.plotly_chart(plot_defaults(fig, 320, "°C"), width="stretch")


# =============================================================================
# Page 2: Live Monitor (Kafka consumer)
# =============================================================================
LIVE_BACKLOG_PER_PARTITION = 600   # on first connect, replay recent messages so charts aren't empty
LIVE_BUFFER_HOURS = 6


def _safe_json(b: bytes):
    try:
        return json.loads(b.decode("utf-8"))
    except (ValueError, UnicodeDecodeError):
        return None  # malformed message - skipped in poll_kafka


def _live_consumer(broker: str, topic: str):
    key = f"live_consumer::{broker}::{topic}"
    if st.session_state.get(key) is not None:
        return st.session_state[key]
    from kafka import KafkaConsumer, TopicPartition
    c = KafkaConsumer(bootstrap_servers=broker.split(","), group_id=None, enable_auto_commit=False,
                      api_version_auto_timeout_ms=3000,
                      value_deserializer=_safe_json)
    parts = c.partitions_for_topic(topic)
    if not parts:
        c.close()
        raise RuntimeError(f"Kafka topic '{topic}' does not exist yet. Start the producer first.")
    tps = [TopicPartition(topic, p) for p in parts]
    c.assign(tps)
    end, begin = c.end_offsets(tps), c.beginning_offsets(tps)
    for tp in tps:
        c.seek(tp, max(begin[tp], end[tp] - LIVE_BACKLOG_PER_PARTITION))
    st.session_state[key] = c
    return c


def poll_kafka(broker: str, topic: str) -> list[dict]:
    """Fetch every message that arrived since the last refresh."""
    consumer = _live_consumer(broker, topic)
    out = []
    for _ in range(5):  # drain in a few quick polls
        batch = consumer.poll(timeout_ms=1000, max_records=5000)
        if not batch:
            break
        for msgs in batch.values():
            for m in msgs:
                d = m.value
                if not isinstance(d, dict) or not all(k in d for k in ("timestamp", "value", "metric_type", "sensor_id")):
                    continue  # skip malformed messages
                d["kafka_partition"], d["kafka_offset"] = m.partition, m.offset
                out.append(d)
    return out


def update_live_buffer(new_rows: list[dict]) -> pd.DataFrame:
    buf = st.session_state.get("live_buffer")
    if new_rows:
        df = pd.DataFrame(new_rows)
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True, errors="coerce")
        df["value"] = pd.to_numeric(df["value"], errors="coerce")
        df = df.dropna(subset=["timestamp", "value"])
        buf = df if buf is None else pd.concat([buf, df], ignore_index=True)
        buf = buf.drop_duplicates(subset=["sensor_id", "metric_type", "timestamp"], keep="last")
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(hours=LIVE_BUFFER_HOURS)
        buf = buf[buf["timestamp"] >= cutoff].sort_values("timestamp").reset_index(drop=True)
        st.session_state["live_buffer"] = buf
    return buf if buf is not None else pd.DataFrame()


def page_live():
    s = settings()
    st.title("📡 Live Monitor")
    st.caption("Messages are read **directly from the Kafka topic** as the producer publishes them. "
               "If Kafka is unreachable, the page falls back to the newest records in MongoDB.")

    with st.container(border=True):
        c1, c2, c3, c4 = st.columns([1.6, 1.3, 1.2, 1.1])
        all_cities = [c["name"] for c in config.CITIES]
        cities = c1.multiselect("Cities", all_cities, key="live_cities", placeholder="All cities")
        metric = c2.selectbox("Trend metric", config.METRIC_ORDER, format_func=metric_label, key="live_metric")
        window = c3.segmented_control("Window", ["15 min", "1 h", "3 h", "6 h"], default="1 h",
                                      key="live_window") or "1 h"
        with c4:
            auto = st.toggle("Auto-refresh", value=True, key="live_auto")
            options = [5, 10, 15, 30, 60]
            default = config.LIVE_REFRESH_SECONDS if config.LIVE_REFRESH_SECONDS in options else 15
            interval = st.select_slider("Every (s)", options, value=default,
                                        key="live_interval", disabled=not auto)

    minutes = {"15 min": 15, "1 h": 60, "3 h": 180, "6 h": 360}[window]

    @st.fragment(run_every=f"{interval}s" if auto else None)
    def live_panel():
        source, err, new_count = "Kafka", None, 0
        try:
            new = poll_kafka(s["kafka_broker"], s["kafka_topic"])
            new_count = len(new)
            st.session_state["live_total"] = st.session_state.get("live_total", 0) + new_count
            data = update_live_buffer(new)
        except Exception as e:
            err = str(e)[:200]
            st.session_state.pop(f"live_consumer::{s['kafka_broker']}::{s['kafka_topic']}", None)
            try:
                data = storage.query_recent(s["mongo_uri"], s["mongo_db"], minutes=LIVE_BUFFER_HOURS * 60)
                source = "MongoDB (fallback)"
            except Exception as e2:
                st.error(f"Neither Kafka nor MongoDB is reachable.\n\nKafka: {err}\n\nMongoDB: {str(e2)[:150]}")
                return

        now_local = datetime.now(tz()).strftime("%H:%M:%S")
        last_msg = data["timestamp"].max() if not data.empty else None
        level = "ok" if source == "Kafka" else "warn"
        refresh_txt = f"auto-refresh every {interval}s" if auto else "auto-refresh off"
        st.markdown(
            pill(f"{ICON[level]} Source: {source}", level)
            + pill(f"📨 {new_count} new · {st.session_state.get('live_total', 0):,} this session", "ok")
            + pill(f"⏱️ last message {ago(last_msg)}", "ok" if last_msg is not None else "warn")
            + pill(f"🔄 {refresh_txt} · updated {now_local}", "ok"),
            unsafe_allow_html=True)
        if err:
            st.warning(f"Kafka unavailable: {err}")
        if data.empty:
            st.info("Waiting for the first messages... make sure `producer.py` is running.")
            return

        view = data[data["location"].isin(cities)] if cities else data

        # Current conditions with change since the previous API update (Open-Meteo refreshes ~15 min)
        latest_idx = view.sort_values("timestamp").groupby(["location", "metric_type"]).tail(1)
        temps = view[view["metric_type"] == "temperature"].sort_values("timestamp")
        prev = {}
        for city, g in temps.groupby("location"):
            col = "observed_at" if "observed_at" in g.columns and g["observed_at"].notna().any() else "timestamp"
            distinct = g.drop_duplicates(col, keep="last")
            if len(distinct) >= 2:
                prev[city] = distinct["value"].iloc[-2]
        city_cards(latest_idx, prev)

        # Trend chart
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(minutes=minutes)
        trend = view[(view["metric_type"] == metric) & (view["timestamp"] >= cutoff)]
        st.markdown(f"#### {metric_label(metric)} · last {window}")
        if trend.empty:
            st.caption("No readings in this window yet.")
        else:
            t = localize(trend, ["timestamp"])
            fig = px.line(t, x="timestamp", y="value", color="location", markers=True,
                          color_discrete_sequence=PALETTE)
            st.plotly_chart(plot_defaults(fig, 360, unit_of(metric)), width="stretch",
                            key=f"live_trend_{metric}")

        c1, c2 = st.columns([1, 1.4])
        with c1:
            st.markdown("#### Throughput (messages / minute)")
            rate = localize(view[view["timestamp"] >= cutoff], ["timestamp"])
            if not rate.empty:
                per_min = rate.set_index("timestamp").resample("1min").size().reset_index(name="messages")
                fig = px.bar(per_min, x="timestamp", y="messages", color_discrete_sequence=[PALETTE[0]])
                st.plotly_chart(plot_defaults(fig, 280, "messages"), width="stretch", key="live_rate")
        with c2:
            st.markdown("#### Latest messages")
            feed = localize(view.sort_values("timestamp", ascending=False).head(30), ["timestamp"])
            cols = [c for c in ["timestamp", "location", "metric_type", "value", "unit", "weather_desc",
                                "kafka_partition", "kafka_offset"] if c in feed.columns]
            st.dataframe(feed[cols], hide_index=True, width="stretch", height=280,
                         column_config={"timestamp": st.column_config.DatetimeColumn("Time", format="HH:mm:ss"),
                                        "value": st.column_config.NumberColumn("Value", format="%.2f")})

    live_panel()


# =============================================================================
# Page 3: History (all collected data from MongoDB)
# =============================================================================
def page_history():
    s = settings()
    st.title("🗂️ History")
    st.caption("Every reading that Spark has stored in MongoDB. The default view shows **all collected data**.")

    stats = load_stats_or_stop()
    if empty_state(stats):
        return
    start, end, cities, metrics = filter_bar("hist", stats, default_range="All time")
    df = c_raw(s["mongo_uri"], s["mongo_db"], start, end, cities, metrics)
    if df.empty:
        st.warning("No records match these filters.")
        return
    local = localize(df, ["timestamp"])

    k1, k2, k3, k4 = st.columns(4)
    k1.metric("Records", f"{len(df):,}")
    k2.metric("Cities", df["location"].nunique())
    k3.metric("From", local["timestamp"].min().strftime("%b %d, %H:%M"))
    k4.metric("To", local["timestamp"].max().strftime("%b %d, %H:%M"))

    tab_table, tab_charts = st.tabs(["📋 Data table", "📈 Charts"])
    with tab_table:
        layout = st.segmented_control("Layout", ["One row per reading", "One row per city & time"],
                                      default="One row per reading", key="hist_layout")
        if layout == "One row per city & time":
            table = (local.pivot_table(index=["timestamp", "location"], columns="metric_type",
                                       values="value", aggfunc="mean").reset_index())
            table = table[["timestamp", "location"] + [m for m in config.METRIC_ORDER if m in table.columns]]
            table = table.sort_values("timestamp", ascending=False)
            table.columns = [metric_label(c) if c in config.METRIC_INFO else c.title() for c in table.columns]
            colcfg = {"Timestamp": st.column_config.DatetimeColumn("Time", format="YYYY-MM-DD HH:mm")}
        else:
            table = local.sort_values("timestamp", ascending=False)[
                [c for c in ["timestamp", "location", "metric_type", "value", "unit", "weather_desc",
                             "quality", "observed_at", "sensor_id"] if c in local.columns]]
            colcfg = {"timestamp": st.column_config.DatetimeColumn("Time", format="YYYY-MM-DD HH:mm:ss"),
                      "location": "City", "metric_type": "Metric",
                      "value": st.column_config.NumberColumn("Value", format="%.2f"),
                      "weather_desc": "Conditions", "observed_at": "API model time (UTC)"}
        st.dataframe(table, hide_index=True, width="stretch", height=520, column_config=colcfg)
        st.caption(f"{len(table):,} rows · click a column header to sort · "
                   "use the 📥 Export page for Excel / PDF.")
        c1, c2, _ = st.columns([1, 1, 3])
        c1.download_button("⬇️ CSV", exports.to_csv(table), "weather_history.csv", "text/csv", width="stretch")
        c2.download_button("⬇️ Excel", exports.to_excel({"History": table}, describe_filters(start, end, cities, metrics)),
                           "weather_history.xlsx",
                           "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", width="stretch")

    with tab_charts:
        available = [m for m in config.METRIC_ORDER if m in local["metric_type"].unique()]
        m = st.selectbox("Metric", available, format_func=metric_label, key="hist_chart_metric")
        sub = local[local["metric_type"] == m]
        note = ""
        if len(sub) > 6000:  # keep the browser responsive on very long histories
            sub = (sub.set_index("timestamp").groupby("location")["value"].resample("1h").mean()
                   .reset_index().dropna())
            note = "Showing hourly averages because the selection is large."
        fig = px.line(sub, x="timestamp", y="value", color="location", color_discrete_sequence=PALETTE)
        fig.update_xaxes(rangeslider_visible=True)
        st.plotly_chart(plot_defaults(fig, 460, unit_of(m)), width="stretch")
        if note:
            st.caption(note)
        box = local[local["metric_type"] == m]
        if len(box) > 20000:
            box = box.sample(20000, random_state=1)
        fig = px.box(box, x="location", y="value", color="location",
                     color_discrete_sequence=PALETTE, points=False)
        fig.update_layout(showlegend=False, title="Distribution by city")
        st.plotly_chart(plot_defaults(fig, 340, unit_of(m)), width="stretch")


# =============================================================================
# Page 4: Analytics (data aggregation)
# =============================================================================
def page_analytics():
    s = settings()
    uri, db, tzname = s["mongo_uri"], s["mongo_db"], s["tz"]
    st.title("📊 Analytics")
    st.caption("Aggregations are computed **inside MongoDB** (time buckets, statistics, hourly profiles) "
               "and **by Spark in real time** (windowed aggregates).")

    stats = load_stats_or_stop()
    if empty_state(stats):
        return
    start, end, cities, (metric,) = filter_bar("ana", stats, default_range="Last 7d", single_metric=True)
    unit = unit_of(metric)

    tabs = st.tabs(["📈 Trends", "🏙️ City comparison", "🕒 Daily pattern", "🔗 Correlations", "⚡ Spark windows"])

    # --- Trends: time-bucketed aggregation
    with tabs[0]:
        gran = st.segmented_control("Aggregate by", list(storage.GRANULARITY), default="Hourly",
                                    key="ana_gran") or "Hourly"
        agg = c_agg(uri, db, gran, start, end, cities, (metric,), tzname)
        if agg.empty:
            st.info("No data for this selection.")
        else:
            a = localize(agg, ["bucket"])
            fig = go.Figure()
            for i, (city, g) in enumerate(a.groupby("location")):
                color = PALETTE[i % len(PALETTE)]
                fig.add_trace(go.Scatter(x=g["bucket"], y=g["max"], line=dict(width=0), showlegend=False,
                                         hoverinfo="skip", legendgroup=city))
                fig.add_trace(go.Scatter(x=g["bucket"], y=g["min"], fill="tonexty", line=dict(width=0),
                                         fillcolor=_rgba(color, .12), showlegend=False, hoverinfo="skip",
                                         legendgroup=city))
                fig.add_trace(go.Scatter(x=g["bucket"], y=g["avg"], name=city, line=dict(color=color, width=2),
                                         legendgroup=city, customdata=g[["min", "max", "count"]],
                                         hovertemplate="%{y:.2f} (min %{customdata[0]:.1f}, max %{customdata[1]:.1f},"
                                                       " n=%{customdata[2]})<extra>" + city + "</extra>"))
            fig.update_layout(title=f"{gran} average {metric_label(metric)} (shaded = min–max range)")
            st.plotly_chart(plot_defaults(fig, 440, unit), width="stretch")
            with st.expander(f"Aggregated table ({len(a):,} rows)"):
                st.dataframe(a, hide_index=True, width="stretch")
                st.download_button("⬇️ Download CSV", exports.to_csv(a), f"weather_{gran.lower().replace(' ', '_')}.csv",
                                   "text/csv")

    # --- City comparison: summary statistics
    with tabs[1]:
        summ = c_summary(uri, db, start, end, cities, ())
        if summ.empty:
            st.info("No data for this selection.")
        else:
            m = summ[summ["metric_type"] == metric].sort_values("avg", ascending=False)
        if summ.empty or m.empty:
            if not summ.empty:
                st.info("No data for this metric.")
        else:
            c1, c2 = st.columns([1.3, 1])
            with c1:
                fig = px.bar(m, x="location", y="avg", error_y="stddev", text="avg", color="location",
                             color_discrete_sequence=PALETTE)
                fig.update_traces(texttemplate="%{text:.1f}", textposition="outside", showlegend=False)
                fig.update_layout(title=f"Average {metric_label(metric)} by city (± std. dev.)")
                st.plotly_chart(plot_defaults(fig, 400, unit), width="stretch")
            with c2:
                st.markdown(f"**Extremes – {metric_label(metric)}**")
                hi, lo = m.loc[m["max"].idxmax()], m.loc[m["min"].idxmin()]
                st.metric(f"Highest · {hi['location']}", f"{hi['max']:.1f} {unit}")
                st.metric(f"Lowest · {lo['location']}", f"{lo['min']:.1f} {unit}")
                st.metric("Largest swing", f"{(m['max'] - m['min']).max():.1f} {unit}",
                          help="Max minus min within one city")
            st.markdown("**Average of every metric by city**")
            wide = summ.pivot_table(index="location", columns="metric_type", values="avg")
            wide = wide[[c for c in config.METRIC_ORDER if c in wide.columns]]
            wide.columns = [metric_label(c) for c in wide.columns]
            st.dataframe(wide.style.format("{:.1f}").background_gradient(cmap="Blues", axis=0),
                         width="stretch")

    # --- Daily pattern heatmap
    with tabs[2]:
        prof = c_hourly(uri, db, metric, start, end, cities, tzname)
        if prof.empty:
            st.info("No data for this selection.")
        else:
            grid = prof.pivot_table(index="location", columns="hour", values="avg")
            fig = px.imshow(grid, aspect="auto", color_continuous_scale="RdYlBu_r", text_auto=".1f",
                            labels=dict(x=f"Hour of day ({tzname})", y="", color=unit))
            fig.update_layout(title=f"Average {metric_label(metric)} by hour of day")
            st.plotly_chart(plot_defaults(fig, 120 + 45 * len(grid)), width="stretch")
            st.caption("Shows the typical daily cycle, e.g. when each city is hottest or most humid.")

    # --- Correlations between metrics
    with tabs[3]:
        raw = c_raw(uri, db, start, end, cities, ())
        if raw.empty:
            st.info("No data for this selection.")
        else:
            wide = raw.pivot_table(index=["timestamp", "location"], columns="metric_type", values="value")
            wide = wide[[c for c in config.METRIC_ORDER if c in wide.columns]]
            corr = wide.corr().round(2)
            corr.index = corr.columns = [config.METRIC_INFO[c]["label"] for c in corr.columns]
            c1, c2 = st.columns([1, 1])
            with c1:
                fig = px.imshow(corr, text_auto=True, color_continuous_scale="RdBu_r", zmin=-1, zmax=1)
                fig.update_layout(title="Correlation between metrics")
                st.plotly_chart(plot_defaults(fig, 420), width="stretch")
            with c2:
                if {"temperature", "humidity"} <= set(wide.columns):
                    pts = wide.reset_index().dropna(subset=["temperature", "humidity"])
                    if len(pts) > 5000:
                        pts = pts.sample(5000, random_state=1)
                    fig = px.scatter(pts, x="temperature", y="humidity", color="location", opacity=.6,
                                     color_discrete_sequence=PALETTE,
                                     labels={"temperature": "Temperature (°C)", "humidity": "Humidity (%)"})
                    fig.update_layout(title="Temperature vs humidity")
                    st.plotly_chart(plot_defaults(fig, 420), width="stretch")
            st.caption("Values near +1 / −1 mean two metrics move together / in opposite directions.")

    # --- Spark real-time windowed aggregates
    with tabs[4]:
        st.caption(f"Computed continuously by `spark_streaming.py`: tumbling **{config.SPARK_AGG_WINDOW}** "
                   f"event-time windows with a {config.SPARK_WATERMARK} watermark for late data.")
        sw = c_spark(uri, db, start, end, cities, (metric,))
        if sw.empty:
            st.info("No Spark windows yet. Is `spark_streaming.py` running?")
        else:
            w = localize(sw, ["window_start", "window_end", "updated_at"])
            fig = px.line(w, x="window_start", y="avg", color="location", markers=True,
                          color_discrete_sequence=PALETTE,
                          hover_data={"min": ":.2f", "max": ":.2f", "count": True})
            fig.update_layout(title=f"Spark window average – {metric_label(metric)}")
            st.plotly_chart(plot_defaults(fig, 400, unit), width="stretch")
            st.dataframe(w.sort_values("window_start", ascending=False)[
                ["window_start", "window_end", "location", "avg", "min", "max", "stddev", "count"]],
                hide_index=True, width="stretch", height=300)


def _rgba(hex_color: str, alpha: float) -> str:
    h = hex_color.lstrip("#")
    r, g, b = int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16)
    return f"rgba({r},{g},{b},{alpha})"


# =============================================================================
# Page 5: Export
# =============================================================================
FORMATS = {
    "Excel (.xlsx)": ("xlsx", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
    "PDF report (.pdf)": ("pdf", "application/pdf"),
    "CSV (.csv)": ("csv", "text/csv"),
    "JSON (.json)": ("json", "application/json"),
    "Chart image (.png)": ("png", "image/png"),
    "Chart image (.jpg)": ("jpg", "image/jpeg"),
}


def page_export():
    s = settings()
    uri, db, tzname = s["mongo_uri"], s["mongo_db"], s["tz"]
    st.title("📥 Export")
    st.caption("Choose what to export, filter it, pick a format, then download.")

    stats = load_stats_or_stop()
    if empty_state(stats):
        return

    st.markdown("##### 1 · What to export")
    dataset = st.radio("Dataset", ["Raw readings", "Aggregated data", "Summary statistics"], horizontal=True,
                       key="exp_dataset", label_visibility="collapsed")
    st.markdown("##### 2 · Filters")
    start, end, cities, metrics = filter_bar("exp", stats, default_range="All time")
    gran = None
    if dataset == "Aggregated data":
        gran = st.segmented_control("Aggregate by", list(storage.GRANULARITY), default="Hourly",
                                    key="exp_gran") or "Hourly"

    st.markdown("##### 3 · Format")
    fmt_label = st.radio("Format", list(FORMATS), horizontal=True, key="exp_fmt", label_visibility="collapsed")
    ext, mime = FORMATS[fmt_label]
    chart_metric = None
    if ext in ("png", "jpg"):
        chart_metric = st.selectbox("Chart metric", [m for m in stats["metrics"] if not metrics or m in metrics],
                                    format_func=metric_label, key="exp_chart_metric")

    # Load the selected dataset
    if dataset == "Raw readings":
        df = localize(c_raw(uri, db, start, end, cities, metrics), ["timestamp"])
    elif dataset == "Aggregated data":
        df = localize(c_agg(uri, db, gran, start, end, cities, metrics, tzname), ["bucket"])
    else:
        df = localize(c_summary(uri, db, start, end, cities, metrics), ["first", "last"])
    if df.empty:
        st.warning("No records match these filters.")
        return

    with st.expander(f"Preview · {len(df):,} rows", expanded=False):
        st.dataframe(df.head(200), hide_index=True, width="stretch")

    info = {"Dataset": dataset + (f" ({gran})" if gran else ""), **describe_filters(start, end, cities, metrics),
            "Rows": f"{len(df):,}"}
    stamp = datetime.now().strftime("%Y%m%d_%H%M")
    fname = f"weather_{dataset.split()[0].lower()}_{stamp}.{ext}"

    st.markdown("##### 4 · Download")
    if st.button(f"⚙️ Prepare {fmt_label}", type="primary"):
        with st.spinner("Building file..."):
            try:
                st.session_state["export_file"] = (build_export(ext, df, dataset, info, chart_metric, start, end,
                                                                cities, metrics), fname, mime)
            except Exception as e:
                st.session_state.pop("export_file", None)
                st.error(f"Export failed: {e}")
    if st.session_state.get("export_file"):
        data, name, m = st.session_state["export_file"]
        st.download_button(f"⬇️ Download {name}", data, name, m, type="primary")
        if name.endswith((".png", ".jpg")):
            st.image(data, caption="Preview")
        st.caption(f"{len(data) / 1024:,.0f} KB")


def build_export(ext, df, dataset, info, chart_metric, start, end, cities, metrics) -> bytes:
    s = settings()
    uri, db, tzname = s["mongo_uri"], s["mongo_db"], s["tz"]
    if ext == "csv":
        return exports.to_csv(df)
    if ext == "json":
        return exports.to_json(df)
    if ext == "xlsx":
        sheets = {dataset: df}
        summ = localize(c_summary(uri, db, start, end, cities, metrics), ["first", "last"])
        if dataset != "Summary statistics" and not summ.empty:
            sheets["Summary"] = summ
        return exports.to_excel(sheets, info)

    hourly = localize(c_agg(uri, db, "Hourly", start, end, cities, metrics, tzname), ["bucket"])
    if ext in ("png", "jpg"):
        h = hourly[hourly["metric_type"] == chart_metric]
        if h.empty:
            raise ValueError("No data for that metric.")
        return exports.chart_image(h, "bucket", "avg", "location", f"Hourly average {metric_label(chart_metric)}",
                                   unit_of(chart_metric), fmt=ext)

    # PDF report
    summ = localize(c_summary(uri, db, start, end, cities, metrics), ["first", "last"])
    summary_tbl = summ[["location", "metric_type", "unit", "avg", "min", "max", "stddev", "count"]].rename(
        columns={"location": "City", "metric_type": "Metric", "unit": "Unit", "avg": "Average", "min": "Min",
                 "max": "Max", "stddev": "Std dev", "count": "Readings"}) if not summ.empty else summ
    charts = []
    for m in [m for m in ["temperature", "humidity", "pressure", "wind_speed"] if not metrics or m in metrics]:
        h = hourly[hourly["metric_type"] == m]
        if not h.empty:
            charts.append((f"Hourly average {metric_label(m)}",
                           exports.chart_image(h, "bucket", "avg", "location", metric_label(m), unit_of(m))))
    if not summ.empty and "temperature" in set(summ["metric_type"]):
        t = summ[summ["metric_type"] == "temperature"]
        charts.append(("Average temperature by city", exports.bar_image(t, "location", "avg", "Average temperature",
                                                                        "°C")))
    sample = df.drop(columns=[c for c in ("latitude", "longitude", "sensor_id", "observed_at") if c in df.columns])
    return exports.pdf_report("Weather Streaming Report", "Open-Meteo API · Kafka · Spark · MongoDB", info,
                              summary_tbl, charts, sample)


# =============================================================================
# Page 6: About (discussion)
# =============================================================================
def page_about():
    st.title("ℹ️ About this project")
    st.graphviz_chart("""
digraph {
  rankdir=LR; bgcolor="transparent";
  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=11, color="#1E3A8A", fillcolor="#DBEAFE"];
  edge [color="#64748B", fontname="Helvetica", fontsize=9];
  api [label="Open-Meteo API\\n(current weather,\\n5 cities)", fillcolor="#FEF3C7"];
  prod [label="producer.py\\nKafka Producer"];
  kafka [label="Apache Kafka\\ntopic: weather-stream", fillcolor="#E0E7FF"];
  spark [label="Spark Structured\\nStreaming\\nclean · validate ·\\nwindowed aggregation", fillcolor="#FFEDD5"];
  mongo [label="MongoDB\\nweather_raw\\nweather_agg_windowed", fillcolor="#DCFCE7"];
  dash [label="Streamlit Dashboard\\nOverview · Live · History\\nAnalytics · Export", fillcolor="#F3E8FF"];
  api -> prod [label="HTTPS / JSON\\nevery 60 s"];
  prod -> kafka [label="JSON messages\\nkey = sensor_id"];
  kafka -> spark [label="readStream"];
  spark -> mongo [label="foreachBatch\\nupserts"];
  kafka -> dash [label="live consumer\\n(Live Monitor)", style=dashed];
  mongo -> dash [label="queries &\\naggregations"];
}""")

    st.markdown("""
### 1. The API used: Open-Meteo
[Open-Meteo](https://open-meteo.com) is a free, open-source weather API. It needs **no account or API key**
and returns JSON. It combines forecasts from national weather services (e.g. NOAA, DWD, JMA) into
high-resolution models that update every 15 minutes.

* **Endpoint:** `https://api.open-meteo.com/v1/forecast` with `current=` variables
* **Variables collected:** temperature, apparent ("feels like") temperature, relative humidity,
  precipitation, sea-level pressure, wind speed, cloud cover and the WMO weather code
* **Locations:** Manila, Quezon City, Baguio, Cebu City and Davao City, all fetched in **one request**
  (comma-separated coordinates). Cities can be changed with the `CITIES` setting.
* **Rate limits:** the free tier allows about 10,000 calls/day, so the producer polls every 60 s
  (5 cities × 1,440 polls ≈ 7,200 calls/day). It retries failed requests with exponential back-off.

Each API result is flattened into one message per city and metric, using the required schema
(`timestamp`, `value`, `metric_type`, `sensor_id`) plus location, unit, coordinates and conditions.

### 2. The pipeline: MongoDB · Kafka · Spark
| Stage | Technology | What it does |
|---|---|---|
| Ingestion | `producer.py` + **Kafka** | Publishes readings to the `weather-stream` topic. Using `sensor_id` as the message key keeps each city's readings in order within one partition. Kafka buffers the data, so nothing is lost if a downstream component restarts. |
| Processing | **Spark Structured Streaming** (`spark_streaming.py`) | Parses JSON against a fixed schema, drops malformed records, converts timestamps, and flags values outside physically plausible ranges. Computes **windowed aggregates** (avg / min / max / std-dev / count per city and metric) over tumbling event-time windows, with a watermark for late data. Checkpointing gives exactly-once progress tracking. |
| Storage | **MongoDB** | `weather_raw` holds every cleaned reading. `weather_agg_windowed` holds Spark's windows. `pipeline_status` holds Spark heartbeats. Writes are idempotent upserts, so replaying Kafka never duplicates data. Indexes on time, city and metric keep queries fast. |
| Serving | **Streamlit** | *Live Monitor* reads Kafka directly, which is the real-time path. *History*, *Analytics* and *Export* query MongoDB, which is the historical path. Heavy aggregations (`$dateTrunc`, `$group`, `$hour`) run inside MongoDB. |

This is a **dual-pipeline (lambda-style)** design. The speed layer (Kafka → Live Monitor) shows data
seconds after it is published. The batch/serving layer (Kafka → Spark → MongoDB) keeps a durable,
queryable history and pre-computed aggregates.

### 3. Why the dashboard matters
* **Situational awareness:** live conditions for several cities on one screen, refreshed every 15 seconds.
  This is useful for disaster-risk teams, event planners, farmers, logistics and commuters, especially
  in a typhoon-prone country like the Philippines.
* **Historical insight:** every reading is kept, so users can see trends, compare cities, find the hottest
  hours of the day, and check how metrics relate (for example, humidity usually falls as temperature rises).
* **Data quality and trust:** Spark validates every record and the sidebar shows the health of each
  pipeline component, so users know whether the data is current.
* **Decision support and reporting:** filtered data can be exported to Excel, CSV, JSON, a formatted PDF
  report or chart images, ready for presentations or further analysis.
* **Scalability:** Kafka partitions, Spark executors and MongoDB can each be scaled out on their own.
  The same design would handle thousands of real IoT sensors instead of five cities.
""")


# =============================================================================
# Navigation
# =============================================================================
PAGES = {
    "pages/overview": st.Page(page_overview, title="Overview", icon="🏠", url_path="overview", default=True),
    "pages/live": st.Page(page_live, title="Live Monitor", icon="📡", url_path="live"),
    "pages/history": st.Page(page_history, title="History", icon="🗂️", url_path="history"),
    "pages/analytics": st.Page(page_analytics, title="Analytics", icon="📊", url_path="analytics"),
    "pages/export": st.Page(page_export, title="Export", icon="📥", url_path="export"),
    "pages/about": st.Page(page_about, title="About the project", icon="ℹ️", url_path="about"),
}


def main():
    settings()
    nav = st.navigation({
        "Monitor": [PAGES["pages/overview"], PAGES["pages/live"]],
        "Explore": [PAGES["pages/history"], PAGES["pages/analytics"], PAGES["pages/export"]],
        "Project": [PAGES["pages/about"]],
    })
    sidebar()
    nav.run()


if __name__ == "__main__":
    main()
