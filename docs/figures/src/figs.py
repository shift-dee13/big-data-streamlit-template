import matplotlib; matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle
import numpy as np
OUT = "/home/claude/big-data-streamlit-template/docs/figures/"
BLUE, ORANGE, GREEN, RED, GREY = "#2563EB", "#F97316", "#10B981", "#EF4444", "#64748B"

# --- Figure: Spark tumbling windows + watermark (illustrative)
fig, ax = plt.subplots(figsize=(12, 4.8))
cols = [BLUE, GREEN, ORANGE, "#A855F7"]
for i, start in enumerate(range(0, 20, 5)):
    ax.add_patch(Rectangle((start, 0.15), 5, 0.7, facecolor=cols[i], alpha=.12, edgecolor=GREY, lw=1))
    ax.text(start + 2.5, 0.88, f"Window {i+1}  ·  10:{start:02d}–10:{start+5:02d}", ha="center", va="bottom", fontsize=9,
            fontweight="bold")
    ax.text(start + 2.5, 0.2, "avg · min · max · std · count", ha="center", fontsize=7.5, color=GREY)
t = np.arange(0.5, 20, 1.0)
ax.scatter(t, np.full_like(t, 0.62), s=55, color=BLUE, zorder=3, label="On-time reading (1 per minute)")
ax.scatter([7.4], [0.4], s=90, color=ORANGE, marker="D", zorder=3, label="Late reading, accepted")
ax.annotate("event time 10:07, arrives 10:20\n→ still updates Window 2", xy=(7.4, 0.4), xytext=(10.4, 0.34),
            fontsize=8.5, arrowprops=dict(arrowstyle="->", color=ORANGE))
ax.scatter([2.4], [0.4], s=90, color=RED, marker="X", zorder=3, label="Too-late reading, dropped")
ax.annotate("event time 10:02, older than\nthe watermark → ignored", xy=(2.4, 0.4), xytext=(-0.3, -0.05),
            fontsize=8.5, color=RED, arrowprops=dict(arrowstyle="->", color=RED))
ax.axvline(20, color=RED, ls="--"); ax.text(20.15, 0.95, "latest event\ntime 10:20", color=RED, fontsize=9, va="top")
ax.axvline(5, color=RED, ls=":", alpha=.8)
ax.text(5.1, 1.08, "watermark = 10:20 − 15 min = 10:05", color=RED, fontsize=9)
ax.set_xlim(-0.6, 23); ax.set_ylim(-0.12, 1.2); ax.set_yticks([])
ax.set_xticks(range(0, 21, 5)); ax.set_xticklabels([f"10:{m:02d}" for m in range(0, 21, 5)])
ax.set_xlabel("Event time (timestamp of the reading)")
ax.set_title("Spark Structured Streaming: 5-minute tumbling windows with a 15-minute watermark",
             loc="left", fontweight="bold", pad=12)
for sp in ("top", "right", "left"): ax.spines[sp].set_visible(False)
ax.legend(loc="upper center", frameon=False, fontsize=9, bbox_to_anchor=(0.5, -0.17), ncol=3)
fig.tight_layout(); fig.savefig(OUT + "spark_windows.png", dpi=160, facecolor="white"); plt.close(fig)

# --- Figure: projected data volume (computed: cities x 7 metrics x 1,440 polls/day)
metrics, polls_day = 7, 1440
scales = [("This project\n(5 cities)", 5), ("Regional\n(100 stations)", 100), ("National\n(1,000 stations)", 1000),
          ("IoT network\n(10,000 sensors)", 10000)]
periods = [("1 day", 1), ("1 month", 30), ("1 year", 365)]
fig, ax = plt.subplots(figsize=(12, 4.8))
x = np.arange(len(scales)); w = 0.26
for j, (pl, days) in enumerate(periods):
    vals = [n * metrics * polls_day * days for _, n in scales]
    bars = ax.bar(x + (j - 1) * w, vals, w, label=pl, color=[BLUE, ORANGE, GREEN][j])
    for b, v in zip(bars, vals):
        lab = f"{v/1e9:.1f}B" if v >= 1e9 else f"{v/1e6:.1f}M" if v >= 1e6 else f"{v/1e3:.0f}K"
        ax.text(b.get_x() + b.get_width()/2, v * 1.15, lab, ha="center", fontsize=8)
ax.set_yscale("log"); ax.set_ylim(1e4, 1e11)
ax.set_xticks(x); ax.set_xticklabels([s for s, _ in scales])
ax.set_ylabel("Readings stored (log scale)")
ax.set_title("Projected data volume: readings = sensors × 7 metrics × 1,440 polls/day × days",
             loc="left", fontweight="bold")
ax.legend(frameon=False, title="Collection period")
ax.grid(axis="y", alpha=.25); ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); fig.savefig(OUT + "data_volume.png", dpi=160, facecolor="white"); plt.close(fig)

# --- Figure: MongoDB vs HDFS fit for this use case (qualitative 1-5 scores, labelled as such)
crit = ["JSON / document\nstorage", "Low-latency\ninteractive queries", "Upserts /\ndeduplication",
        "Built-in time\naggregation", "Setup on a\nlaptop", "Huge write-once\nbatch archives"]
mongo = [5, 5, 5, 4, 5, 3]; hdfs = [2, 2, 1, 2, 2, 5]
fig, ax = plt.subplots(figsize=(12, 4.4))
x = np.arange(len(crit)); w = 0.38
ax.bar(x - w/2, mongo, w, label="MongoDB", color=GREEN); ax.bar(x + w/2, hdfs, w, label="Hadoop HDFS", color=GREY)
ax.set_xticks(x); ax.set_xticklabels(crit, fontsize=9); ax.set_ylim(0, 6.6); ax.set_yticks(range(0, 6))
ax.set_ylabel("Suitability (1 = poor, 5 = excellent)")
ax.set_title("Storage choice for a live dashboard: MongoDB vs HDFS (qualitative assessment)", loc="left",
             fontweight="bold")
ax.legend(frameon=False, ncol=2, loc="upper left"); ax.grid(axis="y", alpha=.25); ax.spines[["top", "right"]].set_visible(False)
fig.tight_layout(); fig.savefig(OUT + "mongo_vs_hdfs.png", dpi=160, facecolor="white"); plt.close(fig)
