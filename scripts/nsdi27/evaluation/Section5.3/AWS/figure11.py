import matplotlib.pyplot as plt
from matplotlib.legend_handler import HandlerTuple
from matplotlib.lines import Line2D
import numpy as np
from util import export_metrics, result_db_path

# fmt: off
KOALA_PATH = result_db_path("taxi_skew_rebalance")

# Per-task throughput of the target operator: one line per task, showing the
# single task that owns most of the hot keys before the rebalance.
TIMES_PER_TASK, VALUES_PER_TASK = export_metrics(
    DBS=[KOALA_PATH],
    TARGET_OPERATOR="movingMedianTripTime",
    FIGURE="Per-task throughput",
)
TIMES_KAFKA, VALUES_KAFKA = export_metrics(
    DBS=[KOALA_PATH],
    TARGET_OPERATOR="source",
    FIGURE="Average kafka lag",
)
# fmt: on

NS_PER_SEC = 1e9

OFFSET = 60
INCREASE_RATE_TIME = 120
SCALEUP_TIME = 183
SLIDING_WINDOW = 10
XMIN, XMAX = 50, 350

LAZY_COLOR = "#778aaf"
LINE_WIDTH = 2.4

# PER_TASK_COLORS = ["#b4b8d4", "#9498bd", "#7478a3", "#656a95"]
# PER_TASK_COLORS = ["#c69968", "#d4a87a", "#e1ba94", "#e8c4a2"]
PER_TASK_COLORS = ["#778aaf", "#778aaf", "#778aaf", "#778aaf"]
OUTPUT_PDF = "./Figure11.pdf"


def sliding_avg(ts, vs, window=SLIDING_WINDOW):
    ts = np.asarray(ts)
    vs = np.asarray(vs, dtype=float)
    out = np.empty(len(ts))
    for i, t in enumerate(ts):
        mask = (ts >= t - window + 1) & (ts <= t)
        w = vs[mask]
        w = w[~np.isnan(w)]
        out[i] = w.sum() / window if w.size else np.nan
    return out


def draw_phase_markers(ax):
    ax.axvline(INCREASE_RATE_TIME, color="lightgrey", linestyle="-.", linewidth=LINE_WIDTH)
    ax.axvline(SCALEUP_TIME, color="lightgrey", linestyle="--", linewidth=LINE_WIDTH)


def style_axis(ax, ylabel):
    ax.set_xlim(XMIN, XMAX)
    ax.set_xticks([100, 200, 300])
    if ylabel == "Kafka lag (s)":
        ax.set_xlabel("(b) Backlog over time (s)", fontsize=18, labelpad=9)
    else:
        ax.set_xlabel("(a) Per-task Tput over time (s)  ", fontsize=18, labelpad=9)
    ax.set_ylabel(ylabel, fontsize=16, labelpad=4)
    ax.tick_params(axis="both", labelsize=16)
    # ax.grid(True, axis="y", linestyle="-", alpha=0.7)
    if ylabel != "Kafka lag (s)":
        ax.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))


def apply_ylim(ax, vals):
    finite = vals[np.isfinite(vals)]
    if finite.size == 0:
        return
    lo, hi = float(finite.min()), float(finite.max())
    pad = hi * 0.2
    ax.set_ylim(bottom=max(0.0, lo - pad), top=hi + pad)


def plot_kafka_lag(ax, times, values_ns):
    # export_metrics already averaged across the source tasks and returns
    # nanoseconds, so this only converts to seconds and smooths
    ts = np.array(times)
    vals = sliding_avg(ts, [float(v) / NS_PER_SEC for v in values_ns])
    x = ts - OFFSET
    fill_vals = np.where(np.isnan(vals), 0.0, vals)
    ax.fill_between(x, 0, fill_vals, color="lightgrey", alpha=0.6, linewidth=0)
    ax.plot(x, vals, color=LAZY_COLOR, linewidth=LINE_WIDTH)
    finite = vals[np.isfinite(vals)]
    if finite.size:
        hi = float(finite.max())
        pad = hi * 0.2
        ax.set_ylim(bottom=0.0, top=hi + pad)


def plot_per_task(ax, times_by_task, values_by_task):
    all_vals = []
    # Sorted so the colour assigned to each task is stable between runs
    for i, wid in enumerate(sorted(times_by_task)):
        ts = np.array(times_by_task[wid])
        vs = sliding_avg(ts, values_by_task[wid])
        color = PER_TASK_COLORS[i % len(PER_TASK_COLORS)]
        ax.plot(ts - OFFSET, vs, label=f"worker {wid}", color=color, linewidth=1.8)
        # ax.plot(ts - OFFSET, vs, label=f"worker {wid}", color="grey", linewidth=2)
        all_vals.append(vs)
    if all_vals:
        apply_ylim(ax, np.concatenate(all_vals))


def plot_metrics():
    fig, (ax_a, ax_b) = plt.subplots(1, 2, figsize=(8, 2.5))

    draw_phase_markers(ax_a)
    plot_per_task(ax_a, TIMES_PER_TASK[0], VALUES_PER_TASK[0])
    style_axis(ax_a, "Task Tput (r/s)")

    draw_phase_markers(ax_b)
    plot_kafka_lag(ax_b, TIMES_KAFKA[0], VALUES_KAFKA[0])
    style_axis(ax_b, "Kafka lag (s)")

    # total_handle = Line2D([0], [0], color=LAZY_COLOR, linewidth=3)
    # per_task_handle = tuple(
    #     Line2D([0], [0], color=c, linewidth=3) for c in PER_TASK_COLORS
    # )

    load_increase_handle = Line2D([0], [0], color="lightgrey", linestyle="-.", linewidth=LINE_WIDTH)
    rebalance_handle = Line2D([0], [0], color="lightgrey", linestyle="--", linewidth=LINE_WIDTH)

    leg = fig.legend(
        [load_increase_handle, rebalance_handle],
        ["Load Increase", "Rebalance"],
        handler_map={tuple: HandlerTuple(ndivide=None, pad=0)},
        loc="upper center", bbox_to_anchor=(0.5, 1.05),
        handlelength=2.5, ncol=2, frameon=False, fancybox=True, fontsize=16,
    )
    # leg.get_frame().set_edgecolor("lightgrey")
    # leg.get_frame().set_facecolor("white")
    # leg.get_frame().set_linewidth(1)

    plt.subplots_adjust(left=0.09, right=0.95, bottom=0.28, top=0.81, wspace=0.28)
    plt.savefig(OUTPUT_PDF, dpi=600)
    # plt.show()


if __name__ == "__main__":
    plot_metrics()