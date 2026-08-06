#!/usr/bin/env python3
"""
Figure 4(d): Combined — (i) aggregate throughput at source vs time; (ii) Kafka lag vs time.

Same pasted data as ``plot_fig4d_i_throughput_state_scalability.py`` and
``plot_fig4d_ii_kafka_lag_scalability.py``; two side-by-side subplots (independent x-axes).
"""

from __future__ import annotations

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np
from util import export_metrics, result_db_path

SLIDING_WINDOW_SIZE = 5
SKIP_FIRST_N_RECORDS = 35
OFFSET = 30
INCREASE_RATE_TIME_SECONDS = 120
SCALEUP_TIME_SECONDS = 183
XMIN = 50
XMAX = 350
SAVE_DPI = 600

NS_PER_SEC = 1_000_000_000.0

LAZY_COLOR = "#778aaf"
LINE_WIDTH = 2.4

OUTPUT_PNG = "./Figure13.pdf"

# ---------------------------------------------------------------------------
# Paste data here
# ---------------------------------------------------------------------------
LABELS = ["Multiple Reconfigigurations"]

# fmt: off
KOALA_PATH = result_db_path("twitch_concurrent_multi_reconfig")
TIMES_SERIES, VALUES_SERIES_TPUT = export_metrics(
    DBS=[KOALA_PATH],
    TARGET_OPERATOR="twitchFileSource",
    FIGURE="Aggregate throughput at source",
)
_, VALUES_SERIES_KAFKA = export_metrics(DBS=[KOALA_PATH],
    TARGET_OPERATOR="twitchFileSource",
    FIGURE="Average kafka lag",
)
# ---------------------------------------------------------------------------

def sliding_avg(
    timestamps: list[int], values: list[float], window_size: int
) -> list[float]:
    """Mean of values in ``[t - window_size + 1, t]`` at each timestamp; NaNs ignored."""
    result: list[float] = []
    n = len(timestamps)
    for i in range(n):
        t = timestamps[i]
        lower_bound = t - window_size + 1
        window_vals = [
            v
            for j, v in enumerate(values)
            if lower_bound <= timestamps[j] <= t and not np.isnan(v)
        ]
        if window_vals:
            result.append(float(np.mean(window_vals)))
        else:
            result.append(float("nan"))
    return result


def main() -> None:
    n = len(LABELS)

    fig, (ax_tput, ax_lag) = plt.subplots(1, 2, figsize=(8, 2.5))

    skip = max(0, int(SKIP_FIRST_N_RECORDS))

    for ax, tag in ((ax_tput, "(a) Tput over time (s)"), (ax_lag, "(b) Kafka lag over time (s)")):
        ax.axvline(x=INCREASE_RATE_TIME_SECONDS, color="lightgrey", linestyle="-.", linewidth=LINE_WIDTH)
        ax.axvline(x=SCALEUP_TIME_SECONDS, color="lightgrey", linestyle="--", linewidth=LINE_WIDTH)
        ax.set_xlim(left=XMIN, right=XMAX)
        ax.set_xticks([100, 200, 300])
        ax.set_xlabel(tag, fontsize=18, labelpad=9)
        ax.tick_params(axis="both", labelsize=16)

    for j in range(n):
        times_full = list(TIMES_SERIES[j])
        tput = sliding_avg(times_full, [float(v) for v in VALUES_SERIES_TPUT[j]], SLIDING_WINDOW_SIZE)
        times = np.array(times_full[skip:]) - OFFSET
        tput = tput[skip:]
        ax_tput.plot(times, tput, color=LAZY_COLOR, linewidth=LINE_WIDTH, label=LABELS[j])

        times_k = np.array(TIMES_SERIES[j][skip:]) - OFFSET
        kafka_s = [float(v) / NS_PER_SEC for v in VALUES_SERIES_KAFKA[j][skip:]]
        ax_lag.fill_between(times_k, 0, kafka_s, color="lightgrey", alpha=0.6)
        ax_lag.plot(times_k, kafka_s, color=LAZY_COLOR, linewidth=LINE_WIDTH, label=LABELS[j])

    ax_tput.set_ylabel("Tput (r/s)", fontsize=16, labelpad=4)
    ax_tput.set_ylim([-(7.5e5*0.013),7.5e5])
    ax_tput.set_yticks([0, 3e5, 6e5])
    ax_lag.set_ylabel("Kafka lag (s)", fontsize=16, labelpad=4)
    ax_lag.set_ylim([-1, 80])
    ax_lag.set_yticks([0, 30, 60])   
    ax_tput.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))

    load_increase_handle = Line2D([0], [0], color="lightgrey", linestyle="-.", linewidth=LINE_WIDTH)
    rebalance_handle = Line2D([0], [0], color="lightgrey", linestyle="--", linewidth=LINE_WIDTH)
    lag_area_handle = Patch(facecolor="lightgrey", alpha=0.6)
    fig.legend(
        [load_increase_handle, rebalance_handle, lag_area_handle],
        ["Load Increase", "Scale-out", "Kafka Backlog"],
        loc="upper center", bbox_to_anchor=(0.5, 1.05),
        handlelength=2.5, ncol=3, frameon=False, fontsize=16,
    )

    plt.subplots_adjust(left=0.09, right=0.95, bottom=0.28, top=0.81, wspace=0.28)
    plt.savefig(OUTPUT_PNG, dpi=SAVE_DPI)
    # plt.show()


if __name__ == "__main__":
    main()