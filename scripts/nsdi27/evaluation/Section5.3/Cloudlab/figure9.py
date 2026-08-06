#!/usr/bin/env python3
"""
Figure 9: Scalability at high parallelism (1x2 panels).

Panel (a): throughput over time (source aggregated).
Panel (b): Kafka lag over time (coordinator).

Reads the "parallelism_16_to_32" run from scripts/results/ (via util.RESULTS_DIR)
and writes Figure9.pdf next to this script.
"""

from __future__ import annotations

import os

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

from util import (
    RESULTS_DIR,
    find_result_dir,
    kafka_lag_aggregated,
    sliding_avg,
    throughput_source_aggregated,
)


SLIDING_WINDOW_SIZE = 5
SKIP_FIRST_N_RECORDS = 35
INCREASE_RATE_TIME_SECONDS = 120
SCALEUP_TIME_SECONDS = 183
XMIN = 50
XMAX = 350
SAVE_DPI = 600

NS_PER_SEC = 1_000_000_000.0

LAZY_COLOR = "#778aaf"
LINE_WIDTH = 2.4

OUTPUT_PDF = "./Figure9.pdf"


def main() -> None:
    result_dir = find_result_dir(RESULTS_DIR, "parallelism_16_to_32")
    if not result_dir:
        raise SystemExit(
            f"No result directory matching 'parallelism_16_to_32' in {RESULTS_DIR}"
        )

    db_path = os.path.join(result_dir, "metricDB", "metricCollector.db")
    if not os.path.exists(db_path):
        raise SystemExit(f"Database not found at {db_path}")

    skip = SKIP_FIRST_N_RECORDS
    fig, (ax_tput, ax_lag) = plt.subplots(1, 2, figsize=(8, 2.5))

    # Throughput
    times, vals = throughput_source_aggregated(db_path)
    if times:
        tput = sliding_avg(times, vals, SLIDING_WINDOW_SIZE)
        times_skip = times[skip:]
        tput_skip = tput[skip:]
        ax_tput.plot(times_skip, tput_skip, color=LAZY_COLOR, linewidth=LINE_WIDTH)
        ax_tput.axvline(
            x=INCREASE_RATE_TIME_SECONDS, color="lightgrey", linestyle="-.", linewidth=LINE_WIDTH
        )
        ax_tput.axvline(
            x=SCALEUP_TIME_SECONDS, color="lightgrey", linestyle="--", linewidth=LINE_WIDTH
        )
        ax_tput.set_ylabel("Tput (r/s)", fontsize=16, labelpad=4)
        ax_tput.set_ylim([0, 1.7e6])
        ax_tput.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
        ax_tput.tick_params(axis="both", labelsize=16)
        ax_tput.set_xlim(left=XMIN, right=XMAX)
        ax_tput.set_xticks([100, 200, 300])
        ax_tput.set_xlabel("(a) Tput over time (s)", fontsize=18, labelpad=9)

    # Kafka lag
    times_k, lag_vals = kafka_lag_aggregated(db_path)
    if times_k:
        lag_s = [float(v) / NS_PER_SEC for v in lag_vals]
        times_k_skip = times_k[skip:]
        lag_s_skip = lag_s[skip:]
        ax_lag.fill_between(times_k_skip, 0, lag_s_skip, color="lightgrey", alpha=0.6)
        ax_lag.plot(times_k_skip, lag_s_skip, color=LAZY_COLOR, linewidth=LINE_WIDTH)
        ax_lag.axvline(
            x=INCREASE_RATE_TIME_SECONDS, color="lightgrey", linestyle="-.", linewidth=LINE_WIDTH
        )
        ax_lag.axvline(
            x=SCALEUP_TIME_SECONDS, color="lightgrey", linestyle="--", linewidth=LINE_WIDTH
        )
        ax_lag.set_ylabel("Kafka lag (s)", fontsize=16, labelpad=4)
        ax_lag.set_ylim([-1, 80])
        ax_lag.set_yticks([0, 30, 60])
        ax_lag.tick_params(axis="both", labelsize=16)
        ax_lag.set_xlim(left=XMIN, right=XMAX)
        ax_lag.set_xticks([100, 200, 300])
        ax_lag.set_xlabel("(b) Kafka lag over time (s)", fontsize=18, labelpad=9)

    # Legend
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
    plt.savefig(OUTPUT_PDF, dpi=SAVE_DPI, bbox_inches="tight")
    print(f"✓ Figure 9 saved to {OUTPUT_PDF}")
    plt.close()


if __name__ == "__main__":
    main()
