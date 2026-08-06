#!/usr/bin/env python3
"""
Figure 14: State size sensitivity (2x4 grid).

Row 1: throughput over time for 4 state sizes (10/20/40/80 GB).
Row 2: state migrated over time for the same 4 state sizes.

Reads the lazy_*Keys runs from scripts/results/ (via util.RESULTS_DIR) and
writes Figure14.pdf next to this script.
"""

from __future__ import annotations

import os

import matplotlib.pyplot as plt

from util import (
    RESULTS_DIR,
    find_result_dir,
    sliding_avg,
    state_transfer_target_aggregated,
    throughput_source_aggregated,
)


SLIDING_WINDOW_SIZE = 5
SKIP_FIRST_N_RECORDS = 15
SCALEUP_TIME_SECONDS = 183
XMAX_STATE_SIZE = 590.0
SAVE_DPI = 600

BYTES_PER_GB = 1024 * 1024 * 1024

LAZY_COLOR = "#778aaf"
LINE_WIDTH = 2.4

YLABEL_FONTSIZE = 22
XLABEL_FONTSIZE = 18
TICK_FONTSIZE = 18
ROW_CAPTION_FONTSIZE = 20

CONFIGS = [
    ("lazy_5MKeys", "10GB"),
    ("lazy_10MKeys", "20GB"),
    ("lazy_20MKeys", "40GB"),
    ("lazy_40MKeys", "80GB"),
]

OUTPUT_PDF = "./Figure14.pdf"


def main() -> None:
    fig, axes = plt.subplots(2, 4, figsize=(18, 10))
    skip = SKIP_FIRST_N_RECORDS

    for col_idx, (keyword, size_label) in enumerate(CONFIGS):
        result_dir = find_result_dir(RESULTS_DIR, keyword)
        if not result_dir:
            print(f"  Skipping {size_label}: directory not found")
            continue

        db_path = os.path.join(result_dir, "metricDB", "metricCollector.db")
        if not os.path.exists(db_path):
            print(f"  Skipping {size_label}: database not found at {db_path}")
            continue

        try:
            ax_tput = axes[0, col_idx]
            ax_state = axes[1, col_idx]

            # Throughput (row 1)
            times, vals = throughput_source_aggregated(db_path)
            if times:
                tput = sliding_avg(times, vals, SLIDING_WINDOW_SIZE)
                times_skip = times[skip:]
                tput_skip = tput[skip:]
                ax_tput.plot(times_skip, tput_skip, color=LAZY_COLOR, linewidth=LINE_WIDTH)
                ax_tput.axvline(
                    x=SCALEUP_TIME_SECONDS, color="lightgrey", linestyle="--", linewidth=LINE_WIDTH
                )
                ax_tput.set_ylabel("Tput (r/s)", fontsize=YLABEL_FONTSIZE)
                ax_tput.ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
                ax_tput.tick_params(labelsize=TICK_FONTSIZE)
                ax_tput.set_xlim(left=0, right=XMAX_STATE_SIZE)
                ax_tput.set_title(size_label, fontsize=XLABEL_FONTSIZE)

            # State migrated (row 2)
            times, vals = state_transfer_target_aggregated(db_path)
            if times:
                vals_gb = [v / BYTES_PER_GB for v in vals]
                times_skip = times[skip:]
                vals_skip = vals_gb[skip:]
                ax_state.plot(times_skip, vals_skip, color=LAZY_COLOR, linewidth=LINE_WIDTH)
                ax_state.axvline(
                    x=SCALEUP_TIME_SECONDS, color="lightgrey", linestyle="--", linewidth=LINE_WIDTH
                )
                ax_state.set_ylabel("State migrated (GB)", fontsize=YLABEL_FONTSIZE)
                ax_state.set_xlabel("Time (s)", fontsize=XLABEL_FONTSIZE)
                ax_state.tick_params(labelsize=TICK_FONTSIZE)
                ax_state.set_xlim(left=0, right=XMAX_STATE_SIZE)
        except Exception as e:
            print(f"  Error processing {size_label}: {e}")

    # Row captions
    fig.text(
        0.5, 0.50,
        "(a) Tput over time (s) under varying state size.",
        ha="center", fontsize=ROW_CAPTION_FONTSIZE,
    )
    fig.text(
        0.5, 0.001,
        "(b) State migrated over time (s) under varying state size.",
        ha="center", fontsize=ROW_CAPTION_FONTSIZE,
    )

    plt.subplots_adjust(left=0.06, right=0.98, top=0.94, bottom=0.08, hspace=0.35, wspace=0.3)
    plt.savefig(OUTPUT_PDF, dpi=SAVE_DPI, bbox_inches="tight")
    print(f"✓ Figure 14 saved to {OUTPUT_PDF}")
    plt.close()


if __name__ == "__main__":
    main()
