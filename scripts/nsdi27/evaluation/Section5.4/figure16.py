#!/usr/bin/env python3
"""
Figure 16: Workload characteristics (2x4 grid).

Row (a): state migrated for 4 key-locality levels (500K, 1M, 2M, 4M active keys).
Row (b): state migrated for 4 skew levels (0%, 25%, 50%, 75% hot keys).

Reads the lazy_* runs from scripts/results/ (via util.RESULTS_DIR) and writes
Figure16.pdf next to this script.
"""

from __future__ import annotations

import os

import matplotlib.pyplot as plt

from util import RESULTS_DIR, find_result_dir, state_transfer_target_aggregated


SKIP_FIRST_N_RECORDS = 15
XMIN_DATA_DIST = 160.0
XMAX_DATA_DIST = 300.0
SAVE_DPI = 600

BYTES_PER_GB = 1024 * 1024 * 1024

LAZY_COLOR = "#778aaf"
LINE_WIDTH = 2.4

YLABEL_FONTSIZE = 22
XLABEL_FONTSIZE = 18
TICK_FONTSIZE = 18
ROW_CAPTION_FONTSIZE = 20

# Row (a): key locality (500K, 1M, 2M, 4M active keys)
LOCALITY_CONFIGS = [
    ("lazy_500k_50k_25", "500K active keys"),
    ("lazy_1M_100k_25", "1M active keys"),
    ("lazy_2M_200k_25", "2M active keys"),
    ("lazy_4M_400k_25", "4M active keys"),
]

# Row (b): skew (0%, 25%, 50%, 75% hot keys)
SKEW_CONFIGS = [
    ("lazy_2M_200k_0", "0% hot keys"),
    ("lazy_2M_200k_25", "25% hot keys"),
    ("lazy_2M_200k_50", "50% hot keys"),
    ("lazy_2M_200k_75", "75% hot keys"),
]

OUTPUT_PDF = "./Figure16.pdf"


def _plot_state_row(axes_row, configs, label_kind: str, with_xlabel: bool) -> None:
    """Plot one row of state-migrated panels, one column per config."""
    skip = SKIP_FIRST_N_RECORDS
    for col_idx, (keyword, label) in enumerate(configs):
        result_dir = find_result_dir(RESULTS_DIR, keyword)
        if not result_dir:
            print(f"  Skipping {label_kind} {label}")
            continue

        db_path = os.path.join(result_dir, "metricDB", "metricCollector.db")
        if not os.path.exists(db_path):
            continue

        try:
            ax = axes_row[col_idx]
            times, vals = state_transfer_target_aggregated(db_path)
            if times:
                vals_gb = [v / BYTES_PER_GB for v in vals]
                times_skip = times[skip:]
                vals_skip = vals_gb[skip:]
                ax.plot(times_skip, vals_skip, color=LAZY_COLOR, linewidth=LINE_WIDTH)
                ax.set_ylabel("State migrated (GB)", fontsize=YLABEL_FONTSIZE)
                if with_xlabel:
                    ax.set_xlabel("Time (s)", fontsize=XLABEL_FONTSIZE)
                ax.tick_params(labelsize=TICK_FONTSIZE)
                ax.set_xlim(left=XMIN_DATA_DIST, right=XMAX_DATA_DIST)
                ax.set_title(label, fontsize=XLABEL_FONTSIZE)
        except Exception as e:
            print(f"  Error processing {label_kind} {label}: {e}")


def main() -> None:
    fig, axes = plt.subplots(2, 4, figsize=(18, 10))

    _plot_state_row(axes[0], LOCALITY_CONFIGS, "locality", with_xlabel=False)
    _plot_state_row(axes[1], SKEW_CONFIGS, "skew", with_xlabel=True)

    # Row captions
    fig.text(
        0.5, 0.50,
        "(a) Migrated state over time (s) across varying key locality.",
        ha="center", fontsize=ROW_CAPTION_FONTSIZE,
    )
    fig.text(
        0.5, 0.001,
        "(b) Migrated state over time (s) across varying skewness.",
        ha="center", fontsize=ROW_CAPTION_FONTSIZE,
    )

    plt.subplots_adjust(left=0.06, right=0.98, top=0.94, bottom=0.08, hspace=0.35, wspace=0.3)
    plt.savefig(OUTPUT_PDF, dpi=SAVE_DPI, bbox_inches="tight")
    print(f"✓ Figure 16 saved to {OUTPUT_PDF}")
    plt.close()


if __name__ == "__main__":
    main()
