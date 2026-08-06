#!/usr/bin/env python3
"""
Scale-down figure: throughput (top row) and state migration (bottom row) over
time across three setups (fetch-on-demand, progressive-default,
progressive-100). Data is read from the Python lists below only.
"""

from __future__ import annotations

from collections import deque

import matplotlib.lines as mlines
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
import numpy as np
from util import export_metrics, result_db_path, trim_to_shortest

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SLIDING_WINDOW_SIZE = 10  # seconds
SKIP_FIRST_N_RECORDS = 15
SCALEDOWN_TIME_EARLY_SECONDS = 120
SCALEUP_TIME_SECONDS = 183
XMIN = 50
XMAX = 600
SAVE_DPI = 600
BYTES_PER_MB = 1024 * 1024

# Columns kept from the raw 4-series data (drops "All at once" at index 3).
KEEP_COLS = [0, 1, 2]
COLUMN_TITLES = ["Fetch-on-demand", "Progressive-default", "Progressive-large"]

# Line / shade styling
LINE_COLOR = "#778aaf"
LINE_WIDTH = 2
LINE_STYLE = "-"
SHADE_COLOR = "lightgrey"
SHADE_ALPHA = 0.5
SCALEDOWN_LINE_COLOR = "lightgrey"
SCALEDOWN_LINE_WIDTH = 2

# Font sizes (tweak here)
TITLE_FONTSIZE = 19
YLABEL_FONTSIZE = 22
TICK_FONTSIZE = 18
LEGEND_FONTSIZE = 19.5
ROW_TITLE_FONTSIZE = 23
SCI_OFFSET_FONTSIZE = 13  # "1e5" unit label at top-left of y-axis
TITLE_PAD = 11  # distance (points) between column title and top of plot
XLABEL_PAD_TOP = 10  # distance (points) from x-axis to row-title for top row (a)
XLABEL_PAD_BOTTOM = 12  # distance (points) from x-axis to row-title for bottom row (b)

# Top-row (throughput) y-axis (tweak here)
TPUT_YLIM = (0, 9e5)
TPUT_YTICKS = [0, 2e5]

# Bottom-row (state migration) y-axis (tweak here); set YTICKS to None for auto
STATE_YLIM = (0, 80)
STATE_YTICKS = [0, 40]

# Annotation for the "X MB state migrated" label on the bottom-left subplot.
# Arrow tip, arrow tail, and text are fully decoupled — tune each independently.
#   TIP   = where the arrowhead points (fixed at the scaleup line)
#   TAIL  = where the arrow tail (non-head end) sits
#   TEXT  = where the label is drawn (unrelated to the arrow geometry)
# Shorter arrow → move TAIL closer to TIP.
# Text further right → increase ANNOT_TEXT_X.
ANNOT_ARROW_TIP_X = SCALEUP_TIME_SECONDS
ANNOT_ARROW_TIP_Y = 0
ANNOT_ARROW_TAIL_X = SCALEUP_TIME_SECONDS + 90
ANNOT_ARROW_TAIL_Y = STATE_YLIM[1] * 0.19
ANNOT_TEXT_X = SCALEUP_TIME_SECONDS + 150
ANNOT_TEXT_Y = STATE_YLIM[1] * 0.2
ANNOT_FONTSIZE = 16

OUTPUT_PNG = "./Figure12.pdf"

# ---------------------------------------------------------------------------
# Data  (paste here)
# ---------------------------------------------------------------------------

# fmt: off
LABELS = ['Fetch-on-demand', 'Progressive-default (25)', 'Progressive-50']
FETCHONDEMAND = result_db_path("nexmark_query6_modified_task_migration_fetch_on_demand")
PROGRESSIVEDEFAULT = result_db_path("nexmark_query6_modified_task_migration_progressive_default")
PROGRESSIVELARGE = result_db_path("nexmark_query6_modified_task_migration_progressive_large")
TIMES_SERIES, VALUES_SERIES_TPUT = export_metrics(
    DBS=[FETCHONDEMAND, PROGRESSIVEDEFAULT, PROGRESSIVELARGE],
    TARGET_OPERATOR="source",
    FIGURE="Aggregate throughput at source",
)
_, VALUES_SERIES_STATE_TRANSFER_BYTES= export_metrics(
    DBS=[FETCHONDEMAND, PROGRESSIVEDEFAULT, PROGRESSIVELARGE],  
    TARGET_OPERATOR="statefulMapper",
    FIGURE="Aggregate state transferred at target",
)
# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
nan = float("nan")


def sliding_avg(timestamps, values, window_size):
    """Mean of non-NaN values in [t - window_size + 1, t] at each timestamp."""
    result = []
    window = deque()
    window_ts = deque()
    for t, v in zip(timestamps, values):
        lower_bound = t - window_size + 1
        while window_ts and window_ts[0] < lower_bound:
            window_ts.popleft()
            window.popleft()
        if not np.isnan(v):
            window_ts.append(t)
            window.append(v)
        result.append(float(np.mean(window)) if window else float("nan"))
    return result


def _style_axis(ax):
    ax.axvline(
        x=SCALEUP_TIME_SECONDS,
        color=SCALEDOWN_LINE_COLOR,
        linestyle="--",
        linewidth=SCALEDOWN_LINE_WIDTH,
        zorder=0,
    )
    ax.set_xlim(left=XMIN, right=XMAX)
    ax.set_xticks([100, 300, 500])
    ax.tick_params(axis="both", labelsize=TICK_FONTSIZE)
    ax.set_axisbelow(True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    n = len(KEEP_COLS)
    fig, axes = plt.subplots(
        2, n,
        figsize=(11, 5.9),
        sharey="row",
    )

    # --- Top row: throughput ---
    skip = max(0, SKIP_FIRST_N_RECORDS)
    for idx, j in enumerate(KEEP_COLS):
        ax = axes[0, idx]
        times, tput_raw = trim_to_shortest(TIMES_SERIES[j], VALUES_SERIES_TPUT[j])
        values = sliding_avg(times, [float(v) for v in tput_raw], SLIDING_WINDOW_SIZE)
        times = times[skip:]
        values = values[skip:]

        ax.plot(times, values, color=LINE_COLOR, linewidth=LINE_WIDTH, linestyle=LINE_STYLE, zorder=3)
        _style_axis(ax)
        ax.set_title(COLUMN_TITLES[idx], fontsize=TITLE_FONTSIZE, pad=TITLE_PAD)

    axes[0, 0].set_ylim(*TPUT_YLIM)
    axes[0, 0].set_yticks(TPUT_YTICKS)
    axes[0, 0].set_ylabel("Tput (r/s)", fontsize=YLABEL_FONTSIZE)
    axes[0, 0].ticklabel_format(axis="y", style="sci", scilimits=(0, 0))
    axes[0, 0].yaxis.get_offset_text().set_fontsize(SCI_OFFSET_FONTSIZE)

    # --- Bottom row: state migration ---
    for idx, j in enumerate(KEEP_COLS):
        ax = axes[1, idx]
        times, state_bytes = trim_to_shortest(
            TIMES_SERIES[j], VALUES_SERIES_STATE_TRANSFER_BYTES[j]
        )
        values = np.asarray(state_bytes, dtype=float) / BYTES_PER_MB
        values = sliding_avg(times, values, SLIDING_WINDOW_SIZE)

        ax.plot(times, values, color=LINE_COLOR, linewidth=LINE_WIDTH, linestyle=LINE_STYLE, zorder=3)
        ax.fill_between(times, 0, values, color=SHADE_COLOR, alpha=SHADE_ALPHA, linewidth=0, zorder=2)
        _style_axis(ax)
        if idx == 0: 
            # Calculate total MB transferred (summing the raw values before sliding average)
            total_transferred_mb = np.nansum(values)
            print(f"Total state transferred for '{COLUMN_TITLES[idx]}': {total_transferred_mb:.2f} MB")
            
            # Arrow: head at TIP, tail at TAIL (text not attached).
            ax.annotate(
                "",
                xy=(ANNOT_ARROW_TIP_X, ANNOT_ARROW_TIP_Y),
                xytext=(ANNOT_ARROW_TAIL_X, ANNOT_ARROW_TAIL_Y),
                arrowprops=dict(
                    arrowstyle="-|>",
                    color=LINE_COLOR,
                    lw=1.8,
                    mutation_scale=20,
                ),
                zorder=4,
            )
            # Text: positioned independently of the arrow.
            ax.text(
                ANNOT_TEXT_X,
                ANNOT_TEXT_Y,
                f"{total_transferred_mb:.1f} MB state migrated",
                fontsize=ANNOT_FONTSIZE,
                ha="center",
                va="bottom",
                zorder=4,
            )

    axes[1, 0].set_ylim(*STATE_YLIM)
    if STATE_YTICKS is not None:
        axes[1, 0].set_yticks(STATE_YTICKS)
    axes[1, 0].set_ylabel("State size (MB)", fontsize=YLABEL_FONTSIZE)

    # Row titles: placed as the xlabel of the middle subplot so they sit centered
    # under each row's x-axis and span the full 3-setup width visually.
    axes[0, 1].set_xlabel("(a) Tput over time (s)", fontsize=ROW_TITLE_FONTSIZE, labelpad=XLABEL_PAD_TOP)
    axes[1, 1].set_xlabel("(b) State migration over time (s)", fontsize=ROW_TITLE_FONTSIZE, labelpad=XLABEL_PAD_BOTTOM)

    # Single-row legend above the top row.
    scaledown_handle = mlines.Line2D(
        [], [],
        color=SCALEDOWN_LINE_COLOR,
        linestyle="--",
        linewidth=SCALEDOWN_LINE_WIDTH,
        label="Task migration",
    )
    shade_handle = mpatches.Patch(
        facecolor=SHADE_COLOR,
        alpha=SHADE_ALPHA,
        label="State migration",
    )
    fig.legend(
        handles=[scaledown_handle, shade_handle],
        loc="upper center",
        ncol=2,
        fontsize=LEGEND_FONTSIZE,
        frameon=False,
        bbox_to_anchor=(0.5, 1.03),
    )

    fig.subplots_adjust(
        left=0.095,
        right=0.98,
        bottom=0.145,
        top=0.86,
        wspace=0.08,
        hspace=0.55,
    )

    plt.savefig(OUTPUT_PNG, dpi=SAVE_DPI)
    # plt.show()


if __name__ == "__main__":
    main()