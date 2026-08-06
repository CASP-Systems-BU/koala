#!/usr/bin/env python3
"""
Figure: Multiple reconfigurations — (a) aggregate throughput at source vs time;
(b) state transferred vs time.
"""


from __future__ import annotations

import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np
from util import export_metrics, result_db_path, trim_to_shortest


SLIDING_WINDOW_SIZE = 5
SKIP_FIRST_N_RECORDS = 35
RATE_CHANGE_TIMES = [120, 300, 480, 660]
RESCALE_TIMES = [183, 363, 543, 723]
XMIN = 50.0
XMAX = 850
SAVE_DPI = 600

BYTES_PER_MB = 1024 * 1024

Lazy_COLOR = "#778aaf"

OUTPUT_PNG = "./Figure10.pdf"

# ---------------------------------------------------------------------------
# Paste data here
# ---------------------------------------------------------------------------
LABELS = ["Multiple Reconfigurations"]

KOALA_PATH = result_db_path("nexmark_query6_modified_multi_reconfig")
# fmt: off
TIMES_SERIES,VALUES_SERIES_TPUT = export_metrics(DBS=[KOALA_PATH],
    TARGET_OPERATOR="source",
    FIGURE="Aggregate throughput at source",
)
_, VALUES_SERIES_STATE_TRANSFER_BYTES = export_metrics(DBS=[KOALA_PATH],
    TARGET_OPERATOR="statefulMapper",
    FIGURE="Aggregate state transferred at target",
)
# ---------------------------------------------------------------------------

def sliding_avg(
    timestamps: list[int], values: list[float], window_size: int
) -> list[float]:
    # Mean of values in ``[t - window_size + 1, t]`` at each timestamp; NaNs ignored.
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
    if len(TIMES_SERIES) != n or len(VALUES_SERIES_TPUT) != n:
        raise SystemExit("LABELS and all *_SERIES must have the same length")
    state_series = VALUES_SERIES_STATE_TRANSFER_BYTES
    if len(state_series) != n:
        raise SystemExit("VALUES_SERIES_STATE_TRANSFER_BYTES must match LABELS length")

    # Per-series lengths are not required to agree: the throughput series comes
    # from "source" and the state-transfer series from "statefulMapper", which
    # report different numbers of samples. Each pair is trimmed to the shorter
    # length at plot time via trim_to_shortest()

    fig, (ax_tput, ax_state) = plt.subplots(
        2,
        1,
        figsize=(8, 4.6),
    )

    skip = max(0, int(SKIP_FIRST_N_RECORDS))
    t_y_min, t_y_max = float("inf"), float("-inf")
    s_y_min, s_y_max = float("inf"), float("-inf")

    for j in range(n):
        times_full, tput_raw = trim_to_shortest(TIMES_SERIES[j], VALUES_SERIES_TPUT[j])
        tput = sliding_avg(times_full, [float(v) for v in tput_raw], SLIDING_WINDOW_SIZE)
        times = times_full[skip:]
        tput = tput[skip:]
        ax_tput.plot(times, tput, color=Lazy_COLOR, linewidth=2, label=LABELS[j])
        finite_t = [v for v in tput if not np.isnan(v)]
        if finite_t:
            t_y_min = min(t_y_min, min(finite_t))
            t_y_max = max(t_y_max, max(finite_t))

        times_s_full, state_bytes = trim_to_shortest(TIMES_SERIES[j], state_series[j])
        times_s = times_s_full[skip:]
        state_mb = [float(v) / BYTES_PER_MB for v in state_bytes[skip:]]
        ax_state.plot(times_s, state_mb, color=Lazy_COLOR, linewidth=2, label=LABELS[j])
        ax_state.fill_between(times_s, 0, state_mb, color="lightgrey", alpha=0.5, zorder=0)
        finite_s = [v for v in state_mb if not np.isnan(v)]
        if finite_s:
            s_y_min = min(s_y_min, min(finite_s))
            s_y_max = max(s_y_max, max(finite_s))

    for ax in (ax_tput, ax_state):
        ax.set_xlim(left=XMIN, right=XMAX)
        ax.tick_params(axis="both", labelsize=13)
        for x in RATE_CHANGE_TIMES:
            ax.axvline(x=x, color="lightgrey", linestyle="-.", linewidth=2, zorder=0)
        for x in RESCALE_TIMES:
            ax.axvline(x=x, color="lightgrey", linestyle="--", linewidth=2, zorder=0)

    RESCALE_LABELS = ["up", "up", "down", "down"]
    for ax in (ax_tput, ax_state):
        for i, (x, lbl) in enumerate(zip(RESCALE_TIMES, RESCALE_LABELS), start=1):
            ax.annotate(
                str(i),
                xy=(x, 0.88),
                xycoords=("data", "axes fraction"),
                ha="center",
                va="center",
                fontsize=11,
                fontweight="bold",
                color="black",
                bbox=dict(boxstyle="circle,pad=0.25", facecolor="white", edgecolor="black", linewidth=1.2),
                zorder=5,
            )
            ax.annotate(
                lbl,
                xy=(x, 0.88),
                xytext=(14, 0),
                xycoords=("data", "axes fraction"),
                textcoords="offset points",
                ha="left",
                va="center",
                fontsize=13,
                color="black",
                zorder=5,
            )

    ax_tput.set_xlabel("(a) Tput over time (s)", fontsize=17, labelpad=7)
    ax_state.set_xlabel("(b) State migration over time (s)", fontsize=17, labelpad=9)

    if t_y_max > t_y_min:
        pad = t_y_max * 0.2
        lo = max(0, t_y_min - pad)
        hi = t_y_max + pad
        ax_tput.set_ylim(lo, top=hi)

    if s_y_max > s_y_min:
        pad = (s_y_max - s_y_min) * 0.15
        lo = max(0, s_y_min - pad)
        hi = s_y_max + pad * 2
        ax_state.set_ylim(bottom=lo, top=hi)

    ax_tput.set_ylabel("Tput (r/s)", fontsize=16)
    ax_state.set_ylabel("State size (MB)", fontsize=16)

    legend_handles = [
        Line2D([0], [0], color="lightgrey", linestyle="-.", linewidth=2, label="Load change"),
        Line2D([0], [0], color="lightgrey", linestyle="--", linewidth=2, label="Rescale"),
        Patch(facecolor="lightgrey", alpha=0.5, label="State migration"),
    ]
    fig.legend(
        handles=legend_handles,
        loc="upper center",
        ncol=3,
        fontsize=14,
        frameon=False,
        bbox_to_anchor=(0.5, 1.0),
    )

    plt.subplots_adjust(left=0.1, right=0.97, bottom=0.137, top=0.885, wspace=0.28, hspace=0.47)

    plt.savefig(OUTPUT_PNG, dpi=SAVE_DPI, bbox_inches="tight")
    # plt.show()


if __name__ == "__main__":
    main()