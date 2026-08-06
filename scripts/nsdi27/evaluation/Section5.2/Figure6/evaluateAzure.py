from matplotlib import ticker
import matplotlib.pyplot as plt
import numpy as np
from util import (
    sliding_avg_Not_ConsiderNAN,
    sliding_avg_ConsiderNAN,
    sliding_avgForSR,
    export_metrics,
    result_db_path,
    colors,
    VLINE_LINEWIDTH,
    PLOT_LINEWIDTH,
)

# ---------------------------------------------------------------------------
# Raw measurement data (inlined so the figure can be regenerated standalone).
#
# Layout for all three arrays below: outer index = variant/column
#   0 -> Lazy, 1 -> S&R, 2 -> DRRS
# Inner list = per-second samples for that variant's run.
#   TIMES_SERIES[j]       : timestamps (seconds) for variant j
#   VALUES_SERIES_TPUT[j] : output-rate (records/s) samples for variant j
#   VALUES_SERIES_KLAG[j] : kafka-queue-time (ns) samples for variant j
# `float('nan')` entries mark gaps (e.g. operator downtime) and are handled
# specially by the sliding-average helpers below.
# ---------------------------------------------------------------------------

# fmt: off
KOALA_PATH = result_db_path("azure_lazy")
SAR_PATH = result_db_path("azure_SAR")
DRRS_PATH = result_db_path("azure_DRRS")

TIMES_SERIES,VALUES_SERIES_TPUT = export_metrics(DBS=[KOALA_PATH,SAR_PATH,DRRS_PATH],
    TARGET_OPERATOR="source",
    FIGURE="Aggregate throughput at source",
)
_, VALUES_SERIES_KLAG = export_metrics(DBS=[KOALA_PATH,SAR_PATH,DRRS_PATH],
    TARGET_OPERATOR="source",
    FIGURE="Average kafka lag",
)

# Shade color/alpha for the area under kafka-lag lines.
KAFKA_SHADE_COLOR = "lightgrey"
KAFKA_SHADE_ALPHA = 0.4

KAFKA_LAG = "Latency.KafkaQueueTime"
OUTPUT_RATE = "OutputRate"


# "Rate" metrics are cluster-wide totals that should be summed across subtasks
# (and shown with fig_type == "agg"). Everything else is averaged per subtask.
RATE_METRIC_MAP ={
    OUTPUT_RATE
}

# Row-indexed: VALUE_MAP[i] pairs with Metrics[i] and fig_types[i] below.
# Row 0 -> output-rate (aggregated), Row 1 -> kafka-lag (averaged).
VALUE_MAP = [
    VALUES_SERIES_TPUT,
    VALUES_SERIES_KLAG,
]

Metrics = [OUTPUT_RATE, KAFKA_LAG]

# One entry per row of the 2x3 subplot grid -- controls which sliding-average
# helper is used and, for "agg", validates that the metric is a rate metric.
fig_types = ["agg", "avg"]

SLIDING_WINDOW_SIZE = 10        # seconds of history in each sliding average
OFFSET = 15                     # shift x-axis left to hide warm-up ramp
INCREASE_RATE_TIME_SECONDS =120 # time at which input rate is bumped (vline)
SCALEUP_TIME_SECONDS = 183      # time at which the system scales up (vline)

# Recovery-time ratio annotation (kafka-lag row only). One label per variant.
RATIO_LABELS = ["", "3.2x", "5.2x"]
RATIO_FONTSIZE = 16

# VLINE_LINEWIDTH = 3             # width of the reference vlines
# PLOT_LINEWIDTH = 3              # width of the per-variant plot lines


###############################################################################
#                                   Plotting
###############################################################################

KAFKA_LAG_TITLE = "Average KafkaLag"
OUTPUT_RATE_TITLE = "Aggregated OutputRate"

# 2 rows (metrics: tput, kafka-lag) x 3 columns (variants: Lazy, S&R, DRRS)
# sharey='row' keeps the y-scale comparable across variants.
fig, axes = plt.subplots(2, 3, figsize=(10,3.3), sharey='row')

# Main plotting loop: one pass per subplot cell (i = row/metric, j = col/variant).
for i in range(2):
    for j in range(3):
        ax = axes[i,j]

        # ===== TIME LINES =====
        # Two reference vlines drawn on every cell: the moment the input rate
        # is increased, and the moment the system reacts by scaling up.
        ax.axvline(
            x=INCREASE_RATE_TIME_SECONDS,
            color="lightgrey",
            linestyle="-.",
            linewidth=VLINE_LINEWIDTH,
        )
        ax.axvline(
            x=SCALEUP_TIME_SECONDS,
            color="lightgrey",
            linestyle="--",
            linewidth=VLINE_LINEWIDTH,
        )
        # Pull the raw samples for this (metric, variant) cell.
        values= VALUE_MAP[i][j]
        times = TIMES_SERIES[j]

        # ===== PLOTTING =====
        # Two rendering modes:
        #   "agg" -> cluster-wide rate (sum-style smoothing, only rate metrics)
        #   "avg" -> per-instance average (NaN-skipping smoothing)
        if fig_types[i] == "agg":
            if Metrics[i] not in RATE_METRIC_MAP:
                raise ValueError(
                    f"Metric '{Metrics[i]}' is not allowed for 'agg' fig_type. "
                    f"Allowed metrics: {RATE_METRIC_MAP}"
                    )
            
            # --- Column 0: Lazy protocol ---
            if j == 0:
                values = sliding_avg_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                ax.plot(np.array(times) - OFFSET, values, label="Lazy", color=colors["Lazy"], linewidth=PLOT_LINEWIDTH)

            # --- Column 1: Stop-and-Restart (S&R) baseline ---
            # Tput uses the SR-aware smoother; downtime NaNs are replaced with
            # 0 so the line visibly drops to zero without being broken.
            if j == 1:
                if Metrics[i] == OUTPUT_RATE:
                    values = sliding_avgForSR(times, values, SLIDING_WINDOW_SIZE, SCALEUP_TIME_SECONDS+OFFSET, SCALEUP_TIME_SECONDS+OFFSET+60)
                    values = np.nan_to_num(np.array(values), nan=0.0)
                else:
                    values = sliding_avg_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)

                ax.plot(np.array(times) - OFFSET, values, label="S&R", color=colors["SR"], linewidth=PLOT_LINEWIDTH)

            # --- Column 2: DRRS baseline ---
            if j == 2:
                values = sliding_avg_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                ax.plot(np.array(times) - OFFSET, values, label="DRRS", color=colors["DRRS"], linewidth=PLOT_LINEWIDTH)

        # "avg" branch mirrors the "agg" branch but picks the NaN-aware
        # smoother for non-rate metrics and converts kafka-lag from ns->s.
        if fig_types[i] == "avg":
            if j == 0:
                if Metrics[i] in RATE_METRIC_MAP:
                    values = sliding_avg_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                else:
                    values = sliding_avg_Not_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                if Metrics[i] == KAFKA_LAG:
                    values = [v/1e9 for v in values]
                xs = np.array(times) - OFFSET
                ax.plot(xs, values, label="Lazy", color=colors["Lazy"], linewidth=PLOT_LINEWIDTH)
                if Metrics[i] == KAFKA_LAG:
                    ax.fill_between(xs, 0, values, color=KAFKA_SHADE_COLOR, alpha=KAFKA_SHADE_ALPHA)
            if j == 1:
                if Metrics[i] in RATE_METRIC_MAP:
                    values = sliding_avg_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                else:
                    values = sliding_avg_Not_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                if Metrics[i] == KAFKA_LAG:
                    values = [v/1e9 for v in values]
                # Drop NaN samples so matplotlib bridges the gap with a
                # straight segment instead of breaking the line.
                xs = np.array(times) - OFFSET
                ys = np.array(values, dtype=float)
                mask = ~np.isnan(ys)
                ax.plot(xs[mask], ys[mask], label="S&R", color=colors["SR"], linewidth=PLOT_LINEWIDTH)
                if Metrics[i] == KAFKA_LAG:
                    ax.fill_between(xs[mask], 0, ys[mask], color=KAFKA_SHADE_COLOR, alpha=KAFKA_SHADE_ALPHA)
            if j == 2:
                if Metrics[i] in RATE_METRIC_MAP:
                    values = sliding_avg_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                else:
                    values = sliding_avg_Not_ConsiderNAN(times, values, SLIDING_WINDOW_SIZE)
                if Metrics[i] == KAFKA_LAG:
                    values = [v/1e9 for v in values]
                xs = np.array(times) - OFFSET
                ax.plot(xs, values, label="DRRS", color=colors["DRRS"], linewidth=PLOT_LINEWIDTH)
                if Metrics[i] == KAFKA_LAG:
                    ax.fill_between(xs, 0, values, color=KAFKA_SHADE_COLOR, alpha=KAFKA_SHADE_ALPHA)

        # ===== TITLES & LABELS =====
        # Look up the right y-axis label and panel title for whichever metric
        # this row is showing. Many branches here are unused in the current
        # figure (which only plots OUTPUT_RATE + KAFKA_LAG) but are kept so
        # the same script can be repointed at other metrics by editing Metrics.
        if Metrics[i] == OUTPUT_RATE:
            metric_label = "Tput(r/s)"
            metric_title = OUTPUT_RATE_TITLE
        elif Metrics[i] == KAFKA_LAG:
            metric_label = "KafkaLag(s)"
            metric_title = KAFKA_LAG_TITLE
        else :
            raise ValueError(f"Unknown metric '{Metrics[i]}'")

        # ax.legend(loc="upper right")
        # ax.grid(True, axis='y', linestyle='--', alpha=0.7)
        ax.set_xlim(left =50, right = 350)
        if j == 0:
            ax.set_ylabel(metric_label, fontsize=19)
        if i == 0:
            ax.set_ylim([-(1.2e5*0.013),1.2e5])
        if i == 1:
            ax.set_ylim([-1,80])
            ax.text(0.97, 0.95, RATIO_LABELS[j], transform=ax.transAxes,
                    ha='right', va='top', color='black', fontsize=RATIO_FONTSIZE)


# ===== GLOBAL TICK STYLING =====
# Same x-ticks on every cell; fixed y-ticks per row so the grid is uniform.
target_xticks = [100,200,300]
for ax in axes.flat:
    ax.tick_params(axis='both', labelsize=17)
    ax.set_xticks(target_xticks)

for j in range(3):
    axes[0, j].set_yticks([0, 1e5])  # tput row: 0..150k r/s
    axes[1, j].set_yticks([0, 30, 60])           # kafka-lag row: 0..80 s

# Force scientific-notation formatting on the tput row's y-axis so the "1e5"
# exponent appears in the top-left corner instead of long "150000" tick labels.
for row_idx in range(1):
    formatter = ticker.ScalarFormatter()
    formatter.set_powerlimits((0, 0))
    axes[row_idx, 0].yaxis.set_major_formatter(formatter)
    axes[row_idx, 0].yaxis.get_offset_text().set_fontsize(14)

# plt.tight_layout()

fig.subplots_adjust(left=0.09, right=0.971, top=0.93, bottom=0.095,
                    wspace=0.14, hspace=0.3)

plt.savefig("azure.pdf", dpi=600)
# plt.show()