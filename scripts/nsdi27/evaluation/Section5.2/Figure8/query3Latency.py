import matplotlib.pyplot as plt
from matplotlib import ticker
import numpy as np
from util import colors, sliding_avg_Not_ConsiderNAN, VLINE_LINEWIDTH, PLOT_LINEWIDTH, export_metrics, result_db_path

# fmt: off
KOALA_PATH = result_db_path("nexmark_query3_lazy")
REMOTE_PATH = result_db_path("nexmark_query3_Remote")
TIMES_SERIES, VALUES_SERIES_LA = export_metrics(DBS=[KOALA_PATH,REMOTE_PATH],
    TARGET_OPERATOR="join",
    FIGURE="Average processing time",
)
PROCESSING_TIME = "Latency.ProcessingTime"
OUTPUT_RATE = "OutputRate"
VALUE_MAP = [VALUES_SERIES_LA]
Metrics = [OUTPUT_RATE, PROCESSING_TIME]

SLIDING_WINDOW_SIZE = 10
OFFSET = 0
INCREASE_RATE_TIME_SECONDS = 120
SCALEUP_TIME_SECONDS = 183

fig, ax = plt.subplots(1, 1, figsize=(3.47, 2.2))

# ===== TIME LINES =====
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
lazy_values= VALUE_MAP[0][0]
lazy_values = lazy_values[:603]
remote_values = VALUE_MAP[0][1]
remote_values = remote_values[:603]
lazy_times = TIMES_SERIES[0]
lazy_times = lazy_times[:603]
remote_times = TIMES_SERIES[1]
remote_times = remote_times[:603]

lazy_values = sliding_avg_Not_ConsiderNAN(lazy_times, lazy_values, SLIDING_WINDOW_SIZE)
remote_values = sliding_avg_Not_ConsiderNAN(remote_times, remote_values, SLIDING_WINDOW_SIZE)
ax.plot(np.array(lazy_times) - OFFSET, np.array(lazy_values) / 1e6, label="Lazy", color=colors["Lazy"], linewidth=PLOT_LINEWIDTH)
ax.plot(np.array(remote_times) - OFFSET, np.array(remote_values) / 1e6, label="Remote", color=colors["Remote"], linewidth=PLOT_LINEWIDTH, linestyle="--")
ax.set_ylim([0, 3])

metric_label = "Latency(ms)"

# ax.set_ylabel(metric_label, fontsize=22)
ax.set_xlim(left =50, right = 350)
        

target_xticks = [100,200,300]

ax.tick_params(axis='both', labelsize=19)
ax.set_xticks(target_xticks)
# ax.set_yticks([0, 1e6])
ax.set_yticks([0, 2])

formatter = ticker.ScalarFormatter()
formatter.set_powerlimits((0, 0))
ax.yaxis.set_major_formatter(formatter)
ax.yaxis.get_offset_text().set_fontsize(15)

ax.set_xlabel("Time(s)", fontsize=19)

# fig.tight_layout()
fig.subplots_adjust(left=0.095, right=0.95, top=0.95, bottom=0.28, hspace=0.4)

plt.savefig("q3_latency.pdf", dpi=600)
# plt.show()