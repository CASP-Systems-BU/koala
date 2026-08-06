"""
Shared metric-reading helpers for the Section 5.3 (Cloudlab) figure script.

Reads the per-run metricCollector.db that the worker writes under
results/<run>/metricDB/ and turns raw metric rows into per-timestamp series
aggregated across workers. Kept separate from the figure scripts so the
database/aggregation logic stays isolated from the plotting code, matching the
util.py + figure-script split used in the other evaluation sections.
"""

from __future__ import annotations

import os
import sqlite3
from collections import defaultdict
from datetime import datetime

import numpy as np


# Experiment results are read straight out of scripts/results/, one folder per
# run named after the experiment (e.g. "parallelism_16_to_32_..."). Resolved
# relative to this file so the figure scripts work from any working directory:
# Cloudlab/ -> Section5.3/ -> evaluation/ -> nsdi27/ -> scripts/
RESULTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
    os.pardir,
    os.pardir,
    os.pardir,
    "results",
)


# --- Metric type strings (match worker / Go metric names) ---
OUTPUT_RATE = "OutputRate"
# Time a record waited in the Kafka queue, recorded on the source operator as
# nanoseconds (kafkaSource.go: time.Since(msg.Timestamp)). This is what the
# paper plots as "Kafka lag".
KAFKA_LAG = "Latency.KafkaQueueTime"


###############################################################################
#                         Database Aggregation Logic
###############################################################################


def connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def fetch_metric_rows(
    db_path: str, metric_type: str, operator_id_prefix: str
) -> list[tuple[str, str, float]]:
    """Rows for one metric and operator prefix: (operator_id, timestamp, metric_value)."""
    conn = connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT operator_id, timestamp, metric_value
            FROM metrics
            WHERE metric_type = ? AND operator_id LIKE ?
            ORDER BY rowid ASC
            """,
            (metric_type, f"{operator_id_prefix}%"),
        )
        return list(cur.fetchall())
    finally:
        conn.close()


def _parse_worker_id(operator_id: str) -> int:
    if operator_id is None or ":" not in operator_id:
        raise ValueError(f"operator_id {operator_id!r} must contain ':' and worker id")
    _, suffix = operator_id.split(":", 1)
    return int(suffix)


def _parse_timestamp_to_datetime(timestamp_str: str) -> datetime:
    fmt = "%Y-%m-%d %H:%M:%S"
    striped = timestamp_str.split(".")[0]
    return datetime.strptime(striped, fmt)


def _parse_timestamp_to_seconds_since_start(timestamp_str: str, start_dt: datetime) -> int:
    return int((_parse_timestamp_to_datetime(timestamp_str) - start_dt).total_seconds())


def group_rows_by_worker(
    rows: list[tuple[str, str, float]], start_time_str: str | None = None
) -> tuple[dict[int, list[int]], dict[int, np.ndarray]]:
    """Group rows by worker id and convert timestamps to seconds since start.

    The worker's -1 sentinel (Rate.Get() returns -1 when nothing was counted
    since the last sample) is mapped to NaN so downstream averaging skips it
    rather than reading a reporting gap as a real zero.
    """
    if not rows:
        raise ValueError("no rows to group")
    if start_time_str is None:
        start_time_str = rows[0][1]
    start_dt = _parse_timestamp_to_datetime(start_time_str)

    timestamps_by_worker: dict[int, list[int]] = {}
    values_by_worker: dict[int, list[float]] = {}

    for operator_id, timestamp, metric_value in rows:
        w = _parse_worker_id(operator_id)
        tsec = _parse_timestamp_to_seconds_since_start(timestamp, start_dt)
        timestamps_by_worker.setdefault(w, []).append(tsec)
        values_by_worker.setdefault(w, []).append(metric_value)

    out_vals: dict[int, np.ndarray] = {}
    for w, vals in values_by_worker.items():
        out_vals[w] = np.array([np.nan if v == -1 else v for v in vals])

    return timestamps_by_worker, out_vals


def load_metric_by_operator(
    metric_name: str, operator_name: str, db_path: str
) -> tuple[dict[int, list[int]], dict[int, np.ndarray]]:
    rows = fetch_metric_rows(db_path, metric_name, operator_name)
    if not rows:
        raise ValueError(
            f"No rows for metric_type={metric_name!r}, operator prefix={operator_name!r}, db={db_path!r}"
        )
    return group_rows_by_worker(rows, rows[0][1])


def aggregate_metrics(
    values_by_worker: dict[int, np.ndarray],
    timestamps_by_worker: dict[int, list[int]],
) -> tuple[list[int], list[float]]:
    """Sum metric values across workers per timestamp.

    A timestamp where every worker reported NaN stays NaN (nobody reported);
    one with at least one real sample sums the reals - which is what an
    aggregate rate should read during partial downtime.
    """
    agg: dict[int, float] = defaultdict(float)
    has_value: dict[int, bool] = defaultdict(bool)

    for worker_id, values in values_by_worker.items():
        timestamps = timestamps_by_worker[worker_id]
        for t, v in zip(timestamps, values):
            if not np.isnan(v):
                agg[t] += float(v)
                has_value[t] = True
            else:
                agg[t] += 0.0

    sorted_ts = sorted(agg.keys())
    sorted_vals: list[float] = []
    for t in sorted_ts:
        if has_value[t]:
            sorted_vals.append(agg[t])
        else:
            sorted_vals.append(float("nan"))

    return sorted_ts, sorted_vals


def average_by_timestamp(
    values_by_worker: dict[int, np.ndarray],
    timestamps_by_worker: dict[int, list[int]],
) -> tuple[list[int], list[float]]:
    """Average metric values across workers per timestamp, ignoring NaNs."""
    values_at_ts: dict[int, list[float]] = defaultdict(list)
    for worker_id, ts_list in timestamps_by_worker.items():
        val_list = values_by_worker[worker_id]
        for t, v in zip(ts_list, val_list):
            values_at_ts[t].append(float(v))

    times = sorted(values_at_ts.keys())
    avg_values: list[float] = []
    for t in times:
        vals = [v for v in values_at_ts[t] if not np.isnan(v)]
        if vals:
            avg_values.append(float(np.mean(vals)))
        else:
            avg_values.append(float("nan"))

    return times, avg_values


def throughput_source_aggregated(
    db_path: str, operator_prefix: str = "source"
) -> tuple[list[int], list[float]]:
    """Throughput at the source operators - per-timestamp sum across workers.

    Uses OutputRate rather than InputRate: InputRate is often mostly -1 in the
    DB (Rate.Get() returns -1 when no records were counted since the last
    sample), and -1 maps to NaN, which can blank a sparse series entirely.
    """
    tm, vm = load_metric_by_operator(OUTPUT_RATE, operator_prefix, db_path)
    times, values = aggregate_metrics(vm, tm)
    return list(times), values


def kafka_lag_aggregated(
    db_path: str, operator_prefix: str = "source"
) -> tuple[list[int], list[float]]:
    """Kafka queue time at the source operators - averaged across workers per timestamp.

    Only the source records Latency.KafkaQueueTime, so this reads the same
    "source" prefix as the throughput panel (not "coordinator", which records no
    metric) and averages across workers, matching the Section5.3/AWS figures.
    """
    tm, vm = load_metric_by_operator(KAFKA_LAG, operator_prefix, db_path)
    times, values = average_by_timestamp(vm, tm)
    return list(times), values


###############################################################################
#                         Figure Generation Helpers
###############################################################################


def sliding_avg(
    timestamps: list[int], values: list[float], window_size: int
) -> list[float]:
    """Mean of the values in ``[t - window_size + 1, t]`` at each timestamp; NaNs ignored."""
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


def find_result_dir(results_dir: str, keyword: str) -> str | None:
    """Newest subdirectory of ``results_dir`` whose name contains ``keyword``.

    Re-running an experiment does not overwrite its result folder:
    moveMetricDBToResults() appends a timestamp suffix
    (``<query>_<keyword>_YYYYMMDDThhmmss``) when the base folder already exists,
    so several folders can match one keyword. Return the most recently modified
    so plots read the latest run instead of a stale earlier one - picking the
    first os.listdir() match is non-deterministic and silently reads whichever.
    """
    matches = [
        os.path.join(results_dir, entry)
        for entry in os.listdir(results_dir)
        if keyword in entry and os.path.isdir(os.path.join(results_dir, entry))
    ]
    if not matches:
        return None
    newest = max(matches, key=os.path.getmtime)
    if len(matches) > 1:
        print(
            f"  [{keyword}] {len(matches)} matching result folders; "
            f"using newest: {os.path.basename(newest)}"
        )
    return newest
