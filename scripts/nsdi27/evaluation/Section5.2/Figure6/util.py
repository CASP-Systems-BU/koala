from __future__ import annotations

import datetime
import numpy as np
import os
import re
import sqlite3
from collections import defaultdict
from typing import Iterable

# Experiment results are read straight out of scripts/results/, where
# moveMetricDBToResults() writes them - one folder per run named
# <QueryName>_<ResultKeyword>, e.g. "azure_lazy". Reading them in place means
# there is no copy/move step between running the suite and plotting.
# Resolved relative to this file so the figure scripts work from any working
# directory: Figure6/ -> Section5.2/ -> evaluation/ -> nsdi27/ -> scripts/
RESULTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
    os.pardir,
    os.pardir,
    os.pardir,
    "results",
)


def result_db_path(resultFolder: str) -> str:
    """Absolute path to the metricCollector.db inside one result folder.

    Raises if it is missing: sqlite3.connect() happily creates an empty database
    for a path that does not exist, which would turn a typo into empty plots
    instead of an error.
    """
    path = os.path.normpath(
        os.path.join(RESULTS_DIR, resultFolder, "metricDB", "metricCollector.db")
    )
    if not os.path.isfile(path):
        raise FileNotFoundError(
            f"No metricCollector.db for result folder '{resultFolder}'.\n"
            f"  looked in: {path}\n"
            f"  available: {sorted(os.listdir(RESULTS_DIR)) if os.path.isdir(RESULTS_DIR) else os.path.normpath(RESULTS_DIR) + ' not found'}"
        )
    return path

# Fixed color per variant so every figure in the paper is visually consistent.
# colors = {
#     "Lazy": "#dda96d",
#     "SR": "#617baf",
#     "DRRS": "#c06a6a",
#     "Remote": "#8b3a3a",
# }
colors = {
    "Lazy": "#778aaf",
    "SR": "#dda96d",
    "DRRS": "#be7b7b",
    "Remote": "#6E6868FF",
}

VLINE_LINEWIDTH = 2.6
PLOT_LINEWIDTH = 2.6

# A -- most desaturated, very high brightness
colors_a = {
    "Lazy": "#b8c8de",
    "SR": "#e6d5bc",
    "DRRS": "#dbc2c3",
    "Remote": "#c4b3a5",
}

# B -- moderately desaturated, very high brightness (recommended)
colors_b = {
    "Lazy": "#e2c6a4",  # bright warm orange
    "SR": "#aac0d8",  # bright soft blue
    "DRRS": "#d6b0b1",  # bright muted red
    "Remote": "#baa48f",  # bright mocha
}

# C -- slightly desaturated, very high brightness, retains some vibrancy
colors_c = {
    "Lazy": "#e5be90",  # bright apricot
    "SR": "#9bb5d4",  # bright medium blue
    "DRRS": "#e0abab",  # bright coral red
    "Remote": "#b09682",  # bright warm mocha
}

colors_d = {
    "Lazy": "#e09c6c",  # warm orange
    "SR": "#8cb0c9",  # medium blue
    "DRRS": "#e09c9c",  # coral red
    "Remote": "#a57f75",  # warm mocha
}


# ---------------------------------------------------------------------------
# Sliding-average helpers. All three walk the series with a window of
# `window_size` seconds ending at each timestamp. They differ in how NaN
# samples are treated -- this matters because NaNs here mean "no data"
# (e.g. during S&R downtime) and the right interpretation depends on whether
# the metric is a per-instance average or a cluster-wide rate.
# ---------------------------------------------------------------------------


# Used for per-instance metrics (e.g. kafka lag averaged over subtasks):
# divide by the actual count of real samples in the window. NaNs are skipped
# as if they weren't there -- missing data doesn't pull the mean toward zero.
def sliding_avg_Not_ConsiderNAN(timestamps, values, window_size):
    result = []
    n = len(timestamps)
    for i in range(n):
        t = timestamps[i]
        # find all points in [t - window_size + 1, t]
        lower_bound = t - window_size + 1
        # collect values within that range
        window_vals = [
            v
            for j, v in enumerate(values)
            if lower_bound <= timestamps[j] <= t and not np.isnan(v)
        ]
        if window_vals:
            result.append(np.mean(window_vals))
        else:
            result.append(np.nan)
    return result


# Used for aggregated rate metrics (tput): divide by the *full* window size,
# not the count of non-NaN samples. Missing seconds count as zero throughput,
# which is what you want when a rate drops to nothing during downtime.
def sliding_avg_ConsiderNAN(timestamps, values, window_size):
    result = []
    n = len(timestamps)
    for i in range(n):
        t = timestamps[i]
        # find all points in [t - window_size + 1, t]
        lower_bound = t - window_size + 1
        # collect values within that range
        window_vals = [
            v
            for j, v in enumerate(values)
            if lower_bound <= timestamps[j] <= t and not np.isnan(v)
        ]
        if window_vals:
            result.append(np.sum(window_vals) / window_size)
        else:
            result.append(np.nan)
    return result


# Specialised helper for the Stop-and-Restart (S&R) baseline. During the
# [rescaleStart, rescaleEnd] window the system is down, so the tput series is
# all NaNs -- but we still want to *draw* NaN (creates the downtime gap in
# the line) rather than silently averaging over it. Outside that window it
# behaves like sliding_avg_ConsiderNAN.
def sliding_avgForSR(timestamps, values, window_size, rescaleStart, rescaleEnd):
    result = []
    n = len(timestamps)
    seen_nan_before = False  # tracks whether we've already emitted the "gap starts" NaN
    for i in range(n):
        t = timestamps[i]
        # find all points in [t - window_size + 1, t]
        lower_bound = t - window_size + 1
        if np.isnan(values[i]) and rescaleStart <= t <= rescaleEnd:
            # Inside the known downtime window: preserve NaNs at the gap
            # boundary so matplotlib breaks the line, but once real data
            # returns mid-window fall back to the normal rate average.
            if not seen_nan_before:
                result.append(np.nan)
                seen_nan_before = True
            else:
                if np.isnan(values[i - 1]):
                    result.append(np.nan)
                else:
                    window_vals = [
                        v
                        for j, v in enumerate(values)
                        if lower_bound <= timestamps[j] <= t and not np.isnan(v)
                    ]
                    if window_vals:
                        result.append(np.sum(window_vals) / window_size)
                    else:
                        result.append(np.nan)

        else:
            # collect values within that range
            window_vals = [
                v
                for j, v in enumerate(values)
                if lower_bound <= timestamps[j] <= t and not np.isnan(v)
            ]
            if window_vals:
                result.append(np.sum(window_vals) / window_size)
            else:
                result.append(np.nan)
    return result

def repr_paste(obj) -> str:
    """repr() safe to paste into a .py file: NaN is emitted as float('nan'), not bare nan."""
    if isinstance(obj, (float, np.floating)) and np.isnan(obj):
        return "float('nan')"
    if isinstance(obj, list):
        return "[" + ", ".join(repr_paste(x) for x in obj) + "]"
    if isinstance(obj, tuple):
        return "(" + ", ".join(repr_paste(x) for x in obj) + ("," if len(obj) == 1 else "") + ")"
    if isinstance(obj, int):
        return str(obj)
    return repr(obj)

# --- Metric type strings (match worker / Go metric names) ---
PROCESSING_TIME = "Latency.ProcessingTime"
KAFKA_LAG = "Latency.KafkaQueueTime"
OUTPUT_RATE = "OutputRate"
INPUT_RATE = "InputRate"
NUM_BYTES_TRANSFERRED = "NumBytesTransferred"
KEY_LOOK_UP_TIME = "KeyLookUpTime"


def connect(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


def fetch_all_metrics(db_path: str) -> list[tuple[str, str, str, float]]:
    """Return all rows as list of (operator_id, timestamp, metric_type, metric_value)."""
    conn = connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT operator_id, timestamp, metric_type, metric_value FROM metrics ORDER BY rowid ASC"
        )
        return list(cur.fetchall())
    finally:
        conn.close()


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


def _parse_timestamp_to_datetime(timestamp_str: str) -> datetime.datetime:
    fmt = "%Y-%m-%d %H:%M:%S"
    striped = timestamp_str.split(".")[0]
    return datetime.datetime.strptime(striped, fmt)


def _parse_timestamp_to_seconds_since_start(timestamp_str: str, start_dt: datetime.datetime) -> int:
    return int((_parse_timestamp_to_datetime(timestamp_str) - start_dt).total_seconds())


def group_rows_by_worker(
    rows: Iterable[tuple[str, str, float]], start_time_str: str | None = None
) -> tuple[dict[int, list[int]], dict[int, np.ndarray]]:
    rows = list(rows)
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


def mean_non_nan(values: Iterable[float]) -> float | None:
    vals = [float(v) for v in values if not np.isnan(v)]
    if not vals:
        return None
    return float(np.mean(vals))


def load_all_num_bytes_transferred(
    db_path: str, operator_id_prefix: str | None = None
) -> tuple[list[tuple[str, str, float]], float]:
    """
    Sum all ``NumBytesTransferred`` metric_value rows.

    If ``operator_id_prefix`` is set (e.g. ``statefulMapper``), only rows with
    ``operator_id LIKE prefix%`` are included (same rule as ``fetch_metric_rows``).
    """
    conn = connect(db_path)
    try:
        cur = conn.cursor()
        if operator_id_prefix is None:
            cur.execute(
                """
                SELECT operator_id, timestamp, metric_value
                FROM metrics
                WHERE metric_type = ?
                ORDER BY operator_id, rowid ASC
                """,
                (NUM_BYTES_TRANSFERRED,),
            )
        else:
            cur.execute(
                """
                SELECT operator_id, timestamp, metric_value
                FROM metrics
                WHERE metric_type = ? AND operator_id LIKE ?
                ORDER BY operator_id, rowid ASC
                """,
                (NUM_BYTES_TRANSFERRED, f"{operator_id_prefix}%"),
            )
        rows = list(cur.fetchall())
    finally:
        conn.close()
    total = sum(r[2] for r in rows) if rows else 0.0
    return rows, total


def parse_state_size_gb(label: str) -> float | None:
    """Parse '10GB', '20 gb' -> 10.0, 20.0; return None if no match."""
    m = re.search(r"([\d.]+)\s*gb", label, re.I)
    if m:
        return float(m.group(1))
    m = re.search(r"^([\d.]+)$", label.strip())
    if m:
        return float(m.group(1))
    return None


def _floats_to_python(vals: Iterable) -> list[float]:
    """Plain Python floats (nan preserved) for pasting into figure scripts."""
    out: list[float] = []
    for v in vals:
        if isinstance(v, (float, np.floating)) and np.isnan(v):
            out.append(float("nan"))
        else:
            out.append(float(v))
    return out


def throughput_source_aggregated(
    db_path: str,
    operator_prefix: str = "source",
    metric_type: str = OUTPUT_RATE,
) -> tuple[list[int], list[float]]:
    """
    Fig 4(a): Throughput at operator — per-timestamp sum across workers (no sliding window).

    Default metric is **OutputRate** (same as realworldqueriesPlot.py aggregated row).
    **InputRate** is often mostly -1 in the DB: Rate.Get() returns -1 when no records were
    counted since the last Get() (see metric/rate.go), and we map -1 -> NaN, which can make
    the whole series NaN if samples are sparse. Use OutputRate unless you specifically need
    input-side rate; you can pass ``metric_type=INPUT_RATE``.
    """
    tm, vm = load_metric_by_operator(metric_type, operator_prefix, db_path)
    times, values = aggregate_metrics(vm, tm)
    return list(times), _floats_to_python(values)


def state_transfer_target_aggregated(
    db_path: str, operator_prefix: str = "statefulMapper"
) -> tuple[list[int], list[float]]:
    """Fig 4(b): NumBytesTransferred at target, aggregated."""
    tm, vm = load_metric_by_operator(NUM_BYTES_TRANSFERRED, operator_prefix, db_path)
    times, values = aggregate_metrics(vm, tm)
    return list(times), _floats_to_python(values)


def kafka_lag_source_averaged(
    db_path: str, operator_prefix: str = "source"
) -> tuple[list[int], list[float]]:
    
    tm, vm = load_metric_by_operator(KAFKA_LAG, operator_prefix, db_path)
    times, values = average_by_timestamp(vm, tm)
    return list(times), _floats_to_python(values)


def fig4c_scalars(
    db_paths: list[str],
    state_gbs: list[float],
    operator_prefix: str = "statefulMapper",
) -> tuple[list[float], list[float], list[float], list[float]]:
    """
    Fig 4(c): For each DB, mean processing time, mean key lookup time (full run),
    and total bytes transferred. Returns (state_gbs, proc_ns, key_ns, total_bytes).
    """
    if len(db_paths) != len(state_gbs):
        raise ValueError("db_paths and state_gbs must have the same length")

    proc_means: list[float] = []
    key_means: list[float] = []
    totals: list[float] = []

    for db_path in db_paths:
        tm, vm = load_metric_by_operator(PROCESSING_TIME, operator_prefix, db_path)
        _, proc_series = average_by_timestamp(vm, tm)
        m = mean_non_nan(proc_series)
        proc_means.append(float(m) if m is not None else float("nan"))

        tm2, vm2 = load_metric_by_operator(KEY_LOOK_UP_TIME, operator_prefix, db_path)
        _, key_series = average_by_timestamp(vm2, tm2)
        m2 = mean_non_nan(key_series)
        key_means.append(float(m2) if m2 is not None else float("nan"))

        _, total_b = load_all_num_bytes_transferred(db_path, operator_prefix)
        totals.append(float(total_b))

    return (
        list(state_gbs),
        proc_means,
        key_means,
        totals,
    )

def export_metrics(DBS: list[str], TARGET_OPERATOR: str, FIGURE: str) -> tuple[list[int], list[float]]:
    """Print Python list assignments for figure scripts."""
    
    if FIGURE == "Aggregate throughput at source":
        # aggregated throughput at source
        tt: list[list[int]] = []
        vv: list[list[float]] = []
        for p in DBS:
            t, v = throughput_source_aggregated(p, TARGET_OPERATOR)
            tt.append(t)
            vv.append(v)
        return (tt, vv)
    elif FIGURE == "Aggregate state transferred at target":
        # aggregated number of bytes transferred at target
        tt = []
        vv = []
        for p in DBS:
            t, v = state_transfer_target_aggregated(p, TARGET_OPERATOR)
            tt.append(t)
            vv.append(v)
        return (tt, vv)
    elif FIGURE == "Average kafka lag":
        # average kafka lag
        tt = []
        vv = []
        for p in DBS:
            t, v = kafka_lag_source_averaged(p, TARGET_OPERATOR)
            tt.append(t)
            vv.append(v)
        return (tt, vv)
    else:
        raise ValueError(f"Unknown FIGURE {FIGURE!r}")

