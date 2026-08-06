"""
Shared metric-reading helpers for the Section 5.4 figure script.

Reads the per-run metricCollector.db that the worker writes under
results/<run>/metricDB/ and turns raw metric rows into per-timestamp series
aggregated across workers, plus the SSH-based key-lookup-table sizing used by
Figure 15. Kept separate from the figure scripts so the database/aggregation
logic stays isolated from the plotting code, matching the util.py +
figure-script split used in the other evaluation sections.
"""

from __future__ import annotations

import json
import os
import sqlite3
import subprocess
from collections import defaultdict
from datetime import datetime

import numpy as np


# Experiment results are read straight out of scripts/results/, one folder per
# run named after the experiment (e.g. "lazy_5MKeys_..."). Resolved relative to
# this file so the figure scripts work from any working directory:
# Section5.4/ -> evaluation/ -> nsdi27/ -> scripts/
RESULTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    os.pardir,
    os.pardir,
    os.pardir,
    "results",
)


# --- Metric type strings (match worker / Go metric names) ---
PROCESSING_TIME = "Latency.ProcessingTime"
OUTPUT_RATE = "OutputRate"
NUM_BYTES_TRANSFERRED = "NumBytesTransferred"
KEY_LOOK_UP_TIME = "KeyLookUpTime"
TARGET_OPERATOR = "statefulMapper"

# Path to the experiment suite configuration, relative to the scripts root.
FULL_SUITE_PATH = "nsdi27/evaluation/Section5.4/fullSuite.json"


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
    aggregate reads correctly during partial downtime.
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


def mean_non_nan(values) -> float | None:
    """Mean of the finite values, or None when every value is NaN."""
    vals = [float(v) for v in values if not np.isnan(v)]
    if not vals:
        return None
    return float(np.mean(vals))


def throughput_source_aggregated(
    db_path: str, operator_prefix: str = TARGET_OPERATOR, metric_type: str = OUTPUT_RATE
) -> tuple[list[int], list[float]]:
    """Throughput at the target operator - per-timestamp sum across workers."""
    tm, vm = load_metric_by_operator(metric_type, operator_prefix, db_path)
    times, values = aggregate_metrics(vm, tm)
    return list(times), values


def state_transfer_target_aggregated(
    db_path: str, operator_prefix: str = TARGET_OPERATOR
) -> tuple[list[int], list[float]]:
    """NumBytesTransferred at the target operator - per-timestamp sum across workers."""
    tm, vm = load_metric_by_operator(NUM_BYTES_TRANSFERRED, operator_prefix, db_path)
    times, values = aggregate_metrics(vm, tm)
    return list(times), values


###############################################################################
#                       Key-Lookup-Table Sizing (SSH)
###############################################################################


def get_worker_ips_from_config(scripts_dir: str = ".") -> list[str] | None:
    """Unique worker IPs from a state-size experiment config in fullSuite.json.

    Walks up from ``scripts_dir`` to the scripts root (the directory containing
    nexmarkJson/) so the paths in fullSuite.json resolve regardless of the
    caller's working directory.
    """
    current = os.path.abspath(scripts_dir)
    scripts_root = None

    while current:
        if os.path.isdir(os.path.join(current, "nexmarkJson")):
            scripts_root = current
            break
        parent = os.path.dirname(current)
        if parent == current:  # reached filesystem root
            break
        current = parent

    if not scripts_root:
        print(f"  Debug: Could not find scripts root (nexmarkJson directory) starting from {scripts_dir}")
        return None

    full_suite_path = os.path.abspath(os.path.join(scripts_root, FULL_SUITE_PATH))

    try:
        with open(full_suite_path, "r") as f:
            suite = json.load(f)
        if "Experiments" not in suite or not suite["Experiments"]:
            return None

        # A state-size experiment (state_10gb, state_20gb, ...) points at the
        # config whose workers hold the key-lookup tables we want to measure.
        state_exp = None
        for exp in suite["Experiments"]:
            if exp.get("Name", "").startswith("state_"):
                state_exp = exp
                break

        if not state_exp or "ConfigPath" not in state_exp:
            return None

        config_path = os.path.abspath(os.path.join(scripts_root, state_exp["ConfigPath"]))
        with open(config_path, "r") as cf:
            config = json.load(cf)
        if "WorkerIPs" in config:
            return list(dict.fromkeys(config["WorkerIPs"]))

    except Exception as e:
        print(f"  Warning: Could not extract worker IPs from {full_suite_path}: {e}")

    return None


def measure_klt_sizes_from_workers(worker_ips: list[str]) -> list[float] | None:
    """Measure key-lookup-table sizes from workers over SSH.

    Returns [10GB, 20GB, 40GB, 80GB] table sizes in MB (max across workers), or
    None if any size could not be measured on every worker.
    """
    configs = ["10GB", "20GB", "40GB", "80GB"]
    sizes: dict[str, list[int]] = {config: [] for config in configs}

    for worker_ip in worker_ips:
        try:
            cmd = "du -sh ~/disaggregated-streaming/pebbleLookUpTable/nexmark_q6_mod/128ByKey_consistent_*"
            result = subprocess.run(
                f'ssh {worker_ip} "{cmd}"',
                shell=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
            for line in result.stdout.strip().split("\n"):
                if not line:
                    continue
                parts = line.split()
                if len(parts) >= 2:
                    size_str = parts[0]
                    path = " ".join(parts[1:])
                    size_mb = int(size_str.rstrip("M"))
                    for config in configs:
                        if config in path:
                            sizes[config].append(size_mb)
                            break
        except Exception as e:
            print(f"  Warning: Could not measure KLT on {worker_ip}: {e}")

    if all(sizes.values()):
        klt_max = [max(sizes[config]) for config in configs]
        print(f"  Measured KLT sizes: {klt_max} MB")
        return [float(sz) for sz in klt_max]
    return None


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
