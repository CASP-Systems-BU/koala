#!/usr/bin/env python3
"""
Figure 15: Key lookup overhead (broken-y processing/lookup time + KLT size).

Panel (a): broken-y axis with processing time (top) and key lookup time (bottom).
Panel (b): key-lookup-table (KLT) size per state size.

Reads the lazy_*Keys runs from scripts/results/ (via util.RESULTS_DIR) and writes
Figure15.pdf next to this script. Panel (b) needs the on-disk KLT sizes: pass
them with --klt-sizes, otherwise they are measured from the workers over SSH
(and panel (b) is omitted if neither source yields sizes).

Usage:
  python3 figure15.py                        # measure KLT sizes over SSH
  python3 figure15.py --klt-sizes 17,34,67,134
"""

from __future__ import annotations

import argparse
import os

import matplotlib.pyplot as plt

from util import (
    KEY_LOOK_UP_TIME,
    PROCESSING_TIME,
    RESULTS_DIR,
    TARGET_OPERATOR,
    average_by_timestamp,
    find_result_dir,
    get_worker_ips_from_config,
    load_metric_by_operator,
    mean_non_nan,
    measure_klt_sizes_from_workers,
)


SAVE_DPI = 600

LAZY_COLOR = "#778aaf"

CONFIGS = [
    ("lazy_5MKeys", "10GB"),
    ("lazy_10MKeys", "20GB"),
    ("lazy_20MKeys", "40GB"),
    ("lazy_40MKeys", "80GB"),
]

OUTPUT_PDF = "./Figure15.pdf"


def resolve_klt_sizes(cli_value: str) -> list[float] | None:
    """KLT sizes for panel (b): the --klt-sizes override, else SSH-measured.

    Returns None when neither source yields sizes, in which case panel (b) is
    omitted.
    """
    if cli_value:
        return [float(x.strip()) for x in cli_value.split(",")]

    print("Attempting to measure KLT sizes from workers...")
    scripts_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"  Looking for config files in: {scripts_dir}")
    worker_ips = get_worker_ips_from_config(scripts_dir)
    if not worker_ips:
        print("  Could not find worker IPs in config files")
        return None

    print(f"  Found {len(worker_ips)} unique workers in config: {worker_ips}")
    return measure_klt_sizes_from_workers(worker_ips)


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate Section 5.4 Figure 15")
    parser.add_argument(
        "--klt-sizes",
        default="",
        help="KLT table sizes for panel (b) as comma-separated values in MB "
        "(e.g., '17,34,67,134'). Measured from 'pebbleLookUpTable/' on workers "
        "over SSH if not provided.",
    )
    args = parser.parse_args()
    klt_sizes_mb = resolve_klt_sizes(args.klt_sizes)

    xtick_labels = []
    proc_ms = []
    key_us = []
    table_mb = []

    for keyword, size_label in CONFIGS:
        result_dir = find_result_dir(RESULTS_DIR, keyword)
        if not result_dir:
            continue

        db_path = os.path.join(result_dir, "metricDB", "metricCollector.db")
        if not os.path.exists(db_path):
            continue

        xtick_labels.append(size_label)

        # Processing time: ns → ms
        tm_proc, vm_proc = load_metric_by_operator(PROCESSING_TIME, TARGET_OPERATOR, db_path)
        _, proc_series = average_by_timestamp(vm_proc, tm_proc)
        proc_ns = mean_non_nan(proc_series)
        proc_ms.append(proc_ns / 1e6 if proc_ns is not None else float("nan"))

        # Key lookup time: ns → us
        tm_key, vm_key = load_metric_by_operator(KEY_LOOK_UP_TIME, TARGET_OPERATOR, db_path)
        _, key_series = average_by_timestamp(vm_key, tm_key)
        key_ns = mean_non_nan(key_series)
        key_us.append(key_ns / 1e3 if key_ns is not None else float("nan"))

        # KLT size
        if klt_sizes_mb and len(klt_sizes_mb) >= len(xtick_labels):
            table_mb.append(klt_sizes_mb[len(xtick_labels) - 1])

    if not xtick_labels:
        print(f"  No result directories found under {RESULTS_DIR}")
        return

    # Create figure matching key_table_overhead.py layout
    fig = plt.figure(figsize=(8, 2.5))
    gs = fig.add_gridspec(2, 2, height_ratios=[2, 1], hspace=0.08, wspace=0.32)
    ax_top = fig.add_subplot(gs[0, 0])
    ax_bot = fig.add_subplot(gs[1, 0], sharex=ax_top)
    ax_kt = fig.add_subplot(gs[:, 1])

    x = list(range(len(xtick_labels)))
    marker_kw = dict(
        markersize=9,
        markerfacecolor="none",
        markeredgecolor="#657493",
        markeredgewidth=2.0,
        linewidth=2.0,
    )

    # Panel (a): Processing time (top)
    ax_top.plot(x, proc_ms, marker="o", linestyle="-", color=LAZY_COLOR, **marker_kw)
    ax_top.set_ylim(0.4, 2.2)
    ax_top.set_yticks([1, 2])
    ax_top.set_yticklabels(["1ms", "2ms"])
    ax_top.spines["bottom"].set_visible(False)
    ax_top.xaxis.tick_top()
    ax_top.tick_params(axis="x", which="both", bottom=False, labeltop=False)
    ax_top.grid(axis="x", color="lightgrey", linestyle="-", linewidth=1.5, alpha=0.8)
    ax_top.set_axisbelow(True)
    ax_top.tick_params(axis="both", labelsize=14)

    # Panel (a): Key lookup time (bottom)
    ax_bot.plot(x, key_us, marker="s", linestyle="-", color=LAZY_COLOR, **marker_kw)
    ax_bot.set_ylim(0, 90)
    ax_bot.set_yticks([0, 50])
    ax_bot.set_yticklabels(["0", "50us"])
    ax_bot.spines["top"].set_visible(False)
    ax_bot.set_xticks(x)
    ax_bot.set_xticklabels(xtick_labels)
    ax_bot.set_xlim(x[0] - 0.35, x[-1] + 0.35)
    ax_bot.grid(axis="x", color="lightgrey", linestyle="-", linewidth=1.5, alpha=0.8)
    ax_bot.set_axisbelow(True)
    ax_bot.tick_params(axis="both", labelsize=14)

    # Break ticks
    s = 0.5
    tick_kw = dict(marker=[(-1, -s), (1, s)], markersize=14, linestyle="none",
                   color="k", mec="k", mew=1.5, clip_on=False)
    ax_top.plot([0, 1], [0, 0], transform=ax_top.transAxes, **tick_kw)
    ax_bot.plot([0, 1], [1, 1], transform=ax_bot.transAxes, **tick_kw)

    ax_bot.set_xlabel("(a) Minor key lookup time", fontsize=18, labelpad=9)

    # Panel (b): KLT size
    if table_mb and len(table_mb) == len(xtick_labels):
        ax_kt.plot(x, table_mb, marker="D", linestyle="-", color=LAZY_COLOR, **marker_kw)
        ax_kt.set_xticks(x)
        ax_kt.set_xticklabels(xtick_labels)
        ax_kt.set_xlim(x[0] - 0.35, x[-1] + 0.35)
        ax_kt.grid(axis="x", color="lightgrey", linestyle="-", linewidth=1.5, alpha=0.8)
        ax_kt.set_axisbelow(True)
        ax_kt.set_ylabel("KLT size (MB)", fontsize=16, labelpad=4)
        ax_kt.set_xlabel("(b) KLT size", fontsize=18, labelpad=9)
        ax_kt.set_ylim(bottom=0, top=max(table_mb) * 1.3)
        ax_kt.tick_params(axis="both", labelsize=14)

    plt.subplots_adjust(left=0.13, right=0.97, bottom=0.28, top=0.81)
    plt.savefig(OUTPUT_PDF, dpi=SAVE_DPI, bbox_inches="tight")
    print(f"✓ Figure 15 saved to {OUTPUT_PDF}")
    plt.close()


if __name__ == "__main__":
    main()
