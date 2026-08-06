import os
import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np   

# ──────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────

data_dir       = "./data"
db_filename    = "metricCollector.db"
metrics_table  = "metrics"
warmup_seconds = 60                      # warm up time - skip first minute
csv_name_in    = "ingestion_times.csv"
csv_name_proc  = "proc_time.csv"

def load_times(folder, warmup_s=60):
    """
    Returns two *lists* (ingestion, processing) with the first
    `warmup_s` seconds removed.  If either CSV is missing it returns None.
    """
    in_path   = os.path.join(folder, csv_name_in)
    proc_path = os.path.join(folder, csv_name_proc)
    if not (os.path.exists(in_path) and os.path.exists(proc_path)):
        print(f"    (missing CSVs in {folder})")
        return None

    ing  = np.loadtxt(in_path,  delimiter=',', dtype=np.int64)
    proc = np.loadtxt(proc_path, delimiter=',', dtype=np.int64)

    t0     = min(ing[0], proc[0])
    cutoff = t0 + warmup_s * 1_000_000_000

    ing  = ing[ing  >= cutoff]
    proc = proc[proc >= cutoff]
    n = min(len(ing), len(proc))      
    ing, proc = ing[:n], proc[:n]   
    return ing.tolist(), proc.tolist()


def plot_graph(xs, ys, title):
    """Scatter + x=y reference line."""
    if not xs or not ys:
        print(f"    (no data for {title})")
        return
    plt.figure()
    plt.plot(xs, ys)
    lo, hi = min(min(xs), min(ys)), max(max(xs), max(ys))
    plt.plot([lo, hi], [lo, hi], linestyle='--', label='x = y')
    plt.xlabel("ingestion timestamp")
    plt.ylabel("processing timestamp")
    plt.title(title)
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    
def windowed_mean(x, t, width_s):
    edges  = np.arange(0, t[-1] + width_s, width_s)
    idx    = np.digitize(t, edges) - 1         
    counts = np.bincount(idx)
    sums   = np.bincount(idx, weights=x)
    keep   = counts > 0
    centers = edges[:-1][keep] + width_s / 2.0
    means   = sums[keep] / counts[keep]
    return centers, means


def plot_completeness(ev_ts, pt_ts, title, unit='s',warmup_s=60):
    """
    ev_ts : Per‑record ingestion timestamps
    pt_ts : Matching processing timestamps 
    """
    if not ev_ts or not pt_ts:
        print("(no data supplied)")
        return

    ev = np.asarray(ev_ts, dtype=np.int64)
    pt = np.asarray(pt_ts, dtype=np.int64)
    ev = (ev // 1_000_000) * 1_000_000
    pt = (pt // 1_000_000) * 1_000_000

    if warmup_s:
        cutoff = min(ev[0], pt[0]) + warmup_s * 1_000_000_000
        keep   = (ev >= cutoff) & (pt >= cutoff)
        ev, pt = ev[keep], pt[keep]

    # --- normalise ---------------------------------------
    t0   = min(ev[0], pt[0])
    mult = 1_000_000 if unit == "ms" else 1_000_000_000   # ns -> ms|s
    ev_rel, pt_rel = (ev - t0) / mult, (pt - t0) / mult

    # --- completeness curve ---------------------------------------------
    order        = np.argsort(pt_rel)
    pt_sorted    = pt_rel[order]
    ev_sorted    = ev_rel[order]
    completeness = np.maximum.accumulate(ev_sorted)      

    # --- lag ------------------------------------------------------------
    lag = np.maximum((pt_sorted - completeness) / 1_000_000, 0)     

    fig, (ax0, ax1) = plt.subplots(
        2, 1, figsize=(7, 6), sharex=False,
        gridspec_kw=dict(height_ratios=[3, 1], hspace=0.25)
    )

    ax0.step(completeness, pt_sorted, where="post",
             color="tab:red", label="Reality", linewidth=2)
    lim = max(completeness.max(), pt_sorted.max())
    ax0.plot([0, lim], [0, lim], "--", color="gray", label="Ideal (x = y)")

    ax0.fill_between(completeness, completeness, pt_sorted,
                     color="tab:red", alpha=0.15, label="Lag area")

    ax0.set_ylabel(f"Processing Time ({unit})")
    ax0.set_xlabel(f"Ingestion Time ({unit})")
    ax0.set_title(title or "Ingestion‑time completeness vs processing time")
    ax0.grid(True, linestyle=":")
    ax0.legend()

    window_s = 25                       # <-- window size for average lag
    lag_ms   = lag * (1000 if unit == "s" else 1)   
    centers, means = windowed_mean(lag_ms, pt_sorted, window_s)

    ax1.plot(centers, means, marker="o", linewidth=1.5, color="tab:blue")
    ax1.set_xlabel(f"Processing Time ({unit})")
    ax1.set_ylabel("Avg Lag (ms)")
    ax1.set_title(f"Average Kafka queue time (window = {window_s}s)")
    ax1.grid(True, linestyle=":")

    ax1.vlines(pt_sorted, 0, lag_ms, linewidth=0.3, alpha=0.2)

    plt.tight_layout()
    plt.show()

def plot_results(exp_prefix, variables, *, var_name,
                 include_kafka=False, drop_first_records=0):
    """
    exp_prefix : experiment prefix e.g. 'inprate='
    variables  : what we vary in the experiment e.g. [1000, 5000, 10000, ...]
    var_name   : name of variable that we vary
    include_kafka : bool       true if it is a Kafka experiment
    drop_first_records : int   warm‑up rows to drop *per (operator,metric_type)*
    """

    print(f"Collecting from: {os.path.abspath(data_dir)}")
    dfs = []

    for v in variables:
        folder  = f"{exp_prefix}{v}"
        db_path = os.path.join(data_dir, folder, db_filename)

        if not os.path.exists(db_path):
            print(f"skipping  {var_name}={v} (DB not found)")
            continue

        try:
            with sqlite3.connect(db_path) as conn:
                df = pd.read_sql_query(
                    f"""SELECT rowid,
                               operator_id,
                               metric_type,
                               metric_value,
                               timestamp          
                        FROM {metrics_table}""",
                    conn
                )
        except Exception as e:
            print(f"error reading {db_path}: {e}")
            continue

        # trim warm‑up per (operator,metric_type)
        df = df[df['metric_value'] != 0]  # remove zero values
        if drop_first_records:
            df = (df.groupby(["operator_id", "metric_type"], as_index=False)
                      .apply(lambda g: g.iloc[drop_first_records:-6])
                      .reset_index(drop=True))
        
        x = df[df["metric_value"] <0]
        if not x.empty:
            for i, row in x.iterrows():
                print(f"  {row['operator_id']} {row['metric_type']} {row['metric_value']}")

        # tag the independent variable
        df[var_name] = v

        dfs.append(df)
        print(f"{len(df):5d} rows  ({var_name}={v})")
        avg = df.groupby(["operator_id","metric_type"])["metric_value"].mean()
        print("avg value", avg)

    if not dfs:
        print("Nothing to plot.")
        return

    raw = pd.concat(dfs, ignore_index=True)

    if include_kafka and "OutputRate" in raw.metric_type.unique():
        kafka_df = pd.DataFrame({
            "operator_id": "kafka",
            "metric_type": "OutputRate",
            "metric_value": variables,      
            var_name: variables,
            "timestamp": pd.NaT,            
        })
        raw = pd.concat([raw, kafka_df], ignore_index=True)


    raw["timestamp"] = pd.to_datetime(raw["timestamp"], errors="coerce", utc=True)
    raw["t_rel"] = (
        raw.groupby(var_name)["timestamp"]
           .transform(lambda s: (s - s.min()).dt.total_seconds())
    )

    # --------------------------------------------------------------------  BAR CHARTS)

    for metric in raw.metric_type.unique():
        sub = raw[raw.metric_type == metric]
        if sub.empty:
            continue

        plt.figure(figsize=(9, 5))
        sns.barplot(
            data=sub,
            x=var_name, y="metric_value",
            hue="operator_id",
            estimator="mean",         
            errorbar="sd",                        
            palette="viridis"
        )
        t=f"Average '{metric}' vs {var_name}"
        plt.title(t)
        plt.xlabel(var_name.replace("_", " ").title())
        plt.ylabel(f"average {metric} value")
        plt.legend(title="Operator", bbox_to_anchor=(1.02, 1), loc="upper left")
        plt.tight_layout()
        # plt.savefig(f"graphs/expensive computation/exp1/graphs/{metric} vs {var_name}.png")
        plt.show()

    # --------------------------------------------------------------------  TIMELINES
    for metric in set(raw.metric_type.unique()):
        base = (raw[raw.metric_type == metric]
                  .sort_values([var_name, "operator_id", "t_rel"]))

        if base.empty:
            continue

        g = sns.relplot(
            data=base,
            kind="line",
            x="t_rel", y="metric_value",
            hue="operator_id",
            col=var_name, col_wrap=2,
            height=3.0, aspect=1.6,
            linewidth=0.8, legend="full"
        )
        xlabel = "Time (s)"
        g.set_axis_labels(xlabel, metric)
        g.fig.subplots_adjust(top=0.88)
        g.fig.suptitle(
            f"{metric} timeline "
        )
        for ax in g.axes.flatten():
            ax.grid(True, linestyle=":", alpha=0.6)

        # plt.savefig(f"graphs/expensive computation/exp1/graphs/{metric} timeline {var_name}.png")
        plt.show()


inprates = [10000, 50000]


print("\n─── Input‑rate experiments ─────────────────────────")
# for kafka ingestion time vs processing time plots
# for r in inprates:
#     folder = os.path.join(data_dir, f"exp2-inprate-{r}")
#     pair   = load_times(folder)
#     if pair:
#         ing, proc = pair
#         plot_completeness(ing, proc, f"input rate {r}", unit='s')

plot_results("exp2-inprate-", inprates,
             var_name="inprate",
             drop_first_records=6, include_kafka=True) 


