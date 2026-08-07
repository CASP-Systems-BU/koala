# Koala — Artifact Evaluation Guide (NSDI 2027)

Welcome to the artifact evaluation guide for **Koala**. This document contains everything
needed to download, build, and run the system, and to reproduce the results in the
experimental section of the paper. 

**We are applying for all three badges: {Available, Functional, Reproduced}.**

Koala is a disaggregated stream processing system that reconfigures (scales out, scales in,
rebalances, migrates tasks) without stopping the dataflow. State is migrated lazily after a reconfiguration, so the system keeps consuming input while
the new task assignment warms up, rather than pausing to transfer state up front.

---

## Table of contents

- [Available Badge](#available-badge)
- [Functional Badge](#functional-badge)
- [Reproduced Badge](#reproduced-badge)
- [Setup](#setup) — [requirements](#1-requirements), [download](#2-download), [install](#3-install-and-build), [cluster](#4-cluster-setup)
- [Hello world](#hello-world) — a 5-minute end-to-end run
- [Experiments](#experiments)
  - [Section 5.2 — Figures 6 and 8 (main result)](#section-52--figures-6-and-8)
  - [Section 5.3 — Figures 10–13, reconfiguration scenarios](#section-53--figures-1013-reconfiguration-scenarios)
    - [Section 5.3.1 — Figure 9, high parallelism (optional)](#section-531--figure-9-high-parallelism-optional)
  - [Section 5.4 — Figures 14–16, microbenchmarks (optional)](#section-54--figures-1416-microbenchmarks-optional)
- [Appendix A: rebuilding the warm-up state](#appendix-a-rebuilding-the-warm-up-state)
- [Appendix B: writing your own query](#appendix-b-writing-your-own-query)
- [Troubleshooting](#troubleshooting)

---

## Available Badge

The artifact is available in our GitHub repository,
[CASP-Systems-BU/koala](https://github.com/CASP-Systems-BU/koala), together with this
README (dependencies, getting-started guide, and per-figure reproduction steps).

## Functional Badge

Koala is a modular streaming runtime: the dataflow API, the coordinator, the workers, the
state backends, and the reconfiguration protocols are separate components, so a protocol
or a state backend can be swapped without touching the queries. The main claim is that
this architecture supports non-disruptive reconfiguration for a range of workloads and
reconfiguration scenarios.

The artifact demonstrates this through:

| Component | What it provides |
|---|---|
| [api/dataflow/](api/dataflow/) | The operator interface and its implementations: source, sink, map, filter, flatmap, join, custom-window join, tumbling and sliding windows, and the stateful variants (`statefulMap`, `statefulFlatmap`, `statefulMap2State`). |
| [api/stateClient/](api/stateClient/), [state/](state/) | The state-access interface and the state backends behind it (local Pebble, remote Pebble, TiKV), plus the key lookup table used for lazy state migration. |
| [coordinator/](coordinator/) | Job deployment, task placement, and the reconfiguration protocols. |
| [worker/](worker/) | The task runtime: batch processing, data-plane and state-plane communication, migration of state on demand. |
| [internal/](internal/) | Configuration, gRPC control plane, and shared internals. |
| [kafka/](kafka/), [cmd/](cmd/) | Kafka source/producer integration and the binaries (`coordinator`, `worker`, `client`, the three producers, `remotePebble`). |
| [query/](query/) | The queries used in the paper: `azure`, `borg`, `taxi`, `twitch`, and the `nexmark` suite, plus small [examples](query/examples/). |
| [scripts/](scripts/) | The experiment harness: cluster preparation, repo sync, Kafka cluster and producer lifecycle, single-experiment and suite runners, and the figure scripts. |

The reconfiguration protocol and the state backend are selected per experiment through the
JSON config, so the same query runs under Koala and under each baseline without code
changes:

| Name in the paper | How it is configured |
|---|---|
| Koala | `"ReconfigProtocol": "lazy"`, `"StateBackendType": "pebble"` |
| S&R (stop-and-restart) | `"ReconfigProtocol": "stop-and-restart"`, `"StateBackendType": "pebble"` |
| Remote | `"ReconfigProtocol": "stop-and-restart"`, `"StateBackendType": "remote-pebble"` — state stays in remote storage |
| DRRS | its own implementation, on the `drrs` branch (`drrs_q3` for Nexmark Q3) |

See [Hello world](#hello-world) for a single-node run of the full system.

## Reproduced Badge

The experimental section supports three claims:

1. **Non-disruptive reconfiguration.** Koala keeps processing during a scale-out: no
   downtime, no latency spike, and the input backlog is consumed faster than with the
   baselines (Section 5.2, Figures 6 and 8).
2. **Generality and scale.** The same mechanism holds at higher parallelism and across
   reconfiguration types — repeated reconfigurations, concurrent reconfiguration of
   multiple operators, skew-driven rebalancing, and scale-in with task migration
   (Section 5.3, Figures 9–13).
3. **The mechanism is cheap.** Lazy state access migrates only the active working set, so
   cost is governed by key locality and skew rather than total state size, and the key
   lookup table stays small (Section 5.4, Figures 14–16).

We compare against three baselines, all implemented in this repository and run through the
same harness: **S&R** (stop-and-restart, the standard approach), **Remote** (state served
from disaggregated storage without migration), and **DRRS**.

All experiments run on Ubuntu Linux in two settings:

- **AWS** — used for Sections 5.2 and 5.3 (Figures 6, 8, 10–13). Working directory on the
  nodes is `~/ssd/disaggregated-streaming`.
- **CloudLab Utah** (`c6620` nodes) — used for Sections 5.3.1 and 5.4 (Figures 9, 14–16).
  Working directory on the nodes is `~/disaggregated-streaming`.

In both settings node `10.10.1.1` runs the coordinator; log in there and run everything
from that node.

| Experiment | Figures | Claim | Setting | Run time | Section |
|---|---|---|---|---|---|
| Reconfiguration vs. baselines, 6 queries × 4 protocols | 6, 8 | #1 | AWS | 4–5 h | [5.2](#section-52--figures-6-and-8) |
| Scale-out at high parallelism (16 → 32 tasks) | 9 | #2 | CloudLab | 10 min | [5.3.1](#section-531--figure-9-high-parallelism-optional) |
| Repeated / concurrent reconfiguration, skew rebalancing, task migration | 10–13 | #2 | AWS | ~2 h | [5.3](#section-53--figures-1013-reconfiguration-scenarios-optional) |
| State size, key lookup overhead, locality and skew | 14–16 | #3 | CloudLab | 2 h 20 min | [5.4](#section-54--figures-1416-microbenchmarks-optional) |

**Section 5.2 is the main result.** Start there; the other three sections are optional and
can be run in any order.

Because the artifact needs multi-node clusters with pre-generated warm-up state, we provide
ready-to-use clusters for both settings. If you use them, skip [Setup](#setup) — everything
below is already installed and the warm-up data is already generated — and go straight to
[Experiments](#experiments).

---

## Setup

Skip this section if you are using the clusters we provide.

### 1. Requirements

On every node:

| Dependency | Version | Notes |
|---|---|---|
| Go | 1.25.6+ | builds the binaries; needs a C toolchain (`gcc`) for the Kafka client |
| Java | 11 | required by Kafka (KRaft mode) |
| Python | 3.9+ | the experiment harness |
| Python packages | `psutil`, `kafka-python` | harness; on the coordinator also `matplotlib` and `numpy` for the figures |
| Apache Kafka | 3.9.0 | downloaded automatically by the deploy script |

The coordinator node needs passwordless SSH access to every other node. SSH is used only
for benchmarking and orchestration, not by the system at runtime.

### 2. Download

```bash
git clone git@github.com:CASP-Systems-BU/koala.git ~/disaggregated-streaming
cd ~/disaggregated-streaming
```

Clone into `~/ssd/disaggregated-streaming` instead if you are reproducing Sections 5.2 or
5.3 on AWS — the suite configs for those sections expect that path (`WorkDir` in the suite
JSON).

Some experiments in Sections 5.2 and 5.3 run on other branches (`drrs`, `koala_q3`,
`drrs_q3`, `skew_mitigation`, `task_migration`). The suite runner checks these out for you
and restores the original branch when it finishes; fetch them once up front:

```bash
git fetch --all
```

### 3. Install and build

Install the OS and Python dependencies on all nodes listed as producers in a config, then
build the binaries on the coordinator node:

```bash
cd ~/disaggregated-streaming/scripts
python3 prepareNode.py nexmarkJson/query1.json     # installs python3-pip, openjdk-11, psutil, kafka-python

cd ~/disaggregated-streaming
make                                               # builds ./bin/{coordinator,worker,client,*Producer,remotePebble}
```

`make` compiles the coordinator, the workers, the client, the three Kafka producers, and
the remote Pebble server into `./bin`. You only build on the coordinator node — the
harness ships `bin/`, `config.yaml`, and `scripts/` to the other nodes.

Refer to [cluster setup](https://github.com/CASP-Systems-BU/koala/wiki/Experiment-Environment-Setup)


---

## Hello world

A single-node run of the whole system — Kafka broker, producer, coordinator, three
workers, and a scale-out of the mapper from 1 to 2 tasks 30 seconds in. Takes 5 minutes:

```bash
cd ~/disaggregated-streaming/scripts
python3 runExperiment.py nexmarkJson/query1.json hello_world
```

`runExperiment.py <configFilePath> <resultKeyword>` performs the whole lifecycle: update
configs → sync to all nodes → start Kafka → deploy the query → start producers → monitor →
trigger the reconfiguration → collect results → shut everything down. Ctrl-C at any point
shuts the cluster down cleanly.

Results land in `scripts/results/nexmark_query1_hello_world/`, containing the metrics
database the figure scripts read (throughput, per-batch latency, state migrated, Kafka
lag). If the run reports no failures and that folder exists, the installation is working.

---

## Experiments

All experiments are long-running. Run them under `screen` or `tmux` so a dropped SSH
connection does not kill the run. Every command below is run from the coordinator node
(`10.10.1.1`).

Two scripts drive everything:

```bash
python3 runExperimentSuite.py <suite.json>     # run a section's experiments back-to-back
    [--only name1,name2]                       #   run a subset
    [--dry-run]                                #   validate and print the plan
    [--verbose]                                #   echo each experiment's output

python3 runAllFigures.py                       # regenerate that section's figures
    [--only figure14] [--list] [--verbose]
```

The suite runner retries a failed experiment (`MaxAttempts`), deletes the partial results
of a failed attempt so the retry writes to the expected folder, checks out the branch each
experiment needs, and restores your original branch at the end. Re-running a suite is
always safe. It prints a summary at the end and writes it to
`results/suiteLogs_<timestamp>/summary.json`, with per-experiment logs alongside it.

### Section 5.2 — Figures 6 and 8

*(Human time: 10 minutes, run time: 4–5 hours. AWS. **This is the main result.**)*

Supports claim #1: Koala reconfigures without downtime, and its per-batch latency does not
spike during reconfiguration.

| Claim in the paper | Figure | Supported by |
|---|---|---|
| "Koala has no downtime after the scale-out, unlike S&R, and consumes the backlog much faster than the other baselines." | 6 | `Figure6/*.pdf` |
| "Koala's per-batch latency is lower than Remote's and does not spike during reconfiguration." | 8 | `Figure8/*_latency.pdf` |

The cluster is ready to use: all packages are installed and the warm-up data for every
query is already generated, so no warm-up phase is needed.

**1. Run the 24 experiments**

```bash
cd ~/ssd/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.2/fullSuite.json
```

Six queries (`azure`, `borg`, `taxi`, `twitch`, `nexmark_query3`,
`nexmark_query6_modified`) × four protocols (Koala (`lazy`), `SAR`, `Remote`, `DRRS`),
producing result folders `<query>_<lazy|SAR|Remote|DRRS>`. Check that all 24 report
`SUCCEEDED` in the final summary; per-experiment logs are in
`scripts/results/suiteLogs_<timestamp>/`.

The 24 entries span four branches — `main`, `koala_q3` and `drrs_q3` (Nexmark Q3's target
operator is a join, so it needs its own implementations), and `drrs`. The suite groups
them by branch so the run performs the minimum number of checkouts, and restores your
original branch at the end.

If some experiments fail, rerun them with the config the suite writes for you:

```bash
cd ~/ssd/disaggregated-streaming/scripts
python3 runExperimentSuite.py results/suiteLogs_<timestamp>/rerunFailed.json
```

Repeat until all 24 report `SUCCEEDED`, then continue with step 2.

**2. Generate the plots**

```bash
cd ~/ssd/disaggregated-streaming/scripts/nsdi27/evaluation/Section5.2
python3 runAllFigures.py
```

Exits non-zero if any figure fails. Output:

| Files | Paper figure |
|---|---|
| `Figure6/{azure,borg,taxi,twitch,q3,q6mod}.pdf` | Figure 6 |
| `Figure8/{azure,borg,taxi,twitch,q3,q6mod}_latency.pdf` | Figure 8 |

**3. Compare with the paper**

| Figure | What you should see |
|---|---|
| 6 | Koala's throughput does not drop to zero at the scale-out, while S&R stops entirely; Koala drains the backlog that builds up during reconfiguration faster than the other baselines. |
| 8 | Koala's per-batch latency stays below Remote's throughout and shows no spike at the reconfiguration point. |

### Section 5.3.1 — Figure 9, high parallelism (optional)

*(Human time: 15 minutes, run time: 10 minutes. CloudLab.)*

Supports claim #2: the mechanism still holds when the target operator runs at high
parallelism. The experiment runs Nexmark Q6\* (`nexmark_query6_modified`) under Koala for
10 minutes at a stable input rate; after three minutes the target operator scales out from
16 to 32 tasks.

| Claim in the paper | Figure | Supported by |
|---|---|---|
| "Koala maintains stable throughput during scale-out at high parallelism, with minimal disruption to the Kafka queue." | 9 | `Figure9.pdf` |

| Step | Human time | Run time |
|---|---|---|
| Check cluster access | 3 min | 1 min |
| Run the experiment | 1 min | 10 min |
| Generate the figure | 1 min | 1 min |
| Compare with the paper | 10 min | — |

**1. Cluster access**

The cluster has 20 `c6620` nodes on CloudLab Utah.

| Node | Role |
|---|---|
| `10.10.1.1` | Kafka broker, 2 producers, coordinator. Log in here and run everything from this node. |
| `10.10.1.2`, `10.10.1.3`, `10.10.1.4` | Kafka broker and 2 producers each |
| `10.10.1.5` – `10.10.1.20` | Workers. 16 worker processes initially: 16 sources, 16 stateful mapper tasks, and 1 sink; scale-out adds 16 more mapper tasks (32 total). |

```bash
ssh <user>@10.10.1.1
sudo -i
cd ~/disaggregated-streaming
git status -sb                     # should report main
for ip in 10.10.1.2 10.10.1.3 10.10.1.4 10.10.1.9 10.10.1.10 10.10.1.11 10.10.1.12 10.10.1.13 10.10.1.14 10.10.1.15 10.10.1.16 10.10.1.17 10.10.1.18 10.10.1.19 10.10.1.20 10.10.1.21 10.10.1.22 10.10.1.23 10.10.1.24; do
  ssh -o BatchMode=yes $ip 'hostname' || echo "UNREACHABLE $ip"
done
ssh 10.10.1.9 'du -sh ~/disaggregated-streaming/pebble_warmup_data/nexmark_q6_mod/128_consistent_80GB_16'
```

You should see all 16 worker nodes respond, and the 80 GB warm-up snapshot for
parallelism-16 on the worker.

**2. Run the experiment**

```bash
cd ~/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/Cloudlab/fullSuite.json
```

About 10 minutes. The script prints a summary:

```
============================================================
                      Suite summary
============================================================
  SUCCEEDED                          598s  parallelism_16_to_32  -> lazy_2M_200k
  Logs: /home/<user>/disaggregated-streaming/scripts/results/suiteLogs_<timestamp>
  Results: /home/<user>/disaggregated-streaming/scripts/results
```

The experiment should say `SUCCEEDED`. The full output is in
`results/suiteLogs_<timestamp>/<NN>_parallelism_16_to_32_attemptN.log`. A failed
experiment is retried twice before the suite gives up, and re-running the suite is safe.

**3. Generate the figure**

```bash
cd ~/disaggregated-streaming/scripts/nsdi27/evaluation/Section5.3/Cloudlab
python3 runAllFigures.py
```

**4. Compare with the paper**

Absolute values depend on the machines and on how Kafka and the producer happen to behave
during a run, so the claims are about trends and stability rather than exact numbers. The
reference values below come from the run used for the paper.

| Figure | What you should see | Reference values |
|---|---|---|
| 9a (Tput) | Throughput is flat before and after scale-out at t=180s, with no dip and no disruption to steady-state input rate | ~800,000 records/s aggregated across 16 sources before and after scale-out; minimal variance |
| 9b (Kafka lag) | Kafka queue lag stays low throughout and does not spike during scale-out | lag under 30 seconds before and after; minimal disruption at t=180s |

**Changing the experiment.** To test other parallelism levels, add a config under
`nexmarkJson/query6/highParallelism/` and an entry in
`nsdi27/evaluation/Section5.3/Cloudlab/fullSuite.json`.

| Field | Effect |
|---|---|
| `StatefulMapperParallelism` | Initial parallelism of the target operator |
| `TargetParrallelism` | The scale-out (or scale-in) target parallelism. `WorkerIPs` must have enough entries for the resulting task count. |

### Section 5.3 — Figures 10–13, reconfiguration scenarios (optional)

*(Human time: 10 minutes, run time: ~2 hours. AWS.)*

Supports claim #2: the same mechanism covers reconfiguration types beyond a single
scale-out — repeated reconfigurations, concurrent reconfiguration of several operators,
skew-driven rebalancing, and scale-in with task migration.

| Claim in the paper | Figure | Supported by |
|---|---|---|
| Koala sustains repeated reconfigurations, with state migration tapering off after each one | 10 | `Figure10.pdf` |
| Rebalancing moves hot keys off the overloaded task and evens out per-task throughput | 11 | `Figure11.pdf` |
| Scale-in with task migration: fetch-on-demand and progressive migration trade migration speed against disruption | 12 | `Figure12.pdf` |
| Several operators can be reconfigured concurrently without disrupting throughput or the Kafka queue | 13 | `Figure13.pdf` |

**1. Run the experiments**

```bash
cd ~/ssd/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/AWS/fullSuite.json
```

Six experiments, spread over three branches — the suite checks each one out for you and
restores your original branch at the end:

| Suite entry | Branch | Scenario | Result folder |
|---|---|---|---|
| `multiReconfig` | `main` | Four reconfigurations in one run (Q6\*) | `nexmark_query6_modified_multi_reconfig` |
| `concurrent_multi_operators_reconfig` | `main` | Two operators reconfigured concurrently (twitch) | `twitch_concurrent_multi_reconfig` |
| `skew_mitigation` | `skew_mitigation` | Rebalancing under key skew (taxi) | `taxi_skew_rebalance` |
| `task_migration_fetch_on_demand` | `task_migration` | Scale-in, state fetched on demand | `nexmark_query6_modified_task_migration_fetch_on_demand` |
| `task_migration_progressive_default` | `task_migration` | Scale-in, progressive migration (default chunk) | `nexmark_query6_modified_task_migration_progressive_default` |
| `task_migration_progressive_large` | `task_migration` | Scale-in, progressive migration (large chunk) | `nexmark_query6_modified_task_migration_progressive_large` |

All six should report `SUCCEEDED`. To rerun a subset:

```bash
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/AWS/fullSuite.json --only skew_mitigation
```

**2. Generate the figures**

```bash
cd ~/ssd/disaggregated-streaming/scripts/nsdi27/evaluation/Section5.3/AWS
python3 runAllFigures.py
```

| File | Paper figure | Reads |
|---|---|---|
| `Figure10.pdf` | Figure 10 — throughput and state transferred across four reconfigurations | `multi_reconfig` |
| `Figure11.pdf` | Figure 11 — per-task throughput and Kafka lag around the rebalance | `taxi_skew_rebalance` |
| `Figure12.pdf` | Figure 12 — throughput and state migration for the three scale-in strategies | the three `task_migration_*` folders |
| `Figure13.pdf` | Figure 13 — throughput and Kafka lag under concurrent reconfiguration | `twitch_concurrent_multi_reconfig` |

**3. Compare with the paper**

As above, the claims are about trends rather than exact numbers.

| Figure | What you should see |
|---|---|
| 10 | Throughput holds across all four reconfiguration points (t≈183, 363, 543, 723s); the volume of state transferred spikes at each one and then decays to zero as the working set is fetched. |
| 11 | Before the rebalance, one task of the target operator carries most of the throughput; afterwards, per-task throughput evens out and Kafka lag returns to its steady-state level. |
| 12 | Fetch-on-demand keeps throughput steady and spreads migration over a longer window; progressive migration moves state faster, and the larger chunk size shortens the migration at the cost of a larger disturbance to throughput. |
| 13 | Reconfiguring two operators at the same time does not stall the pipeline: aggregate throughput stays flat and the Kafka queue does not build up. |

### Section 5.4 — Figures 14–16, microbenchmarks (optional)

*(Human time: 20 minutes, run time: 2 h 20 min. CloudLab.)*

Supports claim #3: cost is governed by the active working set, not by total state size.
Figure 14 varies total state size, Figure 15 measures key lookup overhead, and Figure 16
varies key locality and skew.

All twelve experiments run Nexmark Q6\* (`nexmark_query6_modified`) under Koala for 10
minutes at a stable input rate; after three minutes the target operator scales out from 4
to 8 tasks.

| Claim in the paper | Figure | Supported by |
|---|---|---|
| "Koala demonstrates stable behavior despite increasing state size. Across all runs, it migrates around the same amount of state, as it exploits the temporal key locality of the workload, relocating the active working set only." | 14 | `Figure14.pdf` |
| "Key lookup takes less than 40µs per input batch across all runs, accounting for under 3% of per-batch latency." | 15a | `Figure15.pdf` |
| "While KLT grows with the key space, it remains compact, reaching at most 60 MB for 40M keys and 80 GB of total state." | 15b | `Figure15.pdf` |
| "Migration duration depends on the size of the active key space, as more keys from the old tasks are accessed after reconfiguration, resulting in more state being migrated on demand." | 16a | `Figure16.pdf` |
| "Higher skew leads to smoother reconfiguration, because (i) repeated accesses to hot keys incur no additional cost after initial fetch, and (ii) batch-based state access deduplicates keys before issuing the migration request." | 16b | `Figure16.pdf` |

| Step | Human time | Run time |
|---|---|---|
| Check cluster access | 3 min | 1 min |
| Run the experiments | 1 min | 2 h 20 min |
| Generate the figures | 1 min | 1 min |
| Compare with the paper | 15 min | — |

**1. Cluster access**

The cluster has eight `c6620` nodes on CloudLab Utah.

| Node | Role |
|---|---|
| `10.10.1.1` | Kafka broker, 2 producers, coordinator. Log in here and run everything from this node. |
| `10.10.1.2`, `10.10.1.3`, `10.10.1.4` | Kafka broker and 2 producers each |
| `10.10.1.5` – `10.10.1.8` | Workers. 17 worker processes in total: 8 sources, 4 mapper tasks plus 4 more for the scale-out, and 1 sink. |

```bash
ssh <user>@10.10.1.1
sudo -i
cd ~/disaggregated-streaming
git status -sb                     # should report main
for ip in 10.10.1.2 10.10.1.3 10.10.1.4 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.12; do
  ssh -o BatchMode=yes $ip 'hostname' || echo "UNREACHABLE $ip"
done
ssh 10.10.1.9 'du -sh ~/disaggregated-streaming/pebble_warmup_data/nexmark_q6_mod/*'
```

You should see all eight nodes respond, and four warm-up snapshots (10, 20, 40 and 80 GB)
on the worker.

**2. Run the experiments**

```bash
cd ~/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.4/fullSuite.json
```

About 2 hours and 20 minutes for all twelve experiments.

| Suite entries | What varies | Result folders (prefixed `nexmark_query6_modified_`) |
|---|---|---|
| `state_10gb` … `state_80gb` | total state: 10, 20, 40, 80 GB | `lazy_5MKeys`, `lazy_10MKeys`, `lazy_20MKeys`, `lazy_40MKeys` |
| `locality_500k` … `locality_4m` | active key space: 500K, 1M, 2M, 4M | `lazy_500k_50k_25`, `lazy_1M_100k_25`, `lazy_2M_200k_25`, `lazy_4M_400k_25` |
| `skew_0` … `skew_75` | hot key ratio: 0, 25, 50, 75% | `lazy_2M_200k_0`, `lazy_2M_200k`, `lazy_2M_200k_50`, `lazy_2M_200k_75` |

The script shows intermediate results:

```
============================================================
                      Suite summary
============================================================
  SUCCEEDED                          672s  state_10gb  -> lazy_5MKeys
  SUCCEEDED                          681s  state_20gb  -> lazy_10MKeys
  ...
  Logs: /home/<user>/disaggregated-streaming/scripts/results/suiteLogs_<timestamp>
  Results: /home/<user>/disaggregated-streaming/scripts/results
```

All twelve should say `SUCCEEDED`. To repeat only the entries that failed, pass their names
to `--only`.

**3. Generate the figures**

```bash
cd ~/disaggregated-streaming/scripts/nsdi27/evaluation/Section5.4
python3 runAllFigures.py
```

This runs `figure14.py`, `figure15.py` and `figure16.py`, which:

1. Read the result folders from the experiments.
2. For Figure 15, extract worker IPs from the experiment config files and measure key
   lookup table sizes from `~/disaggregated-streaming/pebbleLookUpTable/` on each worker.
   To skip the SSH measurement, pass the sizes directly:
   `python3 figure15.py --klt-sizes 17,34,67,134`
3. Write one PDF per figure next to the scripts:

| File | Paper figure |
|---|---|
| `Figure14.pdf` | Figure 14 |
| `Figure15.pdf` | Figure 15 |
| `Figure16.pdf` | Figure 16 (both rows) |

**4. Compare with the paper**

Absolute values depend on the machines and on how Kafka and Pebble happen to behave during
a run, so the claims are about trends and orders of magnitude rather than exact numbers.
The reference values below come from the run used for the paper.

| Figure | What you should see | Reference values |
|---|---|---|
| 14a (Tput) | Throughput is flat across the scale-out at t=180s in all four panels, with no dip and no downtime | roughly 8,000 records/s aggregated at the source, before and after |
| 14b (State) | At scale-out, migrated state spikes and then tapers to zero. The total state migrated stays roughly constant even as total state size grows from 10 to 80 GB. | 1.41, 1.44, 1.43, 1.43 GB |
| 15a (Latency) | Key lookup time is flat and two orders of magnitude below per-batch processing time | key lookup 37–43 µs against per-batch 1.33–1.42 ms, under 3% |
| 15b (KLT size) | The key lookup table grows with the key space but stays small | 17, 34, 67, 134 MB for 10–80 GB state |
| 16a (Locality) | Both the volume migrated and the time it takes grow with the active key space, close to linearly | 468, 936, 1824, 3696 MB for 500K, 1M, 2M and 4M active keys |
| 16b (Skew) | More skew means less state migrated and a shorter tail | 1899, 1824, 1806, 1597 MB for 0, 25, 50 and 75% hot keys |

**Changing the experiments.** Adding a config to
`nsdi27/evaluation/Section5.4/fullSuite.json` as another `Experiments` entry lets it run
the same way as the rest.

| Field | Effect |
|---|---|
| `NumActivePeople` | The active key space (the x-axis of Figure 16a). Keep `HotSellerRange` at about 10% of it to hold skew constant. |
| `HotSellerRatio` | The fraction of records that hit the hot key set, between 0 and 1 (the x-axis of Figure 16b). |
| `TargetParrallelism` | The scale-out (or scale-in) factor. `WorkerIPs` must have enough entries for the resulting task count. |

---

## Appendix A: rebuilding the warm-up state

The clusters we provide already have the warm-up state generated, so this is only needed if
you set up your own cluster. Each state size takes about 30 minutes to rebuild, and warm-up
runs have to be started individually rather than through a suite.

A warm-up run generates a bounded, sequential key stream, one new key per event, so the
resulting key space is exactly `NumEvents` times the 8 producers. At the end it moves each
worker's `data/pebble` into the snapshot folder and writes out the key lookup table.

| State size | Config (under `scripts/`) | Key space |
|---|---|---|
| 10 GB | `nexmarkJson/query6/stateSize/query6ModWarmup10GB.json` | 5M keys |
| 20 GB | `nexmarkJson/query6/stateSize/query6ModWarmup20GB.json` | 10M keys |
| 40 GB | `nexmarkJson/query6/stateSize/query6ModWarmup40GB.json` | 20M keys |
| 80 GB | `nexmarkJson/query6/stateSize/query6ModWarmup80GB.json` | 40M keys |

```bash
cd ~/disaggregated-streaming/scripts
python3 runExperiment.py nexmarkJson/query6/stateSize/query6ModWarmup40GB.json warmup_40GB
```

`OutputEventNumber` in a measurement config is the point in the key sequence where the
generator resumes after warm-up, so if you change a warm-up's `NumEvents` you need to
scale it by the same factor (46×): 625K maps to 28,750,000, 1.25M to 57,500,000.

## Appendix B: writing your own query

A query is a Go dataflow built from the operators in [api/dataflow/](api/dataflow/). The
smallest complete examples are in [query/examples/](query/examples/) — `counter.go` (a
stateful count), `filter.go`, `mapper.go`, `tumblingWindow.go` — and the queries used in
the paper are in [query/](query/) (`azure`, `borg`, `taxi`, `twitch`, `nexmark`).

To run one, register the query, rebuild with `make`, and point a JSON config at it via
`QueryName`. The config controls the cluster layout, the runtime, the parallelism of each
operator, when reconfigurations fire, and which protocol handles them:

```json
{
    "Reconfigurations": [{
        "TriggerTimeSeconds": 30,
        "Type": "scaleup",
        "TargetOperator": "mapper",
        "TargetParrallelism": 2
    }],
    "ReconfigProtocol": "lazy",
    "LazyProtocolVersion": "basic"
}
```

[scripts/README.md](scripts/README.md) documents every config field, the Kafka cluster and
producer scripts, and custom task placement.

## Troubleshooting

| Symptom | What to do |
|---|---|
| An experiment reports `FAILED` in the suite summary | It has already been retried twice. Read `results/suiteLogs_<timestamp>/<NN>_<name>_attemptN.log`, then rerun just that entry with `--only <name>` (Section 5.2 also writes a ready-made `rerunFailed.json`). |
| A run leaves processes behind after Ctrl-C | Ctrl-C shuts the cluster down cleanly on all nodes; if a node was unreachable, `python3 stopProducers.py <config.json>` and `python3 stopKafkaCluster.py <config.json>` clean up the rest. |
| A figure script fails with a missing result folder | The experiment that produces it did not complete — check the suite summary for the matching result keyword in the tables above. |
| `figure15.py` hangs or fails on SSH | Pass the key lookup table sizes directly: `python3 figure15.py --klt-sizes 17,34,67,134`. |
| A node is missing binaries or scripts | `python3 syncRepo.py <config.json>` from the coordinator re-broadcasts `bin/`, `config.yaml`, and `scripts/`. |
| `git status -sb` reports an unexpected branch | A suite that was interrupted may not have restored it. `git checkout main` before rerunning; the suite checks out what each experiment needs. |
