# Koala - Artifact Evaluation Guide (NSDI 2027)

Welcome to the artifact evaluation guide for Koala, a non-disruptive reconfiguration protocol for stateful dataflow systems. Koala supports a range of reconfiguration scenarios, including scale-out, scale-in, rebalancing, and task migration. This repository contains (i) a new distributed dataflow runtime that serves as the underlying system for evaluating workload reconfigurations, (ii) an implementation of the Koala protocol, and (iii) implementations of baseline reconfiguration protocols for comparison.

**We target all three badges {Available, Functional, Reproduced}:**

- Available: we publish Koala on [Github](https://github.com/CASP-Systems-BU/koala).
- Functional: we describe all artifact components and provide instructions for running a minimal working example.
- Reproduced: we provide instructions for reproducing the key results from the evaluation section of the paper. Our main results—Figures 6, 8, 10, 11, 12, and 13—are reproducible. Figures 9, 14, 15, and 16 are also reproducible but are optional.

---

## Table of contents

- [Available Badge](#available-badge)
- [Functional Badge](#functional-badge)
- [Reproduced Badge](#reproduced-badge)
- [Hello world](#hello-world)
- [Experiments](#experiments)
  - [Section 5.2 — Figures 6 and 8 (main result)](#section-52--figures-6-and-8)
  - [Section 5.3 — Figures 10–13, reconfiguration scenarios](#section-53--figures-1013-reconfiguration-scenarios)
    - [Section 5.3.1 — Figure 9, high parallelism (optional)](#section-531--figure-9-high-parallelism-optional)
  - [Section 5.4 — Figures 14–16, microbenchmarks (optional)](#section-54--figures-1416-microbenchmarks-optional)
- [Appendix A: rebuilding the warm-up state](#appendix-a-rebuilding-the-warm-up-state)
- [Appendix B: writing your own query](#appendix-b-writing-your-own-query)

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
3. Lazy state access migrates only the active working set, so
   cost is governed by key locality and skew rather than total state size, and the key
   lookup table stays small (Section 5.4, Figures 14–16).

We compare against three baselines, all implemented in this repository and run through the
same harness: **S&R** (stop-and-restart, the standard approach), **Remote** (state served
from disaggregated storage without migration), and **DRRS**.

All experiments run on Ubuntu Linux (22.04) in two settings:

- **AWS** — used for Sections 5.2 and 5.3 (Figures 6, 8, 10–13). Working directory on the
  nodes is `~/ssd/disaggregated-streaming`.
- **CloudLab Utah** (`c6620` nodes) — used for optional Sections 5.3.1 and 5.4 (Figures 9, 14–16).
  Working directory on the nodes is `~/disaggregated-streaming`.

In both settings node `10.10.1.1` runs the coordinator; log in there and run everything
from that node.

| Experiment | Figures | Claim | Setting | Run time | Section |
|---|---|---|---|---|---|
| Reconfiguration vs. baselines, 6 queries × 4 protocols | 6, 8 | #1 | AWS | 4–5 h | [5.2](#section-52--figures-6-and-8) |
| Repeated / concurrent reconfiguration, skew rebalancing, task migration | 10–13 | #2 | AWS | ~2 h | [5.3](#section-53--figures-1013-reconfiguration-scenarios) |
| Scale-out at high parallelism (16 → 32 tasks) | 9 | #2 | CloudLab | 10 min | [5.3.1](#section-531--figure-9-high-parallelism-optional) |
| State size, key lookup overhead, locality and skew | 14–16 | #3 | CloudLab | 2 h 20 min | [5.4](#section-54--figures-1416-microbenchmarks-optional) |

Because the artifact needs multi-node clusters with pre-generated warm-up state, we provide
ready-to-use clusters for both settings.

Refer to [cluster setup](https://github.com/CASP-Systems-BU/koala/wiki/Experiment-Environment-Setup) to setup your own cluster.


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

python3 runAllFigures.py                       # regenerate that section's figures
```

It prints a summary at the end and writes it to
`results/suiteLogs_<timestamp>/summary.json`, with per-experiment logs alongside it.

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

If some experiments fail, rerun them with the config the suite writes for you:

```bash
cd ~/ssd/disaggregated-streaming/scripts
python3 runExperimentSuite.py results/suiteLogs_<timestamp>/rerunFailed.json
```

### Section 5.2 — Figures 6 and 8

*(Human time: 10 minutes, run time: 4–5 hours. AWS. **This is the main result.**)*

Supports claim #1: Koala reconfigures without downtime, and its per-batch processing latency does not
spike during reconfiguration.

| Claim in the paper | Figure | Supported by |
|---|---|---|
| "Koala has no downtime after the scale-out, unlike S&R, and consumes the backlog much faster than the other baselines." | 6 | `Figure6/*.pdf` |
| "Koala's per-batch latency is lower than Remote's and does not spike during reconfiguration." | 8 | `Figure8/*_latency.pdf` |

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
| 6 | Koala's throughput does not drop to zero after scaling out, while S&R stops entirely; Koala drains the backlog that builds up during reconfiguration faster than the other baselines. |
| 8 | Koala's per-batch latency stays below Remote's throughout and shows no spike at the reconfiguration point. |


### Section 5.3 — Figures 10–13, reconfiguration scenarios

*(Human time: 10 minutes, run time: ~2 hours. AWS.)*

Supports claim #2: the same mechanism covers reconfiguration types beyond a single
scale-out — repeated reconfigurations, concurrent reconfiguration of several operators,
skew-driven rebalancing, and scale-in with task migration.

| Claim in the paper | Figure | Supported by |
|---|---|---|
| Koala maintains nondisruptive processing both when scaling-out and when scalingin, demonstrating robustness under repeated reconfigurations| 10 | `Figure10.pdf` |
| After rebalance, Koala quickly resolves the bottleneck, converging to a stable and balanced throughput | 11 | `Figure11.pdf` |
| Fetch-on-demand and progressive-default sustain the input rate without disruption, and the latter completes the migration in the background | 12 | `Figure12.pdf` |
| Koala can also support cases where multiple target operators need to be reconfigured at once.  | 13 | `Figure13.pdf` |

**1. Run the experiments**

```bash
cd ~/ssd/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/AWS/fullSuite.json
```

**2. Generate the figures**

```bash
cd ~/ssd/disaggregated-streaming/scripts/nsdi27/evaluation/Section5.3/AWS
python3 runAllFigures.py
```

**3. Compare with the paper**

As above, the claims are about trends rather than exact numbers.

| Figure | What you should see |
|---|---|
| 10 | Throughput holds across all four reconfiguration points (t≈180, 360, 540, 720s); the volume of state transferred spikes at each one and then decays to zero as the working set is fetched. |
| 11 | Before the rebalance, one task of the target operator carries most of the throughput; afterwards, per-task throughput evens out and Kafka lag returns to its steady-state level. |
| 12 | Fetch-on-demand keeps throughput steady and spreads migration over a longer window; progressive migration moves state faster, and the larger chunk size shortens the migration at the cost of a larger disturbance to throughput. |
| 13 | Reconfiguring two operators at the same time does not stall the pipeline: aggregate throughput stays flat and the Kafka queue does not build up. |


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
| Run the experiment | 1 min | 10 min |
| Generate the figure | 1 min | 1 min |
| Compare with the paper | 2 min | — |

**1. Run the experiment**

We run the experiment on CloudLab Utah `c6620` with 20 nodes.

```bash
cd ~/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/Cloudlab/fullSuite.json
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

| Figure | Claim |
|---|---|
| 9a (Tput) | Throughput is flat before and after scale-out at t=180s, with no dip and no disruption to steady-state input rate |
| 9b (Kafka lag) | Kafka queue lag stays low throughout and does not spike during scale-out | 


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
| Run the experiments | 1 min | 2 h 20 min |
| Generate the figures | 1 min | 1 min |
| Compare with the paper | 15 min | — |

**1. Run the experiments**

```bash
cd ~/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.4/fullSuite.json
```

About 2 hours and 20 minutes for all twelve experiments.

**3. Generate the figures**

```bash
cd ~/disaggregated-streaming/scripts/nsdi27/evaluation/Section5.4
python3 runAllFigures.py
```

**4. Compare with the paper**

Absolute values depend on the machines and on how Kafka and Pebble happen to behave during
a run, so the claims are about trends and orders of magnitude rather than exact numbers.

| Figure | Claim |
|---|---|
| 14a (Tput) | Throughput is flat across the scale-out at t=180s in all four panels, with no dip and no downtime |
| 14b (State) | At scale-out, migrated state spikes and then tapers to zero. The total state migrated stays roughly constant even as total state size grows from 10 to 80 GB. |
| 15a (Latency) | Key lookup time is flat and two orders of magnitude below per-batch processing time |
| 15b (KLT size) | The key lookup table grows with the key space but stays small |
| 16a (Locality) | Both the volume migrated and the time it takes grow with the active key space, close to linearly |
| 16b (Skew) | More skew means less state migrated and a shorter tail |
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
