# Koala - Artifact Evaluation Guide (NSDI 2027)

Welcome to the artifact evaluation guide for Koala, a non-disruptive reconfiguration protocol for stateful dataflow systems. Koala supports a range of reconfiguration scenarios, including scale-out, scale-in, rebalancing, and task migration. This repository contains (i) a new distributed dataflow runtime that serves as the underlying system for evaluating workload reconfigurations, (ii) an implementation of the Koala protocol, and (iii) implementations of baseline reconfiguration protocols for comparison.

**We target all three badges (Available, Functional, Reproduced):**

- [Available](#available-badge): we publish Koala on [GitHub](https://github.com/CASP-Systems-BU/koala).
- [Functional](#functional-badge): we describe all artifact components and provide instructions for running a minimal working example.
- [Reproduced](#reproduced-badge): we provide instructions for reproducing the key results from the evaluation section of the paper. Our main results—Figures 6, 8, 10, 11, 12, and 13—are reproducible. Figures 9, 14, 15, and 16 are also reproducible but are optional.

> [!IMPORTANT]
> All experiments run on AWS `c5d.4xlarge` instances and CloudLab `c6620` machines. To simplify the evaluation process, we provide reviewers with ready-to-use environments on both AWS and CloudLab; access credentials (SSH keys and IP addresses) will be shared via HotCRP.

## Overview

We summarize the experiments outlined in this document and the claims they support. The experiments in the [Reproduced Badge](#reproduced-badge) section are organized by evaluation sections in the paper (sec 5.2, sec 5.3, sec 5.4).

### 1. Primary claim: Sec 5.2

*(Human time: 10 minutes, run time: 4–5 hours)*

> **Primary claim**: Koala effectively eliminates the reconfiguration disruption (e.g., throughput drop, backlog accumulation, latency spike) as compared to the baselines, while sustaining low processing latency during normal operation.

Sec 5.2 scales out six large-state queries under accumulated state and backlog, supporting the primary claim, as shown in Figures 6 and 8.

### 2. Secondary claim: Sec 5.3

*(Human time: 10 minutes, run time: ~2 hours)*

> **Secondary claim**: Koala can handle a variety of reconfiguration scenarios, including repeated reconfigurations, concurrent reconfigurations, skew-driven rebalancing, and task migration.

Sec 5.3 evaluates varying reconfiguration scenarios, demonstrating the applicability and flexibility of the Koala protocol, as shown in Figures 10–13.

### 3. Optional: Sec 5.4

*(Human time: 20 minutes, run time: 2 h 20 min)*

Sec 5.4 runs a set of microbenchmarks to demonstrate the efficiency of Koala's lazy state access mechanism, including the impact of total state size, key lookup overhead, and key locality/skew. This section is **optional** and not required for the main claims.

## Table of contents

- [Getting Started](#getting-started)
- [Available Badge](#available-badge)
- [Functional Badge](#functional-badge)
- [Reproduced Badge](#reproduced-badge)
  - [Section 5.2 - Figures 6 and 8](#section-52---figures-6-and-8)
  - [Section 5.3 - Figures 10–13](#section-53---figures-1013)
    <!-- - [[Optional] Section 5.3.1 - Figure 9](#optional-section-531---figure-9) -->
  - [[Optional] Section 5.4 - Figures 14–16](#optional-section-54---figures-1416)
- [Appendix A: rebuild the warm-up state](#appendix-a-rebuild-the-warm-up-state)
- [Appendix B: write your own query](#appendix-b-write-your-own-query)


## Getting Started

*(Human time: 1 minute, run time: 2 minutes)*

We will provide the SSH credentials and IP addresses over HotCRP. Please SSH into the machine (master node of the cluster) we provided. **We have pre-installed all dependencies and set up the environment for you**. The Koala repository is already cloned in directory `~/ssd/koala` on AWS and `~/koala` on CloudLab.

*(You can refer to [cluster setup](https://github.com/CASP-Systems-BU/koala/wiki/Experiment-Environment-Setup) for cluster setup instructions)*

First start a tmux session (a helpful tmux reference is available [here](https://tmuxcheatsheet.com/)).
```bash
tmux
```
*(To detach from a running tmux session, press `Ctrl-b D`)*

The following commands start a single-node run of the entire system, including the Kafka broker, producer, coordinator, and three workers. It deploys a simple streaming query (src -> mapper -> sink) and scales out the mapper operator from 1 to 2 tasks after 30 seconds. The entire process takes approximately 3 minutes.

```bash
cd ~/ssd/koala/scripts
python3 runExperiment.py nexmarkJson/query1.json hello_world
```

Results land in `koala/scripts/results/nexmark_query1_hello_world/`, containing collected metrics in a database file `metricCollector.db`.

## Available Badge

The artifact is available in our GitHub repository,
[CASP-Systems-BU/koala](https://github.com/CASP-Systems-BU/koala), together with this
README (dependencies, getting-started guide, and per-figure reproduction steps).

## Functional Badge

Please see the [Getting Started](#getting-started) section to run the "hello world" example, a minimal working run of the system. Below, we summarize the artifact structure and the configuration of the reconfiguration protocols.

Artifact components to highlight:

| Component | What it provides |
|---|---|
| [api/dataflow/](api/dataflow/) | The operator interface and its implementations: source, sink, map, filter, flatmap, join, custom-window join, tumbling and sliding windows, and the stateful variants (`statefulMap`, `statefulFlatmap`, `statefulMap2State`). |
| [api/stateClient/](api/stateClient/), [state/](state/) | The state-access interface and the state backends behind it (local Pebble, remote Pebble, TiKV), plus the key lookup table used for lazy state migration. |
| [coordinator/](coordinator/) | Job deployment, task placement, and the reconfiguration protocols. |
| [worker/](worker/) | The task runtime: batch processing, data-plane and state-plane communication, and on-demand state migration. |
| [internal/](internal/) | Configuration, gRPC control plane, and shared internals. |
| [kafka/](kafka/), [cmd/](cmd/) | Kafka source/producer integration and the binaries (`coordinator`, `worker`, `client`, the three producers, `remotePebble`). |
| [query/](query/) | The queries used in the paper: `azure`, `borg`, `taxi`, `twitch`, and the `nexmark` suite, plus small [examples](query/examples/). |
| [scripts/](scripts/) | The experiment harness: cluster preparation, repo sync, Kafka cluster and producer lifecycle, single-experiment and suite runners, and the figure scripts. |

The reconfiguration protocols (Koala, S&R, and Remote) are configured in
each experiment's JSON config file. [The DRRS approach](https://ieeexplore.ieee.org/document/11113181/) is implemented in a separate branch (`drrs`) as it requires major modifications to the runtime. The following table summarizes the protocol configuration.

| Name in the paper | How it is configured in the JSON config file |
|---|---|
| Koala | `"ReconfigProtocol": "lazy"`, `"StateBackendType": "pebble"` |
| S&R (stop-and-restart) | `"ReconfigProtocol": "stop-and-restart"`, `"StateBackendType": "pebble"` |
| Remote | `"ReconfigProtocol": "stop-and-restart"`, `"StateBackendType": "remote-pebble"` — state stays in remote storage |
| DRRS | its own implementation, on the `drrs` branch |

## Reproduced Badge

### Overview of the experiments

| Experiment | Figures | Claim | Setting | Run time | Section |
|---|---|---|---|---|---|
| Koala vs. baselines on 6 queries | 6, 8 | #1 | AWS | 4–5 h | [5.2](#section-52---figures-6-and-8) |
| Koala applicability | 10–13 | #2 | AWS | ~2 h | [5.3](#section-53---figures-1013) |
| [Optional] Large-scale experiment | 9 | #2 | CloudLab | 10 min | [5.3.1](#optional-section-531---figure-9) |
| [Optional] Microbenchmarks | 14–16 | #3 | CloudLab | 2 h 20 min | [5.4](#optional-section-54---figures-1416) |

> [!NOTE]
> All experiments are long-running. Run them under `tmux` so a dropped SSH connection does not kill the run.

At the end of each experiment, the harness prints a summary of the run and writes it to
`scripts/results/suiteLogs_<timestamp>/summary.json`, with per-experiment logs alongside it. Here is an example of the summary output:

```
============================================================
                      Suite summary
============================================================
  SUCCEEDED                          672s  state_10gb  -> lazy_5MKeys
  SUCCEEDED                          681s  state_20gb  -> lazy_10MKeys
  ...
  Logs: /home/<user>/koala/scripts/results/suiteLogs_<timestamp>
  Results: /home/<user>/koala/scripts/results
```

### Section 5.2 - Figures 6 and 8

*(Human time: 10 minutes, run time: 4–5 hours)*

Sec 5.2 runs on AWS `c5d.4xlarge` instances and supports the primary claim of the paper:
> **Primary claim**: Koala effectively eliminates the reconfiguration disruption (e.g., throughput drop, backlog accumulation, latency spike) as compared to the baselines, while sustaining low processing latency during normal operation.

We compare against three baselines, all implemented in this repository and run through the same harness: **S&R** (stop-and-restart, the standard approach), **Remote** (state served from a remote storage service), and **DRRS** (the existing SOTA non-disruptive reconfiguration protocol).

> [!NOTE]
> Experiments in this section require large state aaccumulated before the reconfiguration. We have pre-generated warm-up state for the queries used in the experiments. If you want to run the experiments on your own cluster, please see [Appendix A](#appendix-a-rebuild-the-warm-up-state) for instructions on how to rebuild the warm-up state.

<!-- If you run into failures, rerun the experiment with the config the suite writes for you:

```bash
cd ~/ssd/koala/scripts
python3 runExperimentSuite.py results/suiteLogs_<timestamp>/rerunFailed.json
``` -->

**1. Run all 24 experiments (6 queries × 4 protocols)**

```bash
cd ~/ssd/koala/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.2/fullSuite.json
```

<!-- Six queries (`azure`, `borg`, `taxi`, `twitch`, `nexmark_query3`,
`nexmark_query6_modified`) × four protocols (Koala (`lazy`), `SAR`, `Remote`, `DRRS`),
producing result folders `<query>_<lazy|SAR|Remote|DRRS>`. Check that all 24 report
`SUCCEEDED` in the final summary; per-experiment logs are in
`scripts/results/suiteLogs_<timestamp>/`. -->

**2. Generate the plots**

```bash
cd ~/ssd/koala/scripts/nsdi27/evaluation/Section5.2
python3 runAllFigures.py
```
| Files | Paper figure |
|---|---|
| `Section5.2/Figure6/{azure,borg,taxi,twitch,q3,q6mod}.pdf` | Figure 6 |
| `Section5.2/Figure8/{azure,borg,taxi,twitch,q3,q6mod}_latency.pdf` | Figure 8 |

**3. Validate the results**

| Figure | What you should see |
|---|---|
| Figure 6 | Koala's throughput does not drop to zero after scaling out, while S&R stops entirely; Koala drains the backlog that builds up during reconfiguration faster than the other baselines. |
| Figure 8 | Koala's per-batch latency stays below Remote's throughout and shows no spike at the reconfiguration point. |


### Section 5.3 - Figures 10–13

*(Human time: 10 minutes, run time: ~2 hours)*

Sec 5.3 runs on AWS `c5d.4xlarge` instances and supports the secondary claim of the paper:
>**Secondary claim**: Koala can handle a variety of reconfiguration scenarios, including repeated reconfigurations, concurrent reconfigurations, skew-driven rebalancing, and task migration.

<!-- | Claim in the paper | Figure | Supported by |
|---|---|---|
| Koala maintains nondisruptive processing both when scaling out and when scaling in, demonstrating robustness under repeated reconfigurations | 10 | `Figure10.pdf` |
| After rebalance, Koala quickly resolves the bottleneck, converging to a stable and balanced throughput | 11 | `Figure11.pdf` |
| Fetch-on-demand and progressive-default sustain the input rate without disruption, and the latter completes the migration in the background | 12 | `Figure12.pdf` |
| Koala can also support cases where multiple target operators need to be reconfigured at once.  | 13 | `Figure13.pdf` | -->

**1. Run the experiments**

```bash
cd ~/ssd/koala/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/AWS/fullSuite.json
```

**2. Generate the figures**

```bash
cd ~/ssd/koala/scripts/nsdi27/evaluation/Section5.3/AWS
python3 runAllFigures.py
```

| Files | Paper figure |
|---|---|
| `Section5.3/AWS/Figure10.pdf` | Figure 10 |
| `Section5.3/AWS/Figure11.pdf` | Figure 11 |
| `Section5.3/AWS/Figure12.pdf` | Figure 12 |
| `Section5.3/AWS/Figure13.pdf` | Figure 13 |

**3. Validate the results**

As above, the claims are about trends rather than exact numbers.

| Figure | What you should see |
|---|---|
| Figure 10 | Throughput holds across all four reconfiguration points (t≈180, 360, 540, 720s); the volume of state transferred spikes at each one and then decays to zero as the working set is fetched. |
| Figure 11 | Before the rebalance, one task of the target operator carries most of the throughput; afterwards, per-task throughput evens out and Kafka lag returns to its steady-state level. |
| Figure 12 | Fetch-on-demand keeps throughput steady and spreads migration over a longer window; progressive migration moves state faster, and the larger chunk size shortens the migration at the cost of a larger disturbance to throughput. |
| Figure 13 | Reconfiguring two operators at the same time does not stall the pipeline: aggregate throughput stays flat and the Kafka queue does not build up. |


### [Optional] Section 5.3.1 - Figure 9

<details>
<summary>Click to expand the full instructions</summary>
<br>

*(Human time: 15 minutes, run time: 10 minutes)*

Sec 5.3.1 runs a single experiment with 32 tasks per operator, demonstrating that Koala scales to high parallelism without disruption. This experiment is a complement and not required for the main claims of the paper. This experiment runs on CloudLab `c6620` machines.

**1. Run the experiment**

```bash
cd ~/koala/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/Cloudlab/fullSuite.json
```

**2. Generate the figure**

```bash
cd ~/koala/scripts/nsdi27/evaluation/Section5.3/Cloudlab
python3 runAllFigures.py
```

| Files | Paper figure |
|---|---|
| `Section5.3/Cloudlab/Figure9.pdf` | Figure 9 |


**3. Validate the results**

Absolute values depend on the machines and on how Kafka and the producer happen to behave
during a run, so the claims are about trends and stability rather than exact numbers. The
reference values below come from the run used for the paper.

| Figure | Claim |
|---|---|
| 9a (Tput) | Throughput is flat before and after scale-out at t=180s, with no dip and no disruption to steady-state input rate |
| 9b (Kafka lag) | Kafka queue lag stays low throughout and does not spike during scale-out |

</details>


### [Optional] Section 5.4 - Figures 14–16

<details>
<summary>Click to expand the full instructions</summary>
<br>

*(Human time: 20 minutes, run time: 2 h 20 min)*

Sec 5.4 runs a set of microbenchmarks on CloudLab `c6620` machines to demonstrate the efficiency of Koala's lazy state access mechanism, including the impact of total state size, key lookup overhead, and key locality/skew. This section is **optional** and not required for the main claims of the paper.

<!-- All twelve experiments run Nexmark Q6\* (`nexmark_query6_modified`) under Koala for 10
minutes at a stable input rate; after three minutes the target operator scales out from 4
to 8 tasks. -->

<!-- | Claim in the paper | Figure | Supported by |
|---|---|---|
| "Koala demonstrates stable behavior despite increasing state size. Across all runs, it migrates around the same amount of state, as it exploits the temporal key locality of the workload, relocating the active working set only." | 14 | `Figure14.pdf` |
| "Key lookup takes less than 40µs per input batch across all runs, accounting for under 3% of per-batch latency." | 15a | `Figure15.pdf` |
| "While KLT grows with the key space, it remains compact, reaching at most 60 MB for 40M keys and 80 GB of total state." | 15b | `Figure15.pdf` |
| "Migration duration depends on the size of the active key space, as more keys from the old tasks are accessed after reconfiguration, resulting in more state being migrated on demand." | 16a | `Figure16.pdf` |
| "Higher skew leads to smoother reconfiguration, because (i) repeated accesses to hot keys incur no additional cost after initial fetch, and (ii) batch-based state access deduplicates keys before issuing the migration request." | 16b | `Figure16.pdf` | -->

<!-- | Step | Human time | Run time |
|---|---|---|
| Run the experiments | 1 min | 2 h 20 min |
| Generate the figures | 1 min | 1 min |
| Validate the results | 15 min | — | -->

**1. Run the experiments**

```bash
cd ~/koala/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.4/fullSuite.json
```

<!-- About 2 hours and 20 minutes for all twelve experiments. -->

**2. Generate the figures**

```bash
cd ~/koala/scripts/nsdi27/evaluation/Section5.4
python3 runAllFigures.py
```

| Files | Paper figure |
|---|---|
| `Section5.4/Figure14.pdf` | Figure 14 |
| `Section5.4/Figure15.pdf` | Figure 15 |
| `Section5.4/Figure16.pdf` | Figure 16 |

**3. Validate the results**

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


</details>

## Appendix A: rebuild the warm-up state

<details>
<summary>Click to expand</summary>

The clusters we provide already have the warm-up state generated, so this is only needed if you set up your own cluster. Each state size takes about 30 minutes to rebuild, and warm-up runs have to be started individually rather than through a suite.

A warm-up run generates a bounded, sequential key stream, one new key per event, so the resulting key space is exactly `NumEvents` times the 8 producers. At the end it moves each worker's `data/pebble` into the snapshot folder and writes out the key lookup table.

| State size | Config (under `scripts/`) | Key space |
|---|---|---|
| 10 GB | `nexmarkJson/query6/stateSize/query6ModWarmup10GB.json` | 5M keys |
| 20 GB | `nexmarkJson/query6/stateSize/query6ModWarmup20GB.json` | 10M keys |
| 40 GB | `nexmarkJson/query6/stateSize/query6ModWarmup40GB.json` | 20M keys |
| 80 GB | `nexmarkJson/query6/stateSize/query6ModWarmup80GB.json` | 40M keys |

```bash
cd ~/koala/scripts
python3 runExperiment.py nexmarkJson/query6/stateSize/query6ModWarmup40GB.json warmup_40GB
```

`OutputEventNumber` in a measurement config is the point in the key sequence where the generator resumes after warm-up, so if you change a warm-up's `NumEvents` you need to scale it by the same factor (46×): 625K maps to 28,750,000, 1.25M to 57,500,000.

</details>

## Appendix B: write your own query

<details>
<summary>Click to expand</summary>

A query is a Go dataflow built from the operators in [api/dataflow/](api/dataflow/). The smallest complete examples are in [query/examples/](query/examples/) — `counter.go` (a stateful count), `filter.go`, `mapper.go`, `tumblingWindow.go` — and the queries used in the paper are in [query/](query/) (`azure`, `borg`, `taxi`, `twitch`, `nexmark`).

To run one, register the query, rebuild with `make`, and point a JSON config at it via `QueryName`. The config controls the cluster layout, the runtime, the parallelism of each operator, when reconfigurations fire, and which protocol handles them:

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

</details>
