# Section 5.3.1: High Parallelism

This section reproduces the scalability results in Section 5.3.1 of the paper: Figure 9 for Koala demonstrating stable throughput during scale-out at high parallelism.

We allocate the CloudLab machines, set up SSH between the nodes, and generate the warm-up state.

## 1. Time required

| Step | Human time | Run time |
|---|---|---|
| Check cluster access | 3 min | 1 min |
| Run the experiment | 1 min | 10 min |
| Generate the figures | 1 min | 1 min |
| Compare with the paper | 10 min | — |


The experiment runs Nexmark Q6\* (`nexmark_query6_modified`) under Koala for 10 minutes at a stable input rate. After three minutes, the target operator scales out from 16 to 32 tasks.

## 2. Claims this section supports

| Claim in the paper | Figure | Supported by |
|---|---|---|
| "Koala maintains stable throughput during scale-out at high parallelism, with minimal disruption to the Kafka queue." | 9 | `scalability.pdf` |

## 4. Cluster access

The cluster has 20 `c6620` nodes on CloudLab Utah.

| Node | Role |
|---|---|
| `10.10.1.1` | Kafka broker, 2 producers, coordinator. Log in here and run everything from this node. |
| `10.10.1.2`, `10.10.1.3`, `10.10.1.4` | Kafka broker and 2 producers each |
| `10.10.1.5` – `10.10.1.20` | Workers. 16 worker processes initially: 16 sources, 16 stateful mapper tasks, and 1 sink; scale-out adds 16 more mapper tasks (32 total). |

Cluster check:

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

You should see all 16 worker nodes respond, and the 80 GB warm-up snapshot for parallelism-16
on the worker.

## 5. Running the experiment

```bash
cd ~/disaggregated-streaming/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.3/Cloudlab/fullSuite.json
```

This takes about 10 minutes to run.

The script shows intermediate results:

```
============================================================
                      Suite summary
============================================================
  SUCCEEDED                          598s  parallelism_16_to_32  -> lazy_2M_200k
  Logs: /home/<user>/disaggregated-streaming/scripts/results/suiteLogs_<timestamp>
  Results: /home/<user>/disaggregated-streaming/scripts/results
```

The experiment should say `SUCCEEDED`. The summary is saved as
`results/suiteLogs_<timestamp>/summary.json`, and the full output as
`results/suiteLogs_<timestamp>/<NN>_parallelism_16_to_32_attemptN.log`.

**If the experiment fails.** It is retried twice before the suite gives up, and
partial results from a failed attempt are removed so the retry writes to the expected
folder. Re-running the suite is safe. Pressing Ctrl-C shuts the running experiment down cleanly on all nodes
and stops the suite.

## 6. Generating the figures

```bash
cd ~/disaggregated-streaming/scripts/nsdi27/evaluation/Section5.3/Cloudlab
python3 runAllFigures.py
```

This runs each figure script (`figure9.py`), reads the result folder from the
experiment, and writes one PDF next to the scripts:

| File | Paper figure |
|---|---|
| `Figure9.pdf` | Figure 9 |

## 7. Comparing with the paper

Absolute values depend on the machines and on how Kafka and the producer happen to behave
during a run, so the claims are about trends and stability rather than exact numbers.
The reference values below come from the run used for the paper.

| Figure | What you should see | Reference values |
|---|---|---|
| 9a (Tput) | Throughput is flat before and after scale-out at t=180s, with no dip and no disruption to steady-state input rate | ~800,000 records/s aggregated across 16 sources before, after scale-out; minimal variance |
| 9b (Kafka lag) | Kafka queue lag stays low throughout and does not spike during scale-out | lag under 30 seconds before and after; minimal disruption at t=180s |

## 9. Changing the experiment

Adding new configs to test different parallelism levels or other high-parallelism scenarios
is straightforward: create a new config in `nexmarkJson/query6/highParallelism/` and add an
entry to `nsdi27/evaluation/Section5.3/Cloudlab/fullSuite.json`.

| Field | Effect |
|---|---|
| `StatefulMapperParallelism` | Initial parallelism of the target operator |
| `TargetParrallelism` | The scale-out(in) target parallelism. `WorkerIPs` must have enough entries for the resulting task count. |