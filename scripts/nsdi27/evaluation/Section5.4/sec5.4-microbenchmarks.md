# Section 5.4: Microbenchmarks (optional)

This section reproduces the microbenchmarks in Section 5.4 of the paper: Figure 14 for
varying total state size, Figure 15 for key lookup overhead, and Figure 16 for varying key
locality and skew.

We allocate the CloudLab machines, set up SSH between the nodes, and generate the warm-up state.

## 1. Time required

| Step | Human time | Run time |
|---|---|---|
| Check cluster access | 3 min | 1 min |
| Run the experiments | 1 min | 2 h 20 min |
| Generate the figures | 1 min | 1 min |
| Compare with the paper | 15 min | — |


All twelve experiments run Nexmark Q6\* (`nexmark_query6_modified`) under Koala for 10
minutes at a stable input rate. After three minutes, the target operator scales out from 4 to 8 tasks.

## 2. Claims this section supports

| Claim in the paper | Figure | Supported by |
|---|---|---|
| "Koala demonstrates stable behavior despite increasing state size. Across all runs, it migrates around the same amount of state, as it exploits the temporal key locality of the workload, relocating the active working set only." | 14 | `state_size.pdf` |
| "Key lookup takes less than 40µs per input batch across all runs, accounting for under 3% of per-batch latency." | 15a | `key_lookup_overhead.pdf` |
| "While KLT grows with the key space, it remains compact, reaching at most 60 MB for 40M keys and 80 GB of total state." | 15b | `key_lookup_overhead.pdf` |
| "Migration duration depends on the size of the active key space, as more keys from the old tasks are accessed after reconfiguration, resulting in more state being migrated on demand." | 16a | `data_distribution.pdf` |
| "Higher skew leads to smoother reconfiguration, because (i) repeated accesses to hot keys incur no additional cost after initial fetch, and (ii) batch-based state access deduplicates keys before issuing the migration request." | 16b | `data_distribution.pdf` |


## 4. Cluster access

The cluster has eight `c6620` nodes on CloudLab Utah. 

| Node | Role |
|---|---|
| `10.10.1.1` | Kafka broker, 2 producers, coordinator. Log in here and run everything from this node. |
| `10.10.1.2`, `10.10.1.3`, `10.10.1.4` | Kafka broker and 2 producers each |
| `10.10.1.5` - `10.10.1.8` | Workers. 17 worker processes in total: 8 sources, 4 mapper tasks plus 4 more for the scale-out, and 1 sink. |

Cluster check:

```bash
ssh <user>@10.10.1.1
sudo -i
cd ~/koala
git status -sb                     # should report main
for ip in 10.10.1.2 10.10.1.3 10.10.1.4 10.10.1.8 10.10.1.9 10.10.1.10 10.10.1.12; do
  ssh -o BatchMode=yes $ip 'hostname' || echo "UNREACHABLE $ip"
done
ssh 10.10.1.9 'du -sh ~/koala/pebble_warmup_data/nexmark_q6_mod/*'
```

You should see all eight nodes respond, and four warm-up snapshots (10, 20, 40 and 80 GB)
on the worker.

## 5. Running the experiments


```bash
cd ~/koala/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.4/fullSuite.json
```

This takes about 2 hours and 20 minutes to run all the 12 experiments. 





| Suite entries | What varies | Result folders (prefixed `nexmark_query6_modified_`) |
|---|---|---|
| `state_10gb` … `state_80gb` | total state: 10, 20, 40, 80 GB | `lazy_5MKeys`, `lazy_10MKeys`, `lazy_20MKeys`, `lazy_40MKeys` |
| `locality_500k` … `locality_4m` | active key space: 500K, 1M, 2M, 4M | `lazy_500k_50k_25`, `lazy_1M_100k_25`, `lazy_2M_200k_25`, `lazy_4M_400k_25` |
| `skew_0` … `skew_75` | hot key ratio: 0, 25, 50, 75% | `lazy_2M_200k_0`, `lazy_2M_200k`, `lazy_2M_200k_50`, `lazy_2M_200k_75` |


The script shows intermidiate results:
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

All twelve should say `SUCCEEDED`. The summary is saved as
`results/suiteLogs_<timestamp>/summary.json`, and the full output of each experiment as
`results/suiteLogs_<timestamp>/<NN>_<name>_attemptN.log`.

**If an experiment fails.** Each experiment is retried twice before the suite gives up on it, and
partial results from a failed attempt are removed so the retry writes to the expected
folder. Re-running the suite is safe. To repeat only the entries that failed, pass their
names to `--only`. Pressing Ctrl-C shuts the running experiment down cleanly on all nodes
and stops the suite.

## 6. Generating the figures

```bash
cd ~/koala/scripts/nsdi27/evaluation/Section5.4
python3 runAllFigures.py
```

This runs each figure script (`figure14.py`, `figure15.py`, `figure16.py`) and:
1. Reads the result folders from the experiments
2. For Figure 15, extracts worker IPs from the experiment config files and
   measures key lookup table sizes from
   `~/koala/pebbleLookUpTable/` on each worker. To skip the
   SSH measurement, pass the sizes directly:
   `python3 figure15.py --klt-sizes 17,34,67,134`
3. Writes one PDF per figure next to the scripts:

| File | Paper figure |
|---|---|
| `Figure14.pdf` | Figure 14 |
| `Figure15.pdf` | Figure 15 |
| `Figure16.pdf` | Figure 16 (both rows) |

## 7. Comparing with the paper

Absolute values depend on the machines and on how Kafka and Pebble happen to behave during
a run, so the claims are about trends and orders of magnitude rather than exact numbers.
The reference values below come from the run used for the paper.

| Figure | What you should see | Reference values |
|---|---|---|
| 14a (Tput) | Throughput is flat across the scale-out at t=180s in all four panels, with no dip and no downtime | roughly 8,000 records/s aggregated at the source, before and after |
| 14b (State) | At scale-out, migrated state spikes and then tapers to zero. The total state migrated stays roughly constant even as total state size grows from 10 to 80 GB. | 1.41, 1.44, 1.43, 1.43 GB |
| 15a (Latency) | Key lookup time is flat and two orders of magnitude below per-batch processing time | key lookup 37–43 µs against per-batch 1.33–1.42 ms, under 3% |
| 15b (KLT size) | The key lookup table grows with the key space but stays small | 17, 34, 67, 134 MB for 10-80GB state |
| 16a (Locality) | Both the volume migrated and the time it takes grow with the active key space, close to linearly | 468, 936, 1824, 3696 MB for 500K, 1M, 2M and 4M active keys |
| 16b (Skew) | More skew means less state migrated and a shorter tail | 1899, 1824, 1806, 1597 MB for 0, 25, 50 and 75% hot keys |


## 9. Changing the experiments
Adding a new config to `nsdi27/evaluation/Section5.4/fullSuite.json` file as another `Experiments` entry lets it run the same way as the rest.

| Field | Effect |
|---|---|
| `NumActivePeople` | The active key space (the x-axis of Figure 16a). Keep `HotSellerRange` at about 10% of it to hold skew constant. |
| `HotSellerRatio` | The fraction of records that hit the hot key set, between 0 and 1 (the x-axis of Figure 16b). |
| `TargetParrallelism` | The scale-out(in) factor. `WorkerIPs` must have enough entries for the resulting task count. |


## Appendix A: rebuilding the warm-up state

Each state size takes about 30 minutes to rebuild, and warm-up runs have to be
started individually rather than through a suite.

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
cd ~/koala/scripts
python3 runExperiment.py nexmarkJson/query6/stateSize/query6ModWarmup40GB.json warmup_40GB
```


`OutputEventNumber` in a measurement config is the point in the key sequence where the generator resumes after warm-up, so if you change a warm-up's
`NumEvents` you need to scale it by the same factor(46x):625K maps to 28,750,000, 1.25M to 57,500,000.
