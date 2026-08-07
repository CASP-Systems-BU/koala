What we want to show, tell the context failure solution
# Section 5.2 — Reproducing Figure 6 and Figure 8

Run both steps on the coordinator node (`10.10.1.1`). Takes about 4~5 hours.

The cluster is ready to use: all packages are installed and the warm-up data for
every query is already generated, so no warm-up phase is needed.

## 1. Run the 24 experiments

```bash
cd ~/ssd/koala/scripts
python3 runExperimentSuite.py nsdi27/evaluation/Section5.2/fullSuite.json
```

Six queries (`azure`, `borg`, `taxi`, `twitch`, `nexmark_query3`,
`nexmark_query6_modified`) × four protocols (`Koala(lazy)`, `SAR`, `Remote`, `DRRS`).
Check that all 24 report `SUCCEEDED` in the final summary(per-experiment logs are
in `scripts/results/suiteLogs_<timestamp>/`).

### If some experiments fail

Rerun them with the config the suite writes for you:

```bash
cd ~/ssd/koala/scripts
python3 runExperimentSuite.py results/suiteLogs_<timestamp>/rerunFailed.json
```

Repeat until all 24 report `SUCCEEDED`, then continue with step 2.

## 2. Generate the plots

```bash
cd ~/ssd/koala/scripts/nsdi27/evaluation/Section5.2
python3 runAllFigures.py
```

Exits non-zero if any figure fails. Output:

- **Figure 6** — `Figure6/{azure,borg,taxi,twitch,q3,q6mod}.pdf`
- **Figure 8** — `Figure8/{azure,borg,taxi,twitch,q3,q6mod}_latency.pdf`

## What to expect

Figure 6: Koala has no downtime after the scale-out, unlike S&R, and consumes the
backlog much faster than other baselines.

Figure 8: Koala's per-batch latency is lower than Remote's and does not spike
during reconfiguration.
