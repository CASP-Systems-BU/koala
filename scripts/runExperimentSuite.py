import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime
from typing import Optional


"""
Run a *sequence* of experiments back-to-back (a "suite").

This is a thin driver on top of `runExperiment.py`: for every entry in the
suite it optionally switches the git branch, then runs one full
`runExperiment.py <configPath> <resultKeyword>` to completion (including its
own clean-up), waits for a short cool-down, and moves on to the next one.

Typical use: run the Azure query for lazy, stop-and-restart, remote-pebble and
DRRS in one shot, then walk away.

Flaky clusters: set "MaxAttempts" (suite-wide, or per experiment) to retry an
experiment that does not report SUCCEEDED. Before each retry the partial
results that the failed attempt produced are deleted, so the successful run
lands in the canonical `results/<query>_<keyword>` folder rather than a
timestamp-suffixed one.

Only successful results are kept: when an experiment ends up not SUCCEEDED, the
result folders its attempts created are removed, so `scripts/results/` only
ever holds metricDBs from runs that completed. Only folders created by this
suite run are ever removed - results from earlier runs are never touched - and
the per-attempt logs are always kept, so a post-mortem is still possible. Set
"KeepFailedResults": true to keep the partial metricDBs as well.

Reruns: if anything did not succeed, the suite writes `rerunFailed.json` next
to the logs. It inherits every top-level setting and lists exactly the
experiments that failed plus any the suite never reached, in the original
order, so rerunning is a single command:
    python3 runExperimentSuite.py <resultsDir>/suiteLogs_<ts>/rerunFailed.json

Prerequisites (same as runExperiment.py):
1. The working directory to run this script must be the `scripts/` folder
2. Must be on the root/coordinator node (ssh access to all other nodes)
3. Warm-up data must already exist on the nodes (this script never runs a
   warm-up pass - point each config at the warm-up folder it needs)

Usage:
    python3 runExperimentSuite.py <suiteConfigPath>
    python3 runExperimentSuite.py nsdi27/suites/azureSuite.json

    # Preview what would run without running anything
    python3 runExperimentSuite.py nsdi27/suites/azureSuite.json --dry-run

    # Run only a subset of the suite (by "Name")
    python3 runExperimentSuite.py nsdi27/suites/azureSuite.json --only lazy,sr

    # Also stream runExperiment.py's output to the console. By default only the
    # suite's own [SUITE ...] lines are printed; each experiment's full output
    # always goes to results/suiteLogs_<timestamp>/<NN>_<name>_attemptN.log
    python3 runExperimentSuite.py nsdi27/suites/azureSuite.json --verbose

Why a subprocess instead of `import runExperiment`?
Each experiment runs as a fresh `python3 runExperiment.py ...` process. That
matters for the DRRS entry: DRRS lives on a different branch, and a fresh
process picks up that branch's version of runExperiment.py / utils.py. It also
isolates a crash in one experiment from the rest of the suite.
"""


###############################################################################
#                                  Defaults
###############################################################################

# Seconds to wait between two experiments (lets Kafka ports / sockets settle)
DefaultCooldownSeconds = 60

# Attempts per experiment. 1 = no retry. Bump this (or set MaxAttempts in the
# suite config) on clusters where nodes are flaky e.g. CloudLab
DefaultMaxAttempts = 1

# Seconds to wait after a failed attempt before retrying it
DefaultRetryDelaySeconds = 60

# Echo `runExperiment.py`'s output to the console as well as the log file.
# Off by default, so the suite prints only its own [SUITE ...] lines - the full
# child output always lands in results/suiteLogs_<timestamp>/ regardless.
# Turned on by --verbose
StreamChildOutput = False

# Files that runExperiment.py rewrites in place. They are tracked by git, so
# they must be restored before a branch switch or the checkout will fail.
GeneratedFiles = [
    "config.yaml",
    "scripts/taskPlacement/customPlacement.txt",
]


###############################################################################
#                              Suite config model
###############################################################################


class Experiment:
    def __init__(self, entry: dict, idx: int):
        # Keep the original JSON entry so the rerun-suite file can re-emit this
        # experiment verbatim, including any keys this class does not model
        self.raw: dict = dict(entry)
        self.name: str = entry.get("Name", f"experiment{idx + 1}")
        self.configPath: str = entry["ConfigPath"]
        self.resultKeyword: str = entry["ResultKeyword"]
        # [Optional] Branch to check out before this experiment. None = stay on
        # whatever branch is currently checked out.
        self.branch: Optional[str] = entry.get("Branch") or None
        # [Optional] Per-experiment cool-down override (seconds)
        self.cooldownSeconds: Optional[int] = entry.get("CooldownSeconds")
        # [Optional] Per-experiment retry budget override
        self.maxAttempts: Optional[int] = entry.get("MaxAttempts")
        # [Optional] Skip this entry without deleting it from the suite file
        self.skip: bool = bool(entry.get("Skip", False))

    def __str__(self) -> str:
        branch = self.branch if self.branch else "<current>"
        return f"{self.name} [branch={branch}] {self.configPath} -> {self.resultKeyword}"


class Suite:
    def __init__(self, suiteMap: dict):
        # Keep the original suite map so the rerun-suite file inherits every
        # top-level setting unchanged
        self.raw: dict = dict(suiteMap)
        self.workDir: str = os.path.expanduser(suiteMap["WorkDir"])
        self.cooldownSeconds: int = suiteMap.get(
            "CooldownSeconds", DefaultCooldownSeconds
        )
        # How many times to run an experiment before giving up on it. 1 = no retry
        self.maxAttempts: int = suiteMap.get("MaxAttempts", DefaultMaxAttempts)
        # How long to wait after a failed attempt before retrying
        self.retryDelaySeconds: int = suiteMap.get(
            "RetryDelaySeconds", DefaultRetryDelaySeconds
        )
        # If true, abort the remaining experiments as soon as one fails
        # (i.e. after its retries are exhausted)
        self.stopOnFailure: bool = bool(suiteMap.get("StopOnFailure", False))
        # If true, keep the partial results of an experiment that never
        # succeeded. Default false: scripts/results/ then holds only results
        # from experiments that reported SUCCEEDED
        self.keepFailedResults: bool = bool(suiteMap.get("KeepFailedResults", False))
        # If true, `git fetch` before checking out a branch
        self.gitFetch: bool = bool(suiteMap.get("GitFetch", False))
        # If true, go back to the branch we started on when the suite finishes
        self.restoreBranch: bool = bool(suiteMap.get("RestoreBranch", True))

        experiments = suiteMap.get("Experiments", [])
        if not experiments:
            raise Exception("[ERROR] Suite config has no 'Experiments' entries")
        self.experiments: list[Experiment] = [
            Experiment(e, i) for i, e in enumerate(experiments)
        ]


###############################################################################
#                                 Git helpers
###############################################################################


def runGit(workDir: str, args: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", "-C", workDir] + args,
        capture_output=True,
        text=True,
    )


def gitCurrentBranch(workDir: str) -> str:
    res = runGit(workDir, ["rev-parse", "--abbrev-ref", "HEAD"])
    if res.returncode != 0:
        raise Exception(f"[ERROR] Not a git repo or git failed: {res.stderr.strip()}")
    return res.stdout.strip()


def gitBranchExists(workDir: str, branch: str) -> bool:
    local = runGit(workDir, ["rev-parse", "--verify", "--quiet", branch])
    if local.returncode == 0:
        return True
    remote = runGit(workDir, ["rev-parse", "--verify", "--quiet", f"origin/{branch}"])
    return remote.returncode == 0


def gitFileExistsOnBranch(workDir: str, branch: str, repoRelPath: str) -> bool:
    for rev in (branch, f"origin/{branch}"):
        res = runGit(workDir, ["cat-file", "-e", f"{rev}:{repoRelPath}"])
        if res.returncode == 0:
            return True
    return False


def restoreGeneratedFiles(workDir: str) -> None:
    """runExperiment.py rewrites config.yaml and customPlacement.txt in place.
    Discard those edits so a branch switch is not blocked by local changes."""
    for path in GeneratedFiles:
        # `--` disambiguates path from branch name; ignore files that do not
        # exist on the current branch
        runGit(workDir, ["checkout", "--", path])


def switchBranch(workDir: str, branch: str, gitFetch: bool) -> None:
    current = gitCurrentBranch(workDir)
    if current == branch:
        print(f"[SUITE INFO] Already on branch '{branch}', no switch needed")
        return

    print(f"[SUITE INFO] Switching branch: '{current}' -> '{branch}'")

    # Drop the config edits made by the previous experiment
    restoreGeneratedFiles(workDir)

    if gitFetch:
        print("[SUITE INFO] Running git fetch ...")
        runGit(workDir, ["fetch", "--all", "--prune"])

    # Prefer an existing local branch, otherwise track the remote one
    hasLocal = runGit(workDir, ["rev-parse", "--verify", "--quiet", branch]).returncode == 0
    if hasLocal:
        res = runGit(workDir, ["checkout", branch])
    else:
        res = runGit(workDir, ["checkout", "-b", branch, f"origin/{branch}"])

    if res.returncode != 0:
        raise Exception(
            f"[ERROR] Failed to checkout branch '{branch}':\n"
            f"{res.stdout.strip()}\n{res.stderr.strip()}"
        )

    print(f"[SUITE INFO] Now on branch '{gitCurrentBranch(workDir)}'")


###############################################################################
#                        Result folder book-keeping
###############################################################################

# moveMetricDBToResults() writes to <query>_<keyword>, or appends a timestamp
# (e.g. azure_lazy_20260801T142233) when that folder already exists
TimestampSuffixPattern = re.compile(r"_\d{8}T\d{6}$")


def resultFolderMatches(folderName: str, queryName: str, keyword: str) -> bool:
    """True if folderName is a result folder produced for this (query, keyword).

    Matches the plain `<query>_<keyword>` name and the timestamp-suffixed
    variant, and nothing else - so a keyword like 'lazy' never swallows the
    results of a different experiment whose keyword is 'lazy_v2'.
    """
    base = f"{queryName}_{keyword}"
    if folderName == base:
        return True
    if folderName.startswith(base) and TimestampSuffixPattern.fullmatch(
        folderName[len(base):]
    ):
        return True
    return False


def snapshotResultFolders(resultsDir: str, queryName: str, keyword: str) -> set[str]:
    """Names of the result folders that already exist for this experiment."""
    if not os.path.isdir(resultsDir):
        return set()
    return {
        name
        for name in os.listdir(resultsDir)
        if os.path.isdir(os.path.join(resultsDir, name))
        and resultFolderMatches(name, queryName, keyword)
    }


def purgeNewResultFolders(
    resultsDir: str, queryName: str, keyword: str, before: set[str]
) -> None:
    """Delete result folders created by the attempt that just failed.

    Only folders absent from the pre-attempt snapshot are removed, so results
    from earlier runs or earlier sessions are never touched.
    """
    after = snapshotResultFolders(resultsDir, queryName, keyword)
    created = sorted(after - before)
    if not created:
        print("[SUITE INFO] Failed attempt left no result folder to clean up")
        return
    for name in created:
        path = os.path.join(resultsDir, name)
        print(f"[SUITE INFO] Removing partial results from failed attempt: {path}")
        try:
            shutil.rmtree(path)
        except OSError as e:
            print(f"[SUITE WARNING] Could not remove {path}: {e}")


def readQueryName(configFullPath: str) -> Optional[str]:
    """Read QueryName from an experiment config, or None if unreadable.

    Called after the branch switch, so the file is the one the run will use.
    """
    try:
        with open(configFullPath) as f:
            return json.load(f).get("QueryName")
    except (OSError, ValueError) as e:
        print(f"[SUITE WARNING] Could not read QueryName from {configFullPath}: {e}")
        return None


###############################################################################
#                            Rerun suite generation
###############################################################################


def writeRerunSuiteFile(
    suite: Suite,
    experiments: list[Experiment],
    results: list,
    logDir: str,
) -> Optional[str]:
    """Write a ready-to-run suite JSON covering everything that did not succeed.

    Includes both experiments that failed and experiments the suite never got
    to (e.g. after an abort), in their original suite order, so a single rerun
    finishes the job. Every top-level suite setting is inherited unchanged.

    Returns the path written, or None if everything succeeded.
    """

    statusByName = {exp.name: status for exp, status, _, _ in results}

    pending = [
        exp
        for exp in experiments
        if statusByName.get(exp.name, "NOT RUN") != "SUCCEEDED"
    ]
    if not pending:
        return None

    rerun = dict(suite.raw)
    entries = []
    for exp in pending:
        entry = dict(exp.raw)
        # A rerun must actually run these, so drop any Skip flag
        entry.pop("Skip", None)
        entries.append(entry)
    rerun["Experiments"] = entries

    rerunPath = os.path.join(logDir, "rerunFailed.json")
    with open(rerunPath, "w") as f:
        json.dump(rerun, f, indent=2)

    print("\n============================================================")
    print("                   Rerun the failures")
    print("============================================================")
    for exp in pending:
        print(f"  {statusByName.get(exp.name, 'NOT RUN'):<32} {exp.name}")
    print(f"\n  Wrote: {rerunPath}")
    print("\n  Rerun with:")
    print(f"    cd {os.path.join(suite.workDir, 'scripts')}")
    print(f"    python3 runExperimentSuite.py {rerunPath}")
    print("============================================================")

    return rerunPath


###############################################################################
#                              Experiment runner
###############################################################################


def runOneExperiment(
    exp: Experiment,
    scriptsDir: str,
    logFilePath: str,
) -> tuple[str, int]:
    """Run a single `runExperiment.py` as a child process, writing its output to
    a log file (and to the console too when StreamChildOutput is set).

    Returns (status, elapsedSeconds) where status is
    SUCCEEDED / FAILED / INTERRUPTED.
    """

    cmd = [sys.executable, "runExperiment.py", exp.configPath, exp.resultKeyword]
    print(f"[SUITE INFO] Command: {' '.join(cmd)}")
    print(f"[SUITE INFO] Log file: {logFilePath}")

    start = time.time()
    status = "FAILED"

    # runExperiment.py swallows its own exceptions and still exits 0, so the
    # return code alone is not enough - also look for its final status banner
    sawSucceededBanner = False

    with open(logFilePath, "w", buffering=1) as logFile:
        proc = subprocess.Popen(
            cmd,
            cwd=scriptsDir,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
        try:
            # Always drain the pipe (a full pipe would block the child), but
            # only echo it when the user asked for it
            for line in proc.stdout:
                if StreamChildOutput:
                    sys.stdout.write(line)
                logFile.write(line)
                if "Experiment Status: SUCCEEDED" in line:
                    sawSucceededBanner = True
            returnCode = proc.wait()
            if returnCode == 0 and sawSucceededBanner:
                status = "SUCCEEDED"
            else:
                print(
                    f"[SUITE ERROR] '{exp.name}' did not report success "
                    f"(exit code {returnCode}) - see {logFilePath}"
                )
        except KeyboardInterrupt:
            # Ctrl-C reaches the child too - runExperiment.py installs its own
            # SIGINT handler that shuts everything down, so give it time
            print(
                "\n[SUITE INFO] Ctrl-C received - waiting for the running "
                "experiment to clean up ..."
            )
            try:
                proc.wait(timeout=300)
            except subprocess.TimeoutExpired:
                print("[SUITE ERROR] Clean-up timed out, killing the child process")
                proc.kill()
            status = "INTERRUPTED"

    return status, int(time.time() - start)


###############################################################################
#                                 Preflight
###############################################################################


def preflight(suite: Suite, experiments: list[Experiment], scriptsDir: str) -> None:
    """Validate everything we can before burning hours of cluster time."""

    print("\n============================================================")
    print("                     Preflight checks...")
    print("============================================================")

    if not os.path.isdir(scriptsDir):
        raise Exception(f"[ERROR] scripts dir not found: {scriptsDir}")

    if not os.path.isfile(os.path.join(scriptsDir, "runExperiment.py")):
        raise Exception(f"[ERROR] runExperiment.py not found in {scriptsDir}")

    if suite.maxAttempts < 1:
        raise Exception("[ERROR] MaxAttempts must be >= 1")

    seenKeywords = set()
    for exp in experiments:

        if exp.maxAttempts is not None and exp.maxAttempts < 1:
            raise Exception(f"[ERROR] '{exp.name}': MaxAttempts must be >= 1")

        # Config file must exist - either on disk (current branch) or in the
        # tree of the branch we are going to check out
        repoRelPath = f"scripts/{exp.configPath}"
        if exp.branch:
            if not gitBranchExists(suite.workDir, exp.branch):
                raise Exception(
                    f"[ERROR] '{exp.name}': branch '{exp.branch}' not found "
                    f"(locally or on origin)"
                )
            if not gitFileExistsOnBranch(suite.workDir, exp.branch, repoRelPath):
                raise Exception(
                    f"[ERROR] '{exp.name}': {repoRelPath} does not exist on "
                    f"branch '{exp.branch}'"
                )
        else:
            localPath = os.path.join(scriptsDir, exp.configPath)
            if not os.path.isfile(localPath):
                raise Exception(f"[ERROR] '{exp.name}': config not found: {localPath}")
            # Warm-up runs do not belong in a suite - they produce no results
            with open(localPath) as f:
                configMap = json.load(f)
            if configMap.get("IsWarmUp", False):
                raise Exception(
                    f"[ERROR] '{exp.name}': IsWarmUp is true. This suite only "
                    f"runs measurement experiments - prepare warm-up data first."
                )

        # Duplicate keywords would silently produce timestamp-suffixed folders
        if exp.resultKeyword in seenKeywords:
            print(
                f"[SUITE WARNING] Duplicate ResultKeyword '{exp.resultKeyword}' - "
                f"results will be written to a timestamped folder"
            )
        seenKeywords.add(exp.resultKeyword)

        print(f"  [OK] {exp}")

    print("[SUITE INFO] Preflight passed\n")


###############################################################################
#                                    Main
###############################################################################


def runSuite(suitePath: str, only: Optional[list[str]], dryRun: bool) -> int:

    with open(suitePath) as f:
        suite = Suite(json.load(f))

    scriptsDir = os.path.join(suite.workDir, "scripts")

    # Filter by --only if provided
    experiments = [e for e in suite.experiments if not e.skip]
    if only:
        experiments = [e for e in experiments if e.name in only]
        missing = set(only) - {e.name for e in experiments}
        if missing:
            raise Exception(f"[ERROR] --only names not found in suite: {missing}")
    if not experiments:
        raise Exception("[ERROR] No experiments to run after filtering")

    startingBranch = gitCurrentBranch(suite.workDir)

    print("\n============================================================")
    print(f"  Experiment suite: {os.path.basename(suitePath)}")
    print(f"  WorkDir:          {suite.workDir}")
    print(f"  Current branch:   {startingBranch}")
    print(f"  Experiments:      {len(experiments)}")
    print(f"  Cooldown:         {suite.cooldownSeconds}s")
    print(f"  MaxAttempts:      {suite.maxAttempts} (retry delay {suite.retryDelaySeconds}s)")
    print(f"  StopOnFailure:    {suite.stopOnFailure}")
    print("============================================================")

    preflight(suite, experiments, scriptsDir)

    if dryRun:
        print("[SUITE INFO] --dry-run set, exiting without running anything")
        return 0

    # Logs live under results/ which is gitignored, so they survive branch
    # switches and never dirty the working tree
    timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
    resultsDir = os.path.join(scriptsDir, "results")
    logDir = os.path.join(resultsDir, f"suiteLogs_{timestamp}")
    os.makedirs(logDir, exist_ok=True)

    results = []
    aborted = False

    for i, exp in enumerate(experiments):
        print("\n\n############################################################")
        print(f"#  [{i + 1}/{len(experiments)}] {exp.name}")
        print(f"#  config:  {exp.configPath}")
        print(f"#  keyword: {exp.resultKeyword}")
        print(f"#  branch:  {exp.branch if exp.branch else '<current>'}")
        print("############################################################\n")

        # --- Branch switch (DRRS lives on its own branch) ---
        try:
            if exp.branch:
                switchBranch(suite.workDir, exp.branch, suite.gitFetch)
        except Exception as e:
            print(f"[SUITE ERROR] {e}")
            results.append((exp, "SKIPPED (branch switch failed)", 0, 0))
            if suite.stopOnFailure:
                aborted = True
                break
            continue

        # --- Run it, retrying a failed attempt up to MaxAttempts times ---
        # Read QueryName *after* the branch switch so it reflects the config
        # this run will actually use. It is needed to locate the result folder
        # a failed attempt may have left behind.
        queryName = readQueryName(os.path.join(scriptsDir, exp.configPath))
        maxAttempts = (
            exp.maxAttempts if exp.maxAttempts is not None else suite.maxAttempts
        )

        status, elapsed, attempt = "FAILED", 0, 0
        for attempt in range(1, maxAttempts + 1):
            if maxAttempts > 1:
                print(
                    f"\n[SUITE INFO] '{exp.name}': attempt {attempt}/{maxAttempts}"
                )

            # Record which result folders exist before the attempt, so a retry
            # can delete exactly the ones this attempt creates - and nothing else
            foldersBefore = set()
            if queryName:
                foldersBefore = snapshotResultFolders(
                    resultsDir, queryName, exp.resultKeyword
                )

            logFilePath = os.path.join(
                logDir, f"{i + 1:02d}_{exp.name}_attempt{attempt}.log"
            )
            status, elapsed = runOneExperiment(exp, scriptsDir, logFilePath)

            # Done - either it worked, or the user hit Ctrl-C and wants out
            if status in ("SUCCEEDED", "INTERRUPTED"):
                break

            # Out of retries - the post-loop cleanup below removes whatever
            # partial results this attempt produced
            if attempt >= maxAttempts:
                if maxAttempts > 1:
                    print(
                        f"[SUITE ERROR] '{exp.name}' failed {maxAttempts} times, "
                        f"giving up"
                    )
                break

            # Retry: drop the partial results first so the next attempt writes
            # to the canonical <query>_<keyword> folder instead of a
            # timestamp-suffixed one. Cluster-side state needs no clean-up here
            # - runExperiment.py already wipes data/ on every node on the way
            # out, and again at the start of the next run
            print(f"[SUITE INFO] '{exp.name}' attempt {attempt} failed")
            if queryName:
                purgeNewResultFolders(
                    resultsDir, queryName, exp.resultKeyword, foldersBefore
                )
            else:
                print(
                    "[SUITE WARNING] QueryName unknown - cannot clean up partial "
                    "results. The retry may land in a timestamped folder."
                )

            if suite.retryDelaySeconds > 0:
                print(
                    f"[SUITE INFO] Waiting {suite.retryDelaySeconds}s before retry ..."
                )
                try:
                    time.sleep(suite.retryDelaySeconds)
                except KeyboardInterrupt:
                    print("[SUITE INFO] Ctrl-C during retry delay, aborting suite")
                    status = "INTERRUPTED"
                    break

        # --- Keep scripts/results/ clean: only SUCCEEDED runs leave results ---
        # Same narrow rule as the retry cleanup: remove only folders created by
        # this experiment's attempts, never anything that pre-existed. The logs
        # under suiteLogs_<timestamp>/ are untouched, so post-mortem is still
        # possible. Set "KeepFailedResults": true to keep the metricDB instead.
        if status != "SUCCEEDED" and not suite.keepFailedResults:
            if queryName:
                print(
                    f"[SUITE INFO] '{exp.name}' did not succeed - removing its "
                    f"results so only successful runs remain"
                )
                purgeNewResultFolders(
                    resultsDir, queryName, exp.resultKeyword, foldersBefore
                )
            else:
                print(
                    "[SUITE WARNING] QueryName unknown - cannot remove partial "
                    "results for a failed experiment"
                )

        results.append((exp, status, elapsed, attempt))

        attemptNote = f" after {attempt} attempts" if attempt > 1 else ""
        print(f"\n[SUITE INFO] '{exp.name}' finished: {status} ({elapsed}s){attemptNote}")

        if status == "INTERRUPTED":
            aborted = True
            break
        if status != "SUCCEEDED" and suite.stopOnFailure:
            print("[SUITE INFO] StopOnFailure is set, aborting remaining experiments")
            aborted = True
            break

        # --- Cool-down before the next one ---
        isLast = i == len(experiments) - 1
        if not isLast:
            cooldown = (
                exp.cooldownSeconds
                if exp.cooldownSeconds is not None
                else suite.cooldownSeconds
            )
            if cooldown > 0:
                print(f"[SUITE INFO] Cooling down for {cooldown}s ...")
                try:
                    time.sleep(cooldown)
                except KeyboardInterrupt:
                    print("[SUITE INFO] Ctrl-C during cool-down, aborting suite")
                    aborted = True
                    break

    # --- Restore the branch we started on ---
    if suite.restoreBranch:
        try:
            restoreGeneratedFiles(suite.workDir)
            if gitCurrentBranch(suite.workDir) != startingBranch:
                print(f"\n[SUITE INFO] Restoring original branch '{startingBranch}'")
                runGit(suite.workDir, ["checkout", startingBranch])
        except Exception as e:
            print(f"[SUITE WARNING] Could not restore original branch: {e}")

    # --- Summary ---
    print("\n\n============================================================")
    print("                      Suite summary")
    print("============================================================")
    for exp, status, elapsed, attempt in results:
        attemptNote = f"({attempt} attempts)" if attempt > 1 else ""
        print(
            f"  {status:<32} {elapsed:>5}s  {exp.name}  -> "
            f"{exp.resultKeyword} {attemptNote}"
        )
    notRun = len(experiments) - len(results)
    if notRun > 0:
        print(f"  {notRun} experiment(s) were not run")
    print(f"\n  Logs: {logDir}")
    print(f"  Results: {os.path.join(scriptsDir, 'results')}")
    print("============================================================")

    # Persist the summary next to the logs
    summaryPath = os.path.join(logDir, "summary.json")
    with open(summaryPath, "w") as f:
        json.dump(
            {
                "suite": os.path.abspath(suitePath),
                "startedAt": timestamp,
                "aborted": aborted,
                "results": [
                    {
                        "name": exp.name,
                        "configPath": exp.configPath,
                        "resultKeyword": exp.resultKeyword,
                        "branch": exp.branch,
                        "status": status,
                        "elapsedSeconds": elapsed,
                        "attempts": attempt,
                    }
                    for exp, status, elapsed, attempt in results
                ],
            },
            f,
            indent=2,
        )

    # Emit a ready-to-run suite JSON covering everything that did not succeed,
    # including experiments the suite never reached after an abort
    writeRerunSuiteFile(suite, experiments, results, logDir)

    allSucceeded = results and all(s == "SUCCEEDED" for _, s, _, _ in results)
    return 0 if (allSucceeded and not aborted) else 1


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Run several experiments back-to-back from a suite config"
    )
    parser.add_argument("suiteConfigPath", help="Path to the suite JSON config")
    parser.add_argument(
        "--only",
        default="",
        help="Comma-separated experiment names to run (subset of the suite)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate the suite and print the plan without running anything",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Also echo runExperiment.py's output to the console (it is always "
        "written to the per-experiment log file)",
    )
    args = parser.parse_args()

    StreamChildOutput = args.verbose

    only = [n.strip() for n in args.only.split(",") if n.strip()]

    try:
        sys.exit(runSuite(args.suiteConfigPath, only, args.dry_run))
    except Exception as e:
        print(f"{e}")
        sys.exit(1)
