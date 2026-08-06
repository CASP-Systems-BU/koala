import argparse
import os
import subprocess
import sys
import time


"""
Regenerate every figure in Section5.2.

Walks the Figure*/ subdirectories, runs each figure script as its own process,
and prints one line per script plus a summary. Each script is run with its own
directory as the working directory, so the `plt.savefig("azure.pdf")` calls land
next to the script that produced them instead of wherever this runner was
invoked from.

`util.py` is a shared library, not a figure, so it is skipped - as is this file
and anything under __pycache__.

Usage:
    python3 runAllFigures.py                  # everything
    python3 runAllFigures.py --only Figure6   # one figure directory
    python3 runAllFigures.py --only azure     # substring match on script name
    python3 runAllFigures.py --verbose        # show each script's own output
    python3 runAllFigures.py --list           # print the plan, run nothing

Exit code is 0 only when every script that ran succeeded.
"""


# Files that live alongside the figure scripts but are not figures themselves
NotFigures = {"util.py", os.path.basename(__file__)}

# Only these directories are searched, in this order
FigureDirGlob = "Figure"


def discoverScripts(baseDir: str) -> list[tuple[str, str]]:
    """Return [(figureDir, scriptName)] for every figure script, sorted.

    Sorted so a run is reproducible and the output is diffable between runs.
    """
    found = []
    for entry in sorted(os.listdir(baseDir)):
        figDir = os.path.join(baseDir, entry)
        if not os.path.isdir(figDir) or not entry.startswith(FigureDirGlob):
            continue
        for name in sorted(os.listdir(figDir)):
            if not name.endswith(".py") or name in NotFigures:
                continue
            found.append((entry, name))
    return found


def runScript(
    baseDir: str, figureDir: str, scriptName: str, verbose: bool
) -> tuple[bool, int, str]:
    """Run one figure script. Returns (ok, elapsedSeconds, capturedOutput)."""

    workDir = os.path.join(baseDir, figureDir)
    start = time.time()

    proc = subprocess.run(
        [sys.executable, scriptName],
        cwd=workDir,
        capture_output=True,
        text=True,
    )
    elapsed = int(time.time() - start)
    output = (proc.stdout or "") + (proc.stderr or "")

    if verbose and output.strip():
        for line in output.rstrip().splitlines():
            print(f"        | {line}")

    return proc.returncode == 0, elapsed, output


def summariseFailure(output: str) -> str:
    """Pull the most useful single line out of a traceback.

    The exception line is what identifies the problem (a missing result folder,
    an operator name with no rows); the frames above it are noise here.
    """
    lines = [l.rstrip() for l in output.rstrip().splitlines() if l.strip()]
    if not lines:
        return "no output"
    for line in reversed(lines):
        if not line.startswith(" ") and ":" in line:
            return line
    return lines[-1]


def main() -> int:

    parser = argparse.ArgumentParser(
        description="Regenerate every figure in Section5.2"
    )
    parser.add_argument(
        "--only",
        default="",
        help="Run only scripts whose figure dir or file name contains this "
        "substring (case-insensitive), e.g. Figure6 or azure",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Echo each script's own stdout/stderr",
    )
    parser.add_argument(
        "--list",
        action="store_true",
        help="Print the scripts that would run, then exit",
    )
    args = parser.parse_args()

    baseDir = os.path.dirname(os.path.abspath(__file__))
    scripts = discoverScripts(baseDir)

    if args.only:
        needle = args.only.lower()
        scripts = [
            (d, s) for d, s in scripts if needle in d.lower() or needle in s.lower()
        ]

    if not scripts:
        print(f"[ERROR] No figure scripts matched (base: {baseDir})")
        return 1

    print("=" * 64)
    print(f"  Section5.2 figures: {len(scripts)} script(s)")
    print(f"  Base: {baseDir}")
    print("=" * 64)

    if args.list:
        for figDir, name in scripts:
            print(f"  {figDir}/{name}")
        return 0

    results = []
    for figDir, name in scripts:
        label = f"{figDir}/{name}"
        print(f"  running  {label} ...")
        ok, elapsed, output = runScript(baseDir, figDir, name, args.verbose)
        results.append((label, ok, elapsed, output))
        if ok:
            print(f"  OK       {label}  ({elapsed}s)")
        else:
            print(f"  FAILED   {label}  ({elapsed}s)")
            print(f"           {summariseFailure(output)}")

    failed = [r for r in results if not r[1]]

    print("\n" + "=" * 64)
    print("  Summary")
    print("=" * 64)
    for label, ok, elapsed, _ in results:
        print(f"  {'OK    ' if ok else 'FAILED'}  {elapsed:>3}s  {label}")

    print(f"\n  {len(results) - len(failed)}/{len(results)} succeeded")

    if failed:
        print("\n  Failures:")
        for label, _, _, output in failed:
            print(f"    {label}")
            print(f"      {summariseFailure(output)}")
        print("\n  Re-run one with --verbose for the full traceback, e.g.")
        print(f"    python3 {os.path.basename(__file__)} --only "
              f"{failed[0][0].split('/')[-1][:-3]} --verbose")

    # Where the PDFs ended up (each script writes next to itself)
    print("\n  Generated PDFs:")
    anyPdf = False
    for entry in sorted(os.listdir(baseDir)):
        figDir = os.path.join(baseDir, entry)
        if not os.path.isdir(figDir) or not entry.startswith(FigureDirGlob):
            continue
        for name in sorted(os.listdir(figDir)):
            if name.endswith(".pdf"):
                anyPdf = True
                size = os.path.getsize(os.path.join(figDir, name)) // 1024
                print(f"    {entry}/{name}  ({size} KB)")
    if not anyPdf:
        print("    none")

    print("=" * 64)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
