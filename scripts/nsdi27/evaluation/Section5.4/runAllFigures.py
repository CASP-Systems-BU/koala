import argparse
import os
import subprocess
import sys
import time


"""
Regenerate every figure in Section5.4.

Runs each figure script as its own process and prints one line per script plus a
summary. Each script is run with its own directory as the working directory, so
the `plt.savefig("Figure14.pdf")` calls land next to the script that produced
them instead of wherever this runner was invoked from.

Handles both evaluation layouts, so this file can be dropped into any section:
  - flat, as here:      figure14.py, figure15.py, ... alongside util.py
  - Figure*/ subdirs:   as in Section5.2 (Figure6/, Figure8/)

`util.py` is a shared library, not a figure, so it is skipped - as is this file
and anything under __pycache__.

Usage:
    python3 runAllFigures.py                    # everything
    python3 runAllFigures.py --only figure15    # substring match on script name
    python3 runAllFigures.py --verbose          # show each script's own output
    python3 runAllFigures.py --list             # print the plan, run nothing

Exit code is 0 only when every script that ran succeeded.
"""


# Files that live alongside the figure scripts but are not figures themselves.
# The driver names are listed explicitly as well as this file's own name, so a
# copy of this runner never mistakes another section's driver for a figure
NotFigures = {
    "util.py",
    "runAllFigures.py",
    "generateFigures.py",
    os.path.basename(__file__),
}

# Subdirectories with this prefix are searched when the layout is nested
FigureDirGlob = "Figure"


def figureDirs(baseDir: str) -> list[str]:
    """Directories to search, relative to baseDir.

    Returns the Figure*/ subdirectories when the layout is nested, otherwise
    [""] meaning baseDir itself (the flat layout used in this section).
    """
    nested = [
        entry
        for entry in sorted(os.listdir(baseDir))
        if entry.startswith(FigureDirGlob) and os.path.isdir(os.path.join(baseDir, entry))
    ]
    return nested if nested else [""]


def discoverScripts(baseDir: str) -> list[tuple[str, str]]:
    """Return [(figureDir, scriptName)] for every figure script, sorted.

    Sorted so a run is reproducible and the output is diffable between runs.
    """
    found = []
    for figDir in figureDirs(baseDir):
        searchDir = os.path.join(baseDir, figDir)
        for name in sorted(os.listdir(searchDir)):
            if not name.endswith(".py") or name in NotFigures:
                continue
            found.append((figDir, name))
    return found


def label_for(figureDir: str, scriptName: str) -> str:
    """'Figure6/evaluateAzure.py' when nested, plain 'figure14.py' when flat."""
    return os.path.join(figureDir, scriptName) if figureDir else scriptName


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
        description="Regenerate every figure in Section5.4"
    )
    parser.add_argument(
        "--only",
        default="",
        help="Run only scripts whose figure dir or file name contains this "
        "substring (case-insensitive), e.g. figure15",
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
    print(f"  Section5.4 figures: {len(scripts)} script(s)")
    print(f"  Base: {baseDir}")
    print("=" * 64)

    if args.list:
        for figDir, name in scripts:
            print(f"  {label_for(figDir, name)}")
        return 0

    results = []
    for figDir, name in scripts:
        label = label_for(figDir, name)
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
              f"{os.path.basename(failed[0][0])[:-3]} --verbose")

    # Where the PDFs ended up (each script writes next to itself)
    print("\n  Generated PDFs:")
    anyPdf = False
    for figDir in figureDirs(baseDir):
        searchDir = os.path.join(baseDir, figDir)
        for name in sorted(os.listdir(searchDir)):
            if name.endswith(".pdf"):
                anyPdf = True
                size = os.path.getsize(os.path.join(searchDir, name)) // 1024
                print(f"    {label_for(figDir, name)}  ({size} KB)")
    if not anyPdf:
        print("    none")

    print("=" * 64)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
