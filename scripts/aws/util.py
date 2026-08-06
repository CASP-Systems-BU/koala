import os
import sys

# Add parent directory to path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utils

# Directory of this script (scripts/aws/)
script_dir = os.path.dirname(os.path.abspath(__file__))


# Read instance.txt and return a list of IPs. If instanceFilePath is not provided,
# use the default path: scripts/aws/instance.txt
def getRemoteIPs(instanceFilePath: str = "") -> list:
    if not instanceFilePath:
        instanceFilePath = os.path.join(script_dir, "instance.txt")

    ips = []
    with open(instanceFilePath, "r") as f:
        for line in f:
            ip = line.strip()
            if not ip or ip.startswith("#"):
                continue
            if ip == "10.10.1.1":
                print("10.10.1.1 shouldn't appear in instance.txt")
                sys.exit(1)
            ips.append(ip)
    return ips


def run_on_all_nodes(ips, cmd, description):
    print("[IN PROGRESS] " + description + " ...")
    processes = []
    process_ips = []
    for ip in ips:
        # Use runSSHCmdAsync to run commands in parallel
        proc = utils.runSSHCmdAsync(ip, cmd, livePrint=False)
        processes.append(proc)
        process_ips.append(ip)

    exit_code = 0
    for proc, ip in zip(processes, process_ips):
        proc.wait()
        if proc.returncode != 0:
            exit_code = 1
            print(f"[ERROR DETAIL] Command failed on node {ip}")

    if exit_code != 0:
        print(f"[ERROR] One or more commands failed for: {description}")
        sys.exit(exit_code)
    else:
        print(f"[SUCCESS] {description}\n")
