import sys
from utils import *


# Copy all needed files from local machine to all other machines
def syncRepo(allNodeIPs: list[str], workDir: str) -> None:

    print("\n============================================================")
    print("    [SYNC INFO] Starting SyncRepo to all remote machines...")
    print("============================================================")

    numNodes = len(allNodeIPs)
    if numNodes == 0:
        print("Error: AllNodeIPs is empty")
        raise Exception("Error: AllNodeIPs is empty")

    # Identify local machine IP
    localIPs = getLocalIP()

    for ip in allNodeIPs:
        if ip in localIPs:
            print(f"[SYNC INFO] {ip} is local machine, skipping sync.")
            continue
        syncToRemoteIp(ip, workDir)
    print("[SYNC INFO] SyncRepo completed for all remote machines.\n")


# Sync all needed files from local to a remote machine
def syncToRemoteIp(ip: str, workDir: str) -> None:

    # Create the work dir on the remote machine. Do nothing if it already exists
    runSSHCmdSync(ip, f"mkdir -p {workDir}")

    # Sync config.yaml
    cmdRes = runCmdSync(
        f"scp -r {workDir}/config.yaml {ip}:{workDir}",
        livePrint=False,
    )
    if cmdRes.returncode != 0:
        raise Exception("Error: see above stderr for details")

    # Sync executable bin dir
    cmdRes = runCmdSync(
        f"scp -r {workDir}/bin {ip}:{workDir}",
        livePrint=False,
    )
    if cmdRes.returncode != 0:
        raise Exception("Error: see above stderr for details")

    # Sync scripts dir
    cmdRes = runCmdSync(
        f"scp -r {workDir}/scripts {ip}:{workDir}",
        livePrint=False,
    )
    if cmdRes.returncode != 0:
        raise Exception("Error: see above stderr for details")


if __name__ == "__main__":

    # Get config JSON file path from command line argument
    if len(sys.argv) != 2:
        print("Usage: python3 syncRepo.py <configFilePath>")
        sys.exit(1)
    configFile = sys.argv[1]

    # Read Kafka cluster config info
    configMap = readJsonFile(configFile)
    workDir = getJsonConfigByKey(configMap, "WorkDir")
    allNodeIPs = getJsonConfigByKey(configMap, "AllNodeIPs")

    syncRepo(allNodeIPs, workDir)
