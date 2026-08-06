import sys
from utils import *


# Stop all producer processes on configured producer nodes
def stopProducers(producerIPs: list[str], workDir: str) -> None:

    numProducers = len(producerIPs)
    if numProducers == 0:
        print("[Producer ERROR] Producer IPs list is empty")
        raise Exception("Producer IPs list is empty")
    print(f"[Producer INFO] Total number of Nexmark producers to stop: {numProducers}")

    # Convert producer ip list to set
    producerIPsSet = set(producerIPs)

    allSuccessStop = True
    for ip in producerIPsSet:
        stopped = stopProducerNode(ip, workDir)
        if not stopped:
            allSuccessStop = False
            print(f"[Producer ERROR] Failed to stop Nexmark producers on {ip}")
    if allSuccessStop:
        print(f"[Producer INFO] All Nexmark producers stopped successfully.")
    else:
        print(f"[Producer ERROR] Some Nexmark producers failed to stop.")


def stopProducerNode(ip: str, workDir: str) -> bool:

    cmd = f"cd {workDir}/scripts/producer && python3 stopProducerNode.py"
    print(f"[Producer INFO] Stopping Nexmark producers on {ip} ...")
    procRes = runSSHCmdSync(ip, cmd, livePrint=True)
    if procRes.returncode != 0:
        return False
    return True


if __name__ == "__main__":

    # Get config JSON file path from command line argument
    if len(sys.argv) != 2:
        print("Usage: python3 stopProducers.py <configFilePath>")
        sys.exit(1)
    configFile = sys.argv[1]

    # Read producer config info
    configMap = readJsonFile(configFile)

    # Get all producer IPs (there could be duplicate IPs)
    producerIPs = getJsonConfigByKey(configMap, "ProducerIPs")
    workDir = getJsonConfigByKey(configMap, "WorkDir")

    stopProducers(producerIPs, workDir)
