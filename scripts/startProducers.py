import sys
import signal
from utils import *
from stopProducers import stopProducers


# Start a group of producer processes on configured producer nodes
def startNexmarkProducers(
    producerIPs: list[str], configFile: str, workDir: str, livePrint: bool = True
) -> None:

    numProducers = len(producerIPs)
    if numProducers == 0:
        print("[Producer ERROR] Producer IPs list is empty")
        sys.exit(1)
    print(f"[Producer INFO] Total number of Nexmark producers to start: {numProducers}")

    # Start Nexmark producers on all producer nodes. The replica id starts from 0.
    remoteProducerProcesses = []
    for i, ip in enumerate(producerIPs):
        cfgFilePath = f"{workDir}/scripts/{configFile}"
        cmd = f"cd {workDir} && ./bin/nexmarkKafkaProducer {cfgFilePath} {i}"
        print(f"[Producer INFO] Starting Nexmark producer {i} on {ip} ...")
        remoteProcess = runSSHCmdAsync(ip, cmd, livePrint=livePrint)
        remoteProducerProcesses.append(remoteProcess)

    # Block and wait on all remote producer processes
    for ip, p in zip(producerIPs, remoteProducerProcesses):
        exitCode = p.wait()
        print(
            f"[Producer INFO] Remote Nexmark producer on {ip} exited with code {exitCode}"
        )
    print("[Producer INFO] All Nexmark producers terminated!")


# Start a group of producer processes on configured producer nodes
def startFileReaderProducers(
    producerIPs: list[str], configFile: str, workDir: str, livePrint: bool = True
) -> None:

    numProducers = len(producerIPs)
    if numProducers == 0:
        print("[Producer ERROR] Producer IPs list is empty")
        sys.exit(1)
    print(f"[Producer INFO] Total number of Nexmark producers to start: {numProducers}")

    # Start FileReader producers on all producer nodes
    remoteProducerProcesses = []
    for i, ip in enumerate(producerIPs):
        cfgFilePath = f"{workDir}/scripts/{configFile}"
        cmd = f"cd {workDir} && ./bin/fileReaderKafkaProducer {cfgFilePath} {i}"
        print(f"[Producer INFO] Starting FileReader producer {i} on {ip} ...")
        remoteProcess = runSSHCmdAsync(ip, cmd, livePrint=livePrint)
        remoteProducerProcesses.append(remoteProcess)

    # Block and wait on all remote producer processes
    for ip, p in zip(producerIPs, remoteProducerProcesses):
        exitCode = p.wait()
        print(
            f"[Producer INFO] Remote fileReader producer on {ip} exited with code {exitCode}"
        )
    print("[Producer INFO] All fileReader producers terminated!")


if __name__ == "__main__":

    # Get producer type andconfig JSON file path from command line argument
    if len(sys.argv) != 3:
        print("Usage: python3 startProducers.py <producerType> <configFilePath>")
        sys.exit(1)
    producerType = sys.argv[1]
    configFile = sys.argv[2]

    # Read producer config info
    configMap = readJsonFile(configFile)

    # Get all producer IPs (allow duplicate IPs for multiple producers on the same node)
    producerIPs = getJsonConfigByKey(configMap, "ProducerIPs")
    workDir = getJsonConfigByKey(configMap, "WorkDir")

    # Register ctrl-c signal handler to stop all producers
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: stopProducers(producerIPs, workDir),
    )

    # We only allow 2 types of producers for now: "nexmark" and "fileReader"
    try:
        if producerType == "nexmark":
            startNexmarkProducers(producerIPs, configFile, workDir)
        elif producerType == "fileReader":
            startFileReaderProducers(producerIPs, configFile, workDir)
        else:
            print(
                "[Producer ERROR] Unsupported producer type. Supported types: nexmark, fileReader"
            )
            sys.exit(1)
    except Exception as e:
        print(f"[Producer ERROR] Exception when starting producers: {e}")
        stopProducers(producerIPs, workDir)
