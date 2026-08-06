import sys
from utils import *


# download python for nodes served as producer
def downloadPythonandJava(ProducerIPs: list[str]) -> None:
    setup_cmd = (
        "sudo apt update && "
        "sudo apt install -y python3-pip openjdk-11-jdk-headless iperf3 htop nload && "
        "python3 -m pip install psutil kafka-python"
    )
    for ip in ProducerIPs:
        print(f"=== Installing dependencies on {ip} ===")
        runSSHCmdSync(ip, setup_cmd, livePrint=True)
        print(f"=== Finished installing on {ip} ===\n")


if __name__ == "__main__":

    # Get config JSON file path from command line argument
    if len(sys.argv) != 2:
        print("Usage: python3 prepareNode.py <configFilePath>")
        sys.exit(1)
    configFile = sys.argv[1]

    # Read Kafka cluster config info
    configMap = readJsonFile(configFile)
    producerIPs = getJsonConfigByKey(configMap, "ProducerIPs")

    downloadPythonandJava(producerIPs)
