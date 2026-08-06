import sys
from utils import *


# Stop Kafka cluster on multiple nodes and clear their data
def stopKafkaCluster(kafkaNodeIPs: list[str], workDir: str) -> None:

    print("[Kafka INFO] Stopping the Kafka cluster ...")
    numKafkaNodes = len(kafkaNodeIPs)
    print(f"[Kafka INFO] Total number of Kafka nodes to stop: {numKafkaNodes}")

    # Stop Kafka cluster and clear data in all nodes
    for ip in kafkaNodeIPs:
        stopKafkaNode(ip, workDir)
    print("[Kafka INFO] Kafka cluster stopped and data cleared!")


# Synchronously stop Kafka node on the given IP and clear its data. If any step
# fails e.g. a dir does not exist, it will continue to the next cmd to clean up
def stopKafkaNode(ip: str, workDir: str) -> None:
    print(f"[Kafka INFO] Stopping Kafka node on {ip} ...")

    # First stop the remote kafka process
    stopKafkaCmd = f"cd {workDir}/scripts/kafka/kafka_2.13-3.9.0/bin && sudo ./kafka-server-stop.sh"
    runSSHCmdSync(ip, stopKafkaCmd)

    # Clear data regardless of stopKafkaCmd result
    runSSHCmdSync(
        ip,
        f"cd {workDir}/scripts/kafka/kafka_2.13-3.9.0 && rm -rf logs",
    )
    runSSHCmdSync(
        ip,
        f"sudo rm -rf /tmp/kraft-combined-logs",
    )
    print(f"[Kafka INFO] Kafka node on {ip} stopped and data cleared.")


if __name__ == "__main__":

    # Get config JSON file path from command line argument
    if len(sys.argv) != 2:
        print("Usage: python3 stopCluster.py <configFilePath>")
        sys.exit(1)
    configFile = sys.argv[1]

    # Read Kafka cluster config info
    configMap = readJsonFile(configFile)
    workDir = getJsonConfigByKey(configMap, "WorkDir")
    kafkaNodeIPs = getJsonConfigByKey(configMap, "KafkaClusterIPs")

    stopKafkaCluster(kafkaNodeIPs, workDir)
