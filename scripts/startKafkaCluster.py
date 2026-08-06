import sys
from utils import *
from kafka import KafkaAdminClient
from stopKafkaCluster import stopKafkaCluster
import signal

# This script starts Kafka cluster on multiple nodes. If Kafka is not installed
# on the nodes, it will install Kafka first.
# Usage:
#   python startCluster.py <configFilePath>
# Example:
#   python startCluster.py ~/disaggregated-streaming/scripts/nexmarkJson/query1.json

# Time to wait after deploying Kafka cluster before checking its status
# 15s here is for 1st time kafka installation case, otherwise it should be much faster
KafkaDeployWarmupTime = 15  # seconds

# Time period to monitor the cluster health
ClusterHealthMonitorInterval = 10  # seconds


# Start Kafka cluster on multiple nodes
def startKafkaCluster(
    kafkaClusterIPs: list[str], workDir: str, printKafkaLog: bool = True
) -> None:

    print("\n============================================================")
    print("          [Kafka INFO] Start Kafka cluster...")
    print("============================================================")

    numKafkaNodes = len(kafkaClusterIPs)
    if numKafkaNodes == 0:
        raise ValueError("KafkaClusterIPs is empty")
    # There shouldn't be duplicate IPs in Kafka cluster node list
    if len(set(kafkaClusterIPs)) != numKafkaNodes:
        raise ValueError("Duplicate IPs found in KafkaClusterIPs")
    print(f"[Kafka INFO] Total number of Kafka nodes to start: {numKafkaNodes}")

    # Deploy and start Kafka cluster
    nodeIPListStr = ",".join(kafkaClusterIPs)
    cmd = f"cd {workDir}/scripts/kafka && ./deployKafkaNode.sh {nodeIPListStr}"
    remoteKafkaProcesses = []
    for ip in kafkaClusterIPs:
        print(f"[Kafka INFO] Starting Kafka node on {ip} ...")
        remoteProcess = runSSHCmdAsync(ip, cmd, livePrint=printKafkaLog)
        remoteKafkaProcesses.append(remoteProcess)

    print("[Kafka INFO] Waiting for Kafka cluster to start ...")
    sleepWithLogging(KafkaDeployWarmupTime, printIntervalSeconds=5)

    # Monitor the cluster health in the background
    monitorProc = runFuncAsync(monitorClusterHealth, kafkaClusterIPs, workDir)
    print("[Kafka INFO] Kafka cluster started ...")

    # Block and wait on all remote Kafka processes. They will return when Kafka
    # cluster is stopped by stopKafkaCluster() asynchronously
    for ip, p in zip(kafkaClusterIPs, remoteKafkaProcesses):
        exitCode = p.wait()
        print(f"[Kafka INFO] Remote Kafka process on {ip} exited with code {exitCode}")

    # Stop the cluster monitoring process
    monitorProc.terminate()
    monitorProc.join()
    print("[Kafka INFO] All remote Kafka nodes terminated - cluster stopped!")


# Periodically monitor the health of the Kafka cluster and terminate the whole
# cluster if any node goes down
def monitorClusterHealth(kafkaClusterIPs: list[str], workDir: str) -> None:

    # Let the child process return back to default signal handling to avoid
    # duplicate stopKafkaCluster() calls
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    print("[Kafka INFO] Started monitoring Kafka cluster health ...")
    # Construct the bootstrap server list
    bootstrapServers = [f"{ip}:9092" for ip in kafkaClusterIPs]

    while True:
        try:
            adminClient = KafkaAdminClient(bootstrap_servers=bootstrapServers)
            clusterInfo = adminClient.describe_cluster()
            aliveBrokers = clusterInfo["brokers"]
            if len(aliveBrokers) != len(kafkaClusterIPs):
                print(
                    f"[Kafka Cluster ERROR] Kafka cluster node count mismatch: "
                    f"{len(aliveBrokers)} alive (expected {len(kafkaClusterIPs)})"
                )
                print("[Kafka Cluster ERROR] Current alive brokers:")
                for broker in aliveBrokers:
                    print(
                        f"  Broker ID: {broker['node_id']}, Host: {broker['host']}, Port: {broker['port']}"
                    )
                stopKafkaCluster(kafkaClusterIPs, workDir)
                return
            else:
                time.sleep(ClusterHealthMonitorInterval)

        except Exception as e:
            print(f"[Kafka Cluster ERROR] Exception when checking cluster health: {e}")
            stopKafkaCluster(kafkaClusterIPs, workDir)
            return


if __name__ == "__main__":

    # Get config JSON file path from command line argument
    if len(sys.argv) != 2:
        print("Usage: python3 startCluster.py <configFilePath>")
        sys.exit(1)
    configFile = sys.argv[1]

    # Read Kafka cluster config info
    configMap = readJsonFile(configFile)
    kafkaClusterIPs = getJsonConfigByKey(configMap, "KafkaClusterIPs")
    workDir = getJsonConfigByKey(configMap, "WorkDir")

    # Register ctrl-c signal handler to stop the Kafka cluster
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: stopKafkaCluster(kafkaClusterIPs, workDir),
    )

    try:
        startKafkaCluster(kafkaClusterIPs, workDir, printKafkaLog=False)
    except Exception as e:
        print(f"[Kafka script ERROR] Exception when starting Kafka cluster: {e}")
        stopKafkaCluster(kafkaClusterIPs, workDir)
