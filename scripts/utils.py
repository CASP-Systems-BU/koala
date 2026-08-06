import os
import sys
import json
import subprocess
import time
from multiprocessing import Process
import yaml
import psutil
import socket

# Util functions for all python scripts

###############################################################################
#                              JSON config utils
###############################################################################


# Read JSON config file and return as a map
def readJsonFile(configFile: str) -> dict:
    with open(os.path.expanduser(configFile), "r") as f:
        configMap = json.load(f)
    return configMap


# Read a key from JSON config map, exit if key not found
def getJsonConfigByKey(configMap: dict, key: str) -> any:
    if key not in configMap:
        print(f"Error: key {key} not found in config file.")
        sys.exit(1)
    return configMap[key]


# Read a key from JSON config map, return defaultValue if key not found
def getJsonConfigByKeyWithDefault(configMap: dict, key: str, defaultValue: any) -> any:
    if key not in configMap:
        return defaultValue
    return configMap[key]


###############################################################################
#                            Command execution utils
###############################################################################


# Synchronously run a SSH command and wait until it finishes. Return the command
# execution results
# [WARNING] If local Popen child process is killed, the remote process is NOT
# killed automatically. User needs to manually kill it if needed
def runSSHCmdSync(
    ip: str, cmd: str, livePrint: bool = True
) -> subprocess.CompletedProcess:

    exeCmd = f"ssh {ip} '{cmd}'"
    print(exeCmd)
    if livePrint:
        # Print stdout and stderr live to local terminal
        return subprocess.run(exeCmd, shell=True)
    else:
        # Throw away stdout but still print out stderr
        return subprocess.run(
            exeCmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=sys.stderr,
        )


# Run and track a SSH command in a child process. Return the child process
# handle immediately for process tracking (non blocking).The child process
# establishes the SSH connection and runs the command on the remote machine
# until the command finishes.
# [WARNING] If local Popen child process is killed, the remote process is NOT
# killed automatically. User needs to manually kill it if needed
def runSSHCmdAsync(
    ip: str, cmd: str, livePrint: bool = True, isWarmUPRun: bool = False
) -> subprocess.Popen:

    exeCmd = f"ssh {ip} '{cmd}'"
    print(exeCmd)
    if livePrint:
        # Print stdout and stderr live to local terminal
        return subprocess.Popen(exeCmd, shell=True, start_new_session=isWarmUPRun)
    else:
        # Throw away stdout but still print out stderr
        return subprocess.Popen(
            exeCmd,
            shell=True,
            start_new_session=isWarmUPRun,
            stdout=subprocess.DEVNULL,
            stderr=sys.stderr,
        )


# Execute cmd locally in a blocking way, wait until cmd finishes
def runCmdSync(cmd: str, livePrint: bool = True) -> subprocess.CompletedProcess:

    print(cmd)
    if livePrint:
        # Print stdout and stderr live to local terminal
        return subprocess.run(cmd, shell=True)
    else:
        # Throw away stdout but still print out stderr
        return subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=sys.stderr,
        )


# Execute cmd asynchronously in the background, return the child process handle
# immediately for process tracking (non blocking)
def runCmdAsync(cmd: str, livePrint: bool = True) -> subprocess.Popen:

    print(cmd)
    if livePrint:
        # Print stdout and stderr live to local terminal - do not capture them
        return subprocess.Popen(cmd, shell=True)
    else:
        # Throw away stdout but still print out stderr
        return subprocess.Popen(
            cmd,
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=sys.stderr,
        )


###############################################################################
#                                 Other utils
###############################################################################


# Sleep for given number of seconds, print message every configured interval
def sleepWithLogging(
    totalSleepSeconds: int, printIntervalSeconds: int = 10, printLog: bool = True
) -> None:
    elapsed = 0
    while elapsed < totalSleepSeconds:
        sleepTime = min(printIntervalSeconds, totalSleepSeconds - elapsed)
        time.sleep(sleepTime)
        elapsed += sleepTime
        if printLog:
            print(f"[Sleep] Slept for {elapsed}/{totalSleepSeconds} seconds")


# Start an async background process to execute a py function. This is different
# from helper functinos above to run shell commands
def runFuncAsync(func, *args) -> Process:
    p = Process(target=func, args=args)
    p.start()
    return p


# Find the Coordinator process locally and terminate it. This will automatically
# terminate all workers as well
def shutdownCoordinator() -> None:
    cnt = 0
    for proc in psutil.process_iter(["pid", "name"]):
        if "coordinator" in proc.info["name"]:
            # Terminate the process
            print(
                f"[CLEANUP INFO] Terminating process {proc.info['name']} with PID {proc.info['pid']}"
            )
            proc.terminate()
            proc.wait()
            cnt += 1
    if cnt == 0:
        print("[CLEANUP WARNING] No coordinator process found on this node.")
    else:
        print(f"[CLEANUP INFO] Terminated {cnt} coordinator process(es).")


# Return all ipv4 addresses of the local machine (e.g. private and public IPs)
def getLocalIP() -> set[str]:
    ips = set()
    for addrs in psutil.net_if_addrs().values():
        for addr in addrs:
            if addr.family == socket.AF_INET and addr.address != "127.0.0.1":
                ips.add(addr.address)
    return ips


# Replace a field in the YAML config file with the given value
def updateConfigYamlFile(configYamlFile: str, field: str, value: any) -> None:
    print(f"[config.yaml] Updating field {field} to {value} in config.yaml")
    with open(os.path.expanduser(configYamlFile), "r") as file:
        config = yaml.safe_load(file)
    config[field] = value
    with open(os.path.expanduser(configYamlFile), "w") as file:
        yaml.dump(config, file, default_flow_style=False, sort_keys=False)


# Update local config.yaml file with local private IP as coordinator address
def updateCoordinatorAddrInConfigYaml(
    configYamlFile: str, allNodeIPs: list[str]
) -> None:
    localIPs = getLocalIP()
    coordinatorIP = None
    for ip in allNodeIPs:
        if ip in localIPs:
            coordinatorIP = ip
            break
    if coordinatorIP is None:
        raise Exception("[ERROR] Could not find local machine IP in allNodeIPs list.")

    # We need to update 3 fields in config.yaml for coordinator address
    print(
        f"[CONFIG INFO] Updating coordinator address to {coordinatorIP} in {configYamlFile}"
    )
    updateConfigYamlFile(configYamlFile, "CoordinatorAddr", f"{coordinatorIP}:8888")
    updateConfigYamlFile(configYamlFile, "CoordinatorAPIAddr", f"{coordinatorIP}:9999")
    updateConfigYamlFile(
        configYamlFile, "CoordinatorMetricCollectorAddr", f"{coordinatorIP}:8887"
    )


# Overwrite custom task placement file
def writeCustomTaskPlacementTxt(workDir: str, placementLines: list[str]) -> None:

    if not placementLines:
        raise Exception("[ERROR] placementLines is empty, cannot write placement file.")

    workDirExpanded = os.path.expanduser(workDir)
    placementRelPath = os.path.join("scripts", "taskPlacement", "customPlacement.txt")
    placementFilePath = os.path.join(workDirExpanded, placementRelPath)

    # Ensure directory exists
    os.makedirs(os.path.dirname(placementFilePath), exist_ok=True)
    with open(placementFilePath, "w") as f:
        for line in placementLines:
            print("   " + line)
            # Trim trailing newlines and write exactly one per entry
            f.write(line.rstrip("\n") + "\n")
    print("[Custom Placement INFO] customPlacement.txt updated")


class Reconfig:
    def __init__(
        self,
        triggerTimeSeconds: int,
        reconfigType: str,
        targetOperator: str,
        targetParallelism: int,
    ):
        self.triggerTimeSeconds = triggerTimeSeconds
        self.reconfigType = reconfigType
        self.targetOperator = targetOperator
        self.targetParallelism = targetParallelism


# Validate the config json input
def validateConfigJsonInput(
    queryName: str,
    allNodeIPs: list[str],
    kafkaClusterIPs: list[str],
    producerIPs: list[str],
    workerIPs: list[str],
    remotePebbleAddrs: list[str],
    stateBackendType: str,
    reconfigProtocol: str,
    lazyProtocolVersion: str,
    configMap: dict,
    partitionPolicy: str,
    isWarmUpRun: bool,
    reconfigurations: list[Reconfig],
    initialCustomTaskPlacement: list[str],
) -> None:

    # If this is a warm-up run, (i) there shouldn't be any reconfigurations,
    # (ii) must be custom placement
    if isWarmUpRun:
        if reconfigurations:
            raise Exception("[ERROR] No reconfigurations allowed for warm-up runs.")
        if not initialCustomTaskPlacement:
            raise Exception(
                "[ERROR] InitialCustomTaskPlacement must be provided for warm-up runs."
            )

    # All IP list shouldn't be empty
    if len(allNodeIPs) == 0:
        raise Exception("[ERROR] AllNodeIPs list is empty.")
    if len(kafkaClusterIPs) == 0:
        raise Exception("[ERROR] KafkaClusterIPs list is empty.")
    if len(producerIPs) == 0:
        raise Exception("[ERROR] ProducerIPs list is empty.")
    if len(workerIPs) == 0:
        raise Exception("[ERROR] WorkerIPs list is empty.")

    # All IPs cannot have duplicates
    if len(allNodeIPs) != len(set(allNodeIPs)):
        raise Exception("[ERROR] AllNodeIPs list has duplicate IPs.")

    # Kafka cluster IPs cannot have duplicates
    if len(kafkaClusterIPs) != len(set(kafkaClusterIPs)):
        raise Exception("[ERROR] KafkaClusterIPs list has duplicate IPs.")

    # All IPs should include all roles
    allNodeIPsSet = set(allNodeIPs)
    kafkaClusterIPsSet = set(kafkaClusterIPs)
    producerIPsSet = set(producerIPs)
    workerIPsSet = set(workerIPs)
    if not kafkaClusterIPsSet.issubset(allNodeIPsSet):
        raise Exception(
            "[ERROR] Some KafkaClusterIPs are not included in AllNodeIPs list."
        )
    if not producerIPsSet.issubset(allNodeIPsSet):
        raise Exception("[ERROR] Some ProducerIPs are not included in AllNodeIPs list.")
    if not workerIPsSet.issubset(allNodeIPsSet):
        raise Exception("[ERROR] Some WorkerIPs are not included in AllNodeIPs list.")

    if remotePebbleAddrs:
        # Entry in remotePebbleAddrs should be in format host:port
        for addr in remotePebbleAddrs:
            if ":" not in addr:
                raise Exception(
                    "[ERROR] RemotePebbleAddrs must be in host:port format."
                )
        remotePebbleIPsSet = set(addr.split(":")[0] for addr in remotePebbleAddrs)
        if not remotePebbleIPsSet.issubset(allNodeIPsSet):
            raise Exception(
                "[ERROR] Some RemotePebble IPs are not included in AllNodeIPs list."
            )

    # If query type is Nexmark, validate Nexmark specific configs
    if queryName.startswith("nexmark_"):

        # ConfigMap should include "NexmarkLogicalSourceConfigs" and its size
        # should be either 1 or 2
        if "NexmarkLogicalSourceConfigs" not in configMap:
            raise Exception(
                "[ERROR] ConfigMap is missing 'NexmarkLogicalSourceConfigs'."
            )
        if not (1 <= len(configMap["NexmarkLogicalSourceConfigs"]) <= 2):
            raise Exception(
                "[ERROR] 'NexmarkLogicalSourceConfigs' size must be 1 or 2."
            )

    # Validate reconfig protocol
    allowedReconfigProtocols = ["stop-and-restart", "lazy"]
    if reconfigProtocol not in allowedReconfigProtocols:
        raise Exception(
            f"[ERROR] ReconfigProtocol '{reconfigProtocol}' is not allowed. Allowed values: {allowedReconfigProtocols}"
        )

    # Validate lazy protocol version if lazy protocol is selected
    allowedLazyProtocolVersions = [
        "basic",
        "optimized",
        "no-migration",
        "drrs",
        "by-key",
    ]
    if lazyProtocolVersion not in allowedLazyProtocolVersions:
        raise Exception(
            f"[ERROR] LazyProtocolVersion '{lazyProtocolVersion}' is not allowed. Allowed values: {allowedLazyProtocolVersions}"
        )

    # Validate state backend type
    allowedStateBackendTypes = [
        "pebble",
        "tikv",
        "memory",
        "preLoadedMemory",
        "remote-pebble",
    ]
    if stateBackendType not in allowedStateBackendTypes:
        raise Exception(
            f"[ERROR] StateBackendType '{stateBackendType}' is not allowed. Allowed values: {allowedStateBackendTypes}"
        )

    # If remote pebble backend is selected, remote pebble addrs must be provided
    if stateBackendType == "remote-pebble":
        if not remotePebbleAddrs:
            raise Exception(
                "[ERROR] RemotePebbleAddrs must be set for remote-pebble backend."
            )

    # Validate partition policy
    allowedPartitionPolicies = [
        "consistent-hashing",
        "consistent-hashing-v2",
        "uniform",
        "consistent-even",
    ]
    if partitionPolicy not in allowedPartitionPolicies:
        raise Exception(
            f"[ERROR] PartitionPolicy '{partitionPolicy}' is not allowed. Allowed values: {allowedPartitionPolicies}"
        )


# Parse reconfiguration settings from config map
def parseReconfigurationSettings(configMap: dict) -> list[Reconfig]:

    reconfigurations = []
    reconfigJsonList = getJsonConfigByKeyWithDefault(configMap, "Reconfigurations", [])
    if len(reconfigJsonList) == 0:
        return reconfigurations

    for reconfigJson in reconfigJsonList:
        triggerTimeSeconds = getJsonConfigByKey(reconfigJson, "TriggerTimeSeconds")
        reconfigType = getJsonConfigByKey(reconfigJson, "Type")
        targetOperator = getJsonConfigByKey(reconfigJson, "TargetOperator")
        targetParallelism = getJsonConfigByKey(reconfigJson, "TargetParrallelism")

        # Validate target parallelism
        if targetParallelism <= 1:
            raise Exception(
                f"[ERROR] TargetParrallelism must be greater than 1, got {targetParallelism}."
            )

        # Validate reconfiguration type
        allowedReconfigTypes = ["scaleup", "scaledown", "task-migration"]

        if reconfigType not in allowedReconfigTypes:
            raise Exception(
                f"[ERROR] Reconfiguration type '{reconfigType}' is not allowed. Allowed values: {allowedReconfigTypes}"
            )
            # TODO: validate rescale input

        reconfig = Reconfig(
            triggerTimeSeconds,
            reconfigType,
            targetOperator,
            targetParallelism,
        )
        reconfigurations.append(reconfig)

    # Validate trigger time ordering and gaps
    for i in range(1, len(reconfigurations)):
        prevTime = reconfigurations[i - 1].triggerTimeSeconds
        currTime = reconfigurations[i].triggerTimeSeconds

        # Check increasing order
        if currTime < prevTime:
            raise Exception(
                f"[ERROR] TriggerTimeSeconds mustn't be in decreasing order. Found {prevTime} followed by {currTime}."
            )

    return reconfigurations


# Trigger reconfiguration action
def triggerReconfiguration(
    reconfig: Reconfig,
    workDir: str,
) -> None:
    print(
        f"[Reconfig INFO] Triggering reconfiguration - type: {reconfig.reconfigType}, operator: {reconfig.targetOperator}, target parallelism: {reconfig.targetParallelism}"
    )
    cmd = f"cd {workDir} && ./bin/client rescale {reconfig.targetOperator} {reconfig.targetParallelism}"
    clientProc = runCmdSync(cmd)
    if clientProc.returncode != 0:
        raise Exception("Client failed to submit the rescale command")


# Move the /data/metricDB directory to the scripts/results. This always happens
# locally at the coordinator node - so no ssh is needed
def moveMetricDBToResults(workDir: str, queryName: str, keyword: str) -> None:
    """Move <workDir>/data/metricDB to
    <workDir>/scripts/results/<queryName>_<keyword>/metricDB.

    If the destination folder <queryName>_<keyword> already exists, it will be
    removed (overridden) it
    """
    workDirExpanded = os.path.expanduser(workDir)
    src = os.path.join(workDirExpanded, "data", "metricDB")
    if not os.path.exists(src):
        print(f"[RESULTS WARNING] No metricDB found at {src}, skipping move.")
        return

    # Construct destination parent folder: scripts/results/<queryName>_<keyword>
    safe_keyword = str(keyword)
    folder_name = f"{queryName}_{safe_keyword}"
    exp_result_folder = os.path.join(workDirExpanded, "scripts", "results", folder_name)

    if os.path.exists(exp_result_folder):
        print(
            f"[RESULTS WARNING] Destination {exp_result_folder} already exists. Creating a new folder with timestamp"
        )
        # Append timestamp to new folder name to avoid clobbering existing results
        import datetime

        ts = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
        new_folder_name = f"{folder_name}_{ts}"
        exp_result_folder = os.path.join(
            workDirExpanded, "scripts", "results", new_folder_name
        )
        print(f"[RESULTS INFO] New results will be written to {exp_result_folder}")

    # Ensure the result folder path exists
    os.makedirs(exp_result_folder, exist_ok=True)

    # Final destination should be <exp_result_folder>/metricDB
    final_dest = os.path.join(exp_result_folder, "metricDB")

    # Move metricDB folder into final destination (this will place metricDB
    # inside the per-experiment folder)
    mv_cmd = f"mv {src} {final_dest}"
    print(f"[RESULTS INFO] Moving metricDB from {src} to {final_dest}")
    mv_proc = runCmdSync(mv_cmd)
    if mv_proc.returncode != 0:
        print(
            f"[RESULTS ERROR] Failed to move metricDB: command '{mv_cmd}' returned {mv_proc.returncode}"
        )
    else:
        print(f"[RESULTS INFO] metricDB successfully moved to {final_dest}")
