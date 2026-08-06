import sys
from utils import *
from syncRepo import syncRepo
from startKafkaCluster import startKafkaCluster
from startKafkaCluster import KafkaDeployWarmupTime
from stopProducers import stopProducers
from stopKafkaCluster import stopKafkaCluster
from multiprocessing import Process
import signal
from typing import Optional


"""
This is a basic run experiment template for Kafka queries. Prerequisites:

1. The working directory to run this script must be in the scripts/ folder
2. Must be at root node to run this script (with ssh access to all other nodes)

Usage: python3 runExperiment.py <configFilePath> <resultKeyword>
e.g. python3 runExperiment.py nexmarkJson/query1.json q1_lei_results
"""

# How aften to check the status of all components and apply timeline actions
ExecutionLoopInterval = 5  # in seconds

# Allowed query names
allowedQueries = {
    "nexmark_query1",
    "nexmark_query2",
    "nexmark_query3",
    "nexmark_query3_warmup",
    "nexmark_query4",
    "nexmark_query4_modified",
    "nexmark_query5",
    "nexmark_query6",
    "nexmark_query6_modified",
    "nexmark_query7",
    "nexmark_query8",
    "taxi",
    "taxi_warmup",
    "taxi_skew",
    "twitch",
    "borg",
    "borg_warmup",
    "azure",
    "azure_warmup",
}


# Execute an experiment for a given query with the provided JSON config file.
# This is safe to be called multiple times for various experiments (sequentially)
def runExperiment(
    jsonPath: str,
    resultKeyword: str,
) -> None:

    ###########################################################################
    #                          Read and validate config
    ###########################################################################

    # Read the JSON config file
    configMap = readJsonFile(jsonPath)

    # Get query name and validate if it's supported
    queryName = getJsonConfigByKey(configMap, "QueryName")
    if queryName not in allowedQueries:
        print(f"Error: queryName must be one of {allowedQueries}")
        sys.exit(1)

    # Work directory
    workDir = getJsonConfigByKey(configMap, "WorkDir")

    # Full path for the config JSON file as input for other scripts - other
    # scripts could use different working directories
    jsonPath = f"{workDir}/scripts/{jsonPath}"

    # All nodes involved in the experiment
    allNodeIPs = getJsonConfigByKey(configMap, "AllNodeIPs")

    # Kafka broker nodes
    kafkaClusterIPs = getJsonConfigByKey(configMap, "KafkaClusterIPs")

    # Kafka producer nodes
    producerIPs = getJsonConfigByKey(configMap, "ProducerIPs")

    # Worker nodes
    workerIPs = getJsonConfigByKey(configMap, "WorkerIPs")

    # State backend type
    stateBackendType = getJsonConfigByKey(configMap, "StateBackendType")

    # Remote pebble nodes (optional)
    remotePebbleAddrs = getJsonConfigByKeyWithDefault(
        configMap, "RemotePebbleAddrs", []
    )

    # Total experiment duration
    totalRuntimeSeconds = getJsonConfigByKey(configMap, "TotalRuntimeSeconds")

    # Reconfig protocol
    reconfigProtocol = getJsonConfigByKey(configMap, "ReconfigProtocol")

    # Lazy protocol version
    lazyProtocolVersion = getJsonConfigByKey(configMap, "LazyProtocolVersion")

    # [Optional][lazy-by-key] How state of a cancelling task is migrated:
    # "fetch-on-demand" (only keys that are accessed) or "eventual" (gradually
    # piggyback extra keys on remote fetches until the task is drained).
    # None means the key is absent from the JSON - config.yaml is then left
    # untouched and whatever value it already holds is used
    cancellingTaskMigrationMode = getJsonConfigByKeyWithDefault(
        configMap, "LazyByKeyCancellingTaskMigrationMode", None
    )

    # [Optional][lazy-by-key] Extra keys piggybacked per batch during eventual
    # migration. -1 fetches all remaining keys at once. Only used when the mode
    # above is "eventual". None means absent - see above
    gradualMigrationBatchSize = getJsonConfigByKeyWithDefault(
        configMap, "LazyByKeyGradualMigrationBatchSize", None
    )

    # Validation for cancelling task migration config - only what was provided
    allowedMigrationModes = {"fetch-on-demand", "eventual"}
    if (
        cancellingTaskMigrationMode is not None
        and cancellingTaskMigrationMode not in allowedMigrationModes
    ):
        raise Exception(
            f"[ERROR] LazyByKeyCancellingTaskMigrationMode must be one of "
            f"{allowedMigrationModes}, got {cancellingTaskMigrationMode!r}"
        )
    if gradualMigrationBatchSize is not None and (
        not isinstance(gradualMigrationBatchSize, int)
        or (gradualMigrationBatchSize != -1 and gradualMigrationBatchSize < 1)
    ):
        raise Exception(
            f"[ERROR] LazyByKeyGradualMigrationBatchSize must be -1 (fetch all "
            f"at once) or a positive integer, got {gradualMigrationBatchSize!r}"
        )

    # Number of buckets
    numBuckets = getJsonConfigByKey(configMap, "NumBuckets")

    # Key partition policy
    partitionPolicy = getJsonConfigByKey(configMap, "PartitionPolicy")

    # Buffer size
    bufferSize = getJsonConfigByKey(configMap, "BufferSize")

    # Batch size
    batchSize = getJsonConfigByKey(configMap, "BatchSize")
    
    # WarmupDataFolder for key lookup table
    lookupTableWarmUpDataFolder = getJsonConfigByKeyWithDefault(configMap, "LookupTableWarmUpDataFolder", "")

    # [Optional] Identify if this is a warm up run
    isWarmUpRun = getJsonConfigByKey(configMap, "IsWarmUp")

    pebbleEnableConcurrentGetMany = getJsonConfigByKey(configMap, "PebbleEnableConcurrentGetMany")

    pebbleGetManyMaxConcurrency = getJsonConfigByKey(configMap, "PebbleGetManyMaxConcurrency")

    pebbleGetManyBatchSize = getJsonConfigByKey(configMap, "PebbleGetManyBatchSize")

    # If need to load warm-up data before starting the experiment
    useWarmUpData = getJsonConfigByKey(configMap, "LoadWarmUpData")
    if isWarmUpRun and useWarmUpData:
        raise Exception("[ERROR] IsWarmUp and LoadWarmUpData cannot both be true")

    # [Optional] If warm up data folder path is provided
    warmUpDataFolder = getJsonConfigByKeyWithDefault(configMap, "WarmUpDataFolder", "")
    if useWarmUpData and not warmUpDataFolder:
        raise Exception(
            "[ERROR] WarmUpDataFolder must be provided if LoadWarmUpData is true."
        )
    if isWarmUpRun and not warmUpDataFolder:
        raise Exception("[ERROR] WarmUpDataFolder must be provided for warm-up runs.")

    pebbleWarmUpDataFolder = ""
    if (isWarmUpRun or useWarmUpData) and warmUpDataFolder:
        pebbleWarmUpDataFolder = getJsonConfigByKey(warmUpDataFolder, "Pebble")

    # [Optional] Reconfiguration settings. If list empty, no reconfiguration
    # will be applied
    reconfigurations = parseReconfigurationSettings(configMap)

    # [Optional] Set initial custom task placement. If list empty, use random
    initialCustomTaskPlacement = getJsonConfigByKeyWithDefault(
        configMap, "InitialCustomTaskPlacement", []
    )

     # [Optional] EnablePendingBatchTimeout. Default value is true
    enablePendingBatchTimeout =  getJsonConfigByKeyWithDefault(configMap, "EnablePendingBatchTimeout", False)

    # Validate the JSON input
    validateConfigJsonInput(
        queryName,
        allNodeIPs,
        kafkaClusterIPs,
        producerIPs,
        workerIPs,
        remotePebbleAddrs,
        stateBackendType,
        reconfigProtocol,
        lazyProtocolVersion,
        configMap,
        partitionPolicy,
        isWarmUpRun,
        reconfigurations,
        initialCustomTaskPlacement,
    )

    # Extract remote pebble IPs for clean up
    remotePebbleIPs = []
    if remotePebbleAddrs:
        remotePebbleIPs = list(set(addr.split(":")[0] for addr in remotePebbleAddrs))

    ###########################################################################
    #                       Register the signal handler
    ###########################################################################

    # Register Ctrl+C handler to clean up on interrupt
    signal.signal(
        signal.SIGINT,
        lambda sig, frame: cleanUp(
            producerIPs,
            kafkaClusterIPs,
            allNodeIPs,
            workDir,
            remotePebbleIPs,
            not isWarmUpRun),
    )

    ###########################################################################
    #                             Run the experiment
    ###########################################################################

    try:
        print(f"[INFO] Starting experiment for query {queryName} ...")
        runExperimentImpl(
            queryName,
            jsonPath,
            workDir,
            isWarmUpRun,
            allNodeIPs,
            kafkaClusterIPs,
            producerIPs,
            workerIPs,
            totalRuntimeSeconds,
            reconfigProtocol,
            lazyProtocolVersion,
            numBuckets,
            partitionPolicy,
            bufferSize,
            batchSize,
            stateBackendType,
            useWarmUpData,
            pebbleWarmUpDataFolder,
            lookupTableWarmUpDataFolder,
            resultKeyword,
            reconfigurations,
            initialCustomTaskPlacement,
            remotePebbleAddrs,
            remotePebbleIPs,
            pebbleEnableConcurrentGetMany,
            pebbleGetManyMaxConcurrency,
            pebbleGetManyBatchSize,
            enablePendingBatchTimeout,
            cancellingTaskMigrationMode,
            gradualMigrationBatchSize,
        )
    except Exception as e:
        print(f"[ERROR] Experiment failed with exception: {e}")
        cleanUp(
            producerIPs,
            kafkaClusterIPs,
            allNodeIPs,
            workDir,
            remotePebbleIPs,
        )

    ###########################################################################
    #                       Unregister the signal handler
    ###########################################################################

    # Let the process return back to default signal handling since the clean up
    # is already executed
    signal.signal(signal.SIGINT, signal.SIG_DFL)


def runExperimentImpl(
    queryName: str,
    jsonPath: str,
    workDir: str,
    isWarmUpRun: bool,
    allNodeIPs: list[str],
    kafkaClusterIPs: list[str],
    producerIPs: list[str],
    workerIPs: list[str],
    totalRuntimeSeconds: int,
    reconfigProtocol: str,
    lazyProtocolVersion: str,
    numBuckets: int,
    partitionPolicy: str,
    bufferSize: int,
    batchSize: int,
    stateBackendType: str,
    useWarmUpData: bool,
    pebbleWarmUpDataFolder: str,
    lookupTableWarmUpDataFolder: str,
    resultKeyword: str,
    reconfigurations: list[Reconfig],
    initialCustomTaskPlacement: list[str],
    remotePebbleAddrs: list[str],  # list of "ip:port" for remote pebble instances
    remotePebbleIPs: list[str],  # unique IPs for remote pebble instances
    pebbleEnableConcurrentGetMany: bool,
    pebbleGetManyMaxConcurrency: int,
    pebbleGetManyBatchSize: int,
    enablePendingBatchTimeout: bool,
    cancellingTaskMigrationMode: Optional[str],
    gradualMigrationBatchSize: Optional[int],
) -> None:

    ###########################################################################
    #               Clean up before experiment (not necessary)
    ###########################################################################

    # Clean up any existing components from previous runs
    cleanUp(producerIPs, kafkaClusterIPs, allNodeIPs, workDir, remotePebbleIPs)

    ###########################################################################
    #                  Update config scripts before SyncRepo
    ###########################################################################

    # Update config files if needed
    print("\n============================================================")
    print("                   Updating config files...")
    print("============================================================")

    # - Update the coordinator address to local IP (Coordinator must be started
    #   locally)
    updateCoordinatorAddrInConfigYaml(f"{workDir}/config.yaml", allNodeIPs)

    # - Update task placement config if needed
    # If no custom task placement provided, random placement will be used
    if initialCustomTaskPlacement:
        print("[Placement INFO] Using custom task placement")
        writeCustomTaskPlacementTxt(workDir, initialCustomTaskPlacement)
        updateConfigYamlFile(f"{workDir}/config.yaml", "TaskPlacementPolicy", "custom")
    else:
        print("[Placement INFO] Using random task placement")
        updateConfigYamlFile(f"{workDir}/config.yaml", "TaskPlacementPolicy", "random")

    # - Update reconfig policy and version
    updateConfigYamlFile(f"{workDir}/config.yaml", "ReconfigProtocol", reconfigProtocol)

    # - Update lazy protocol version
    updateConfigYamlFile(
        f"{workDir}/config.yaml", "LazyProtocolVersion", lazyProtocolVersion
    )

    # - Update state backend config
    updateConfigYamlFile(f"{workDir}/config.yaml", "StateBackendType", stateBackendType)

    # If remote pebble is used, update the config with remote pebble addrs
    if stateBackendType == "remote-pebble":
        updateConfigYamlFile(
            f"{workDir}/config.yaml",
            "RemotePebbleAddrs",
            remotePebbleAddrs,
        )
        
    updateConfigYamlFile(f"{workDir}/config.yaml", "NumBuckets", numBuckets)
    updateConfigYamlFile(f"{workDir}/config.yaml", "PartitionPolicy", partitionPolicy)
    updateConfigYamlFile(f"{workDir}/config.yaml", "BufferSize", bufferSize)
    updateConfigYamlFile(f"{workDir}/config.yaml", "BatchSize", batchSize)
    updateConfigYamlFile(f"{workDir}/config.yaml", "IsWarmup", isWarmUpRun)
    updateConfigYamlFile(f"{workDir}/config.yaml", "LoadWarmupData", useWarmUpData)
    updateConfigYamlFile(f"{workDir}/config.yaml", "LookupTableWarmUpDataFolder", lookupTableWarmUpDataFolder)
    updateConfigYamlFile(f"{workDir}/config.yaml", "PebbleEnableConcurrentGetMany", pebbleEnableConcurrentGetMany)
    updateConfigYamlFile(f"{workDir}/config.yaml", "PebbleGetManyMaxConcurrency", pebbleGetManyMaxConcurrency)
    updateConfigYamlFile(f"{workDir}/config.yaml", "PebbleGetManyBatchSize", pebbleGetManyBatchSize)
    updateConfigYamlFile(f"{workDir}/config.yaml", "EnablePendingBatchTimeout", enablePendingBatchTimeout)

    # - Update cancelling task migration config, only for the fields the JSON
    #   actually provides. If a field is absent, config.yaml is left as-is
    if cancellingTaskMigrationMode is not None:
        updateConfigYamlFile(
            f"{workDir}/config.yaml",
            "LazyByKeyCancellingTaskMigrationMode",
            cancellingTaskMigrationMode,
        )
    if gradualMigrationBatchSize is not None:
        updateConfigYamlFile(
            f"{workDir}/config.yaml",
            "LazyByKeyGradualMigrationBatchSize",
            gradualMigrationBatchSize,
        )


    ###########################################################################
    #                    Compile codebase to generate /bin
    ###########################################################################

    cmdRes = runCmdSync(f"cd {workDir} && make")
    if cmdRes.returncode != 0:
        raise Exception("Error: make fails - see above stderr for details")

    ###########################################################################
    #                    Compile codebase to generate /bin
    ###########################################################################

    cmdRes = runCmdSync(f"cd {workDir} && make")
    if cmdRes.returncode != 0:
        raise Exception("Error: make fails - see above stderr for details")

    ###########################################################################
    #                          Sync repo to all nodes
    ###########################################################################

    # Sync the required executables/scripts/configs to all nodes (overwrite)
    syncRepo(allNodeIPs, workDir)

    # Track all started long-running processes
    procsToWait = []

    ###########################################################################
    #                Create Folder to store LookupTableWarmupData
    ###########################################################################
    if isWarmUpRun:
        dedupedWorkerIPs = list(set(workerIPs))
        for workerIP in dedupedWorkerIPs:
            cmd = f"cd {workDir} && rm -rf {lookupTableWarmUpDataFolder} && mkdir -p {lookupTableWarmUpDataFolder}"
            cmdRes = runSSHCmdSync(workerIP, cmd)
            if cmdRes.returncode != 0:
                raise Exception(
                    f"Error: Failed to create lookup table warmup data on worker {workerIP}"
                    )
    ###########################################################################
    #                       Load warmup data if configured
    ###########################################################################

    print("\n============================================================")
    print("                     Load warmup Data...")
    print("============================================================")

    # Load warmup data
    if useWarmUpData:
        dedupedWorkerIPs = list(set(workerIPs))
        if stateBackendType == "pebble" or stateBackendType == "preLoadedMemory":
            for workerIP in dedupedWorkerIPs:
                cmd = f"cd {workDir} && mkdir data && cp -r {pebbleWarmUpDataFolder}/pebble data"
                cmdRes = runSSHCmdSync(workerIP, cmd)
                if cmdRes.returncode != 0:
                    raise Exception(
                        f"Error: Failed to load pebble warmup data on worker {workerIP}"
                    )
        elif stateBackendType == "remote-pebble":
            for workerIP in remotePebbleIPs:
                cmd = f"cd {workDir} && mkdir data && cp -r {pebbleWarmUpDataFolder}/pebble data"
                cmdRes = runSSHCmdSync(workerIP, cmd)
                if cmdRes.returncode != 0:
                    raise Exception(
                        f"Error: Failed to load pebble warmup data on worker {workerIP}"
                    )
            
    ###########################################################################
    #                     Start remote pebble instances
    ###########################################################################

    if stateBackendType == "remote-pebble":
        print("\n============================================================")
        print("                   Start Remote Pebble...")
        print("============================================================")
        for addr in remotePebbleAddrs:
            ip, port = addr.split(":")
            cmd = f"cd {workDir} && ./bin/remotePebble {port}"
            remotePebbleProc = runSSHCmdAsync(ip, cmd)
            procsToWait.append(remotePebbleProc)
            time.sleep(1)
        print("[INFO] Starting remote pebble instances...")
        sleepWithLogging(3, printIntervalSeconds=1)
    ###########################################################################
    #                           Start Kafka cluster
    ###########################################################################

    # Start Kafka cluster - cluster is monitored within the background process.
    # If any broker fails, the whole Kafka cluster will be shut down
    kafkaClusterProc = runFuncAsync(
        startKafkaClusterWrapper, kafkaClusterIPs, workDir, False
    )
    sleepWithLogging(KafkaDeployWarmupTime + 5, printLog=False)

    ###########################################################################
    #                         Deploy and run the query
    ###########################################################################

    # 1. Start Coordinator - Coordinator must be started locally
    print("\n============================================================")
    print("                     Start Coordinator...")
    print("============================================================")
    cmd = f"cd {workDir} && ./bin/coordinator {queryName} {jsonPath}"
    coordinatorProc = runCmdAsync(cmd)
    procsToWait.append(coordinatorProc)
    print("[INFO] Starting coordinator...")
    sleepWithLogging(3, printIntervalSeconds=1)

    # 2. Start all workers
    print("\n============================================================")
    print("                       Start Workers...")
    print("============================================================")
    dataPlanePortBase = 9001
    controlPlanePortBase = 8001
    for i, workerIP in enumerate(workerIPs):
        cmd = f"cd {workDir} && ./bin/worker {dataPlanePortBase + i} {controlPlanePortBase + i} {queryName} {jsonPath}"
        if isWarmUpRun:
            workerProc = runSSHCmdAsync(workerIP, cmd, True, True)
        else:
            workerProc = runSSHCmdAsync(workerIP, cmd)
        procsToWait.append(workerProc)
        # Ensure the workers are successfully started in order
        time.sleep(2)
    print("[INFO] Starting all workers...")
    sleepWithLogging(3, printIntervalSeconds=1)

    # Wait enough time for all workers with in memory state backend to preload data
    if stateBackendType == "preLoadedMemory":
        sleepWithLogging(60, printIntervalSeconds=5)
    # 3. Submit the query
    print("\n============================================================")
    print("                     Submit the query...")
    print("============================================================")
    cmd = f"cd {workDir} && ./bin/client deploy"
    clientProc = runCmdSync(cmd)
    if clientProc.returncode != 0:
        raise Exception("Client failed to submit the query")

    ###########################################################################
    #                           Start Kafka producers
    ###########################################################################

    # Start the Kafka producers to generate data
    print("\n============================================================")
    print("                   Start Kafka producers...")
    print("============================================================")
    if queryName.startswith("nexmark_"):
        for i, producerIP in enumerate(producerIPs):
            cmd = f"cd {workDir} && ./bin/nexmarkKafkaProducer {jsonPath} {i}"
            producerProc = runSSHCmdAsync(producerIP, cmd)
            procsToWait.append(producerProc)
    # If query is not a nexmark query, it should use a fileReader kafkaProducer
    elif queryName in allowedQueries :
        for i, producerIP in enumerate(producerIPs):
            cmd = f"cd {workDir} && ./bin/fileReaderKafkaProducer {jsonPath} {i}"
            producerProc = runSSHCmdAsync(producerIP, cmd)
            procsToWait.append(producerProc)
    else: 
         raise Exception(f"Unsupported query name: {queryName}")
    print("[INFO] All producers started")
    sleepWithLogging(3, printIntervalSeconds=1)

    ###########################################################################
    #                           Main execution loop
    ###########################################################################

    # Monitor all components until the experiment duration is reached
    # 1. Check all existing processes if any of them is failed
    # 2. Check elapsed time to trigger rescaling action if needed
    # 3. Shut down and clean up after experiment duration is reached
    # 4. Monitor loop is executed at configured interval
    print("\n============================================================")
    print("                     Running experiment...")
    print("============================================================")
    start = time.time()
    experimentStatus = "SUCCEEDED"
    reconfigIdx = 0
    while True:

        # Check all started processes and identify failure if exists. Note that
        # Ctrl-C is also eventually identified here to exit the loop
        if findFailure(procsToWait, kafkaClusterProc):
            experimentStatus = "FAILED"
            break

        # Exit if experiment duration is reached
        elapsed = int(time.time() - start)
        if elapsed >= totalRuntimeSeconds:
            print("[INFO] Experiment duration reached, stopping all components...")
            break

        # Trigger reconfiguration actions if needed
        if (
            reconfigIdx < len(reconfigurations)
            and elapsed >= reconfigurations[reconfigIdx].triggerTimeSeconds
        ):
            triggerReconfiguration(reconfigurations[reconfigIdx], workDir)
            reconfigIdx += 1

        print(
            f"[INFO] Experiment running... Elapsed time: {elapsed}/{totalRuntimeSeconds} seconds"
        )
        time.sleep(ExecutionLoopInterval)

    # Store or results or warmup data based on config
    if isWarmUpRun:

        #######################################################################
        #                           Store warmup data
        #######################################################################

        print("\n============================================================")
        print("                     Store warmup data...")
        print("============================================================")

        if stateBackendType == "pebble":
            dedupedWorkerIPs = list(set(workerIPs))
            for workerIP in dedupedWorkerIPs:
                cmd = f"cd {workDir} && rm -rf {pebbleWarmUpDataFolder} && mkdir -p {pebbleWarmUpDataFolder} && mv data/pebble {pebbleWarmUpDataFolder}/"
                cmdRes = runSSHCmdSync(workerIP, cmd)
                if cmdRes.returncode != 0:
                    print(
                        f"[ERROR] Failed to store pebble warmup data in worker {workerIP}"
                    )
        elif stateBackendType == "remote-pebble":
            for workerIP in remotePebbleIPs:
                cmd = f"cd {workDir} && rm -rf {pebbleWarmUpDataFolder} && mkdir -p {pebbleWarmUpDataFolder} && mv data/pebble {pebbleWarmUpDataFolder}/"
                cmdRes = runSSHCmdSync(workerIP, cmd)
                if cmdRes.returncode != 0:
                    print(
                        f"[ERROR] Failed to store pebble warmup data in worker {workerIP}"
                    )

    else:
        #######################################################################
        #                        Store experiment results
        #######################################################################

        # Move metric DB and needed logs to a separate folder otherwise they will
        # be removed in the clean up step. No matter if the experiment succeeded or
        # failed, we still want to keep the results
        print("\n============================================================")
        print("                       Store results...")
        print("============================================================")
        # Move local metricDB to scripts/results/<queryName>_<keyword>
        moveMetricDBToResults(workDir, queryName, resultKeyword)

    ###########################################################################
    #                               Final clean up
    ###########################################################################

    # Clean up and remote the /data folder in all nodes
    if isWarmUpRun:
         cleanUp(producerIPs, kafkaClusterIPs, allNodeIPs, workDir, remotePebbleIPs,True)
    else:
         cleanUp(producerIPs, kafkaClusterIPs, allNodeIPs, workDir, remotePebbleIPs)
   

    # Make sure all started processes are exited for graceful termination
    for proc in procsToWait:
        proc.wait()
    kafkaClusterProc.join()

    print("\n============================================================")
    print("runExperiment() completed - all processes exited and cleaned up")
    print(f"             Experiment Status: {experimentStatus}")
    print("============================================================")


# Check if all started processes are still running
def findFailure(
    procs: list[subprocess.Popen],
    kafkaClusterProc: Process,
) -> bool:

    # Check all subprocess.Popen processes
    for proc in procs:
        returnCode = proc.poll()
        if returnCode is not None:
            print(
                f"[ERROR] A process (e.g. coordinator, worker, producer) exited with code {returnCode}, stopping all..."
            )
            return True

    # Check the multiprocessing.Process
    if not kafkaClusterProc.is_alive():
        print("[ERROR] Kafka cluster process exited, stopping all...")
        return True
    return False


# Helper to shut down all components and clean up
# 1. Stop all producers
# 2. Shut down coordinator (this will automatically terminate all workers)
# 3. Stop Kafka cluster
# 4. Clear the /data folder in all nodes
def cleanUp(
    producerIPs: list[str],
    kafkaClusterIPs: list[str],
    allNodeIPs: list[str],
    workDir: str,
    remotePebbleIPs: list[str],
    ifDeleteDataFolder: bool = True,
) -> None:
    print("\n============================================================")
    print("      [CLEANUP INFO] Cleaning up all components...")
    print("============================================================")

    # Shut down all producers
    print("\n[CLEANUP INFO][Progress 1/5] Stopping all producers...")
    stopProducers(producerIPs, workDir)

    # Shut down remote pebble instances
    print("\n[CLEANUP INFO][Progress 2/5] Stopping remote pebble instances...")
    if remotePebbleIPs:
        for ip in remotePebbleIPs:
            cmd = "pkill -f remotePebble || true"
            runSSHCmdSync(ip, cmd)
    else:
        print("[CLEANUP INFO] No remote pebble instances to stop.")

    # Shut down coordinator - this will automatically terminate all workers
    print("\n[CLEANUP INFO][Progress 3/5] Shutting down coordinator and workers...")
    shutdownCoordinator()

    # Shut down Kafka cluster
    print("\n[CLEANUP INFO][Progress 4/5] Stopping Kafka cluster...")
    stopKafkaCluster(kafkaClusterIPs, workDir)

    # Clear the /data folder in all nodes
    if ifDeleteDataFolder:
        print("\n[CLEANUP INFO][Progress 5/5] Cleaning up data folders in all nodes...")
        cmd = f"rm -rf {workDir}/data"
        for ip in allNodeIPs:
            runSSHCmdSync(ip, cmd)
        print("[CLEANUP INFO] Clean up done!\n")


def startKafkaClusterWrapper(
    kafkaClusterIPs: list[str], workDir: str, printKafkaLog: bool = True
) -> None:
    # multiprocessing passes the signal handler to child processes, so we need to
    # ignore ctrl-c signals in this wrapper function to avoid duplicate handling
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    startKafkaCluster(kafkaClusterIPs, workDir, printKafkaLog)


if __name__ == "__main__":

    # <configFilePath>: path to the query JSON config file
    # <resultKeyword>: keyword to identify the result folder

    if len(sys.argv) != 3:
        print("Usage: python3 runExperiment.py <configFilePath> <resultKeyword>")
        sys.exit(1)

    jsonPath = sys.argv[1]
    resultKeyword = sys.argv[2]

    runExperiment(jsonPath, resultKeyword)
