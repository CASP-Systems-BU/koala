
# Summary

Instructions to run scripts for Kafka cluster, Kafka producer, and query execution, with configurable reconfiguration actions.

Note:
- For all JSON config file path parameters below, the working directory should be `koala/scripts`.

## Run Experiment

Before running the script, we should manually compile the repo on root node.
```bash
cd ~/koala
make
```

`runExperiment.py` is the script that includes all steps in a single place. It is responsible:
1. Update config scripts
2. Sync files from root to all other nodes
3. Start Kafka cluster
4. Deploy and run the query
5. Start Kafka producers
6. Monitor the progress and status of the whole system
7. Apply reconfiguration operations
8. Store results
9. Graceful termination and clean up

Run the script by providing:
1. Query name
2. Config file path (in `scripts/` folder)
3. Keyword for result folder - `<query_name>_keyword`. The experiment result folder will be stored in `/scripts/results/<query_name>_keyword`. If the exp_result folder already exists, it will create a new folder with current timestamp appended.
```bash
cd ~/koala/scripts
python3 runExperiment.py nexmark_query1 nexmarkJson/query1.json exp_key_word
```

Add experiment configurations in the config JSON file. Example: `nexmarkJson/query1.json`
- `AllNodeIPs`: include IPs of all nodes involved in the experiment (no duplicates)
- `KafkaClusterIPs`: IPs of Kafka brokers on different machines (no duplicates)
- `ProducerIPs`: IPs of Kafka producers (allow duplicates - indicates multiple producers on same machine).
    - Note that the number of producers specified here is used to identify **(i) the number of partitions in each Kafka topic**, and **(ii) parallelism for Kafka source operator**. We enforce the 1-to-1 mapping from `producer -> topic partition -> source task` to guarantee strict input order. Therefore, source operator parallelism is not a configurable parameter.
- `WorkerIPs`: IPs of all task workers (allow duplicates - indicates multiple workers on same machine)
```json
{
  "WorkDir": "~/koala",
  "AllNodeIPs": ["10.10.1.1"],
  "KafkaClusterIPs": ["10.10.1.1"],
  "ProducerIPs": ["10.10.1.1"],
  "WorkerIPs": ["10.10.1.1", "10.10.1.1", "10.10.1.1"],
  "NexmarkLogicalSourceConfigs": [{
    "EventType": "Bid",
    "NexmarkSourceConfig": {}
  }],
  "TotalRuntimeSeconds": 300,
  "MapperParallelism": 1,
  "SinkParallelism": 1
}
```
Notes:
- The script monitors all processes started: it shut down all components if any failure is identified
- Ctrl-C triggers full shut-down and clean-up

To trigger reconfiguration operation, add the following optional `Reconfigurations` field to the JSON config file. It defines when will a reconfig operation will be triggered. Note that the number of workers started at the beginning should consider the total number of workers needed after rescale - otherwise there can be resource-not-enough error for scale up.

By default, if `Reconfigurations` field is missing in JSON, there will be no reconfiguration applied.

Protocol policy and version can also be specified in JSON file: `ReconfigProtocol` and `LazyProtocolVersion`. By default (if not specified), they are "stop-and-restart" and "basic".

```json
{
    "Reconfigurations": [{
        "TriggerTimeSeconds": 30,
        "Type": "scaleup",
        "TargetOperator": "mapper",
        "TargetParrallelism": 2
    }],
    "ReconfigProtocol": "lazy",
    "LazyProtocolVersion": "basic"
}
```

To specify custom task placement place for for initial job deployment, add the following optional `InitialCustomTaskPlacement` field to the JSON config file. It will automatically update the `/scripts/taskPlacement/customPlacement.txt` file and update `TaskPlacementPolicy` field in config.yaml file.

By default, if `InitialCustomTaskPlacement field is missing in JSON, random task placement policy will be used.

```json
{
    "InitialCustomTaskPlacement": [
        "source: 10.10.1.1",
        "mapper: 10.10.1.1",
        "sink: 10.10.1.1"
    ]
}
```

## Sync Repo

We operate on the root node of the experiment cluster. `syncRepo.py` broadcasts the local work directory (config.yaml, /bin, /scripts) to all other nodes (overwrite) based on JSON config file e.g. `nexmarkJson/query1.json`.
```json
{
  "AllNodeIPs": ["10.10.1.1", "10.10.1.2", "10.10.1.3"]
}
```
The script automatically identifies the root node IP in `"AllNodeIPs"`, and broadcasts to other ndoes. `"AllNodeIPs"` should include all nodes used in the experiment e.g. producer, kafka, worker.

Run `syncRepo.py` to execute broadcast. Provide JSON config file path.
```bash
$ python3 syncRepo.py nexmarkJson/query1.json
```


## Kafka Cluster

Steps to deploy and stop a Kafka cluster:

### 1. Define Kafka cluster IPs

Provide a list of IP addresses for Kafka cluster nodes in the JSON config file e.g. `nexmarkJson/query1.json`.
```json
{
  "KafkaClusterIPs": ["10.10.1.1", "10.10.1.2", "10.10.1.3"],
}
```

### 2. Start Kafka cluster

Run `startKafkaCluster.py` to start the cluster. Provide JSON config file path.
```bash
$ python3 startKafkaCluster.py nexmarkJson/query1.json
```
This script is a long running process that keeps connections with all Kafka nodes:
- It only exits when all Kafka nodes are terminated.
- This script automatically monitors the health of the Kafka cluster - if any node is failed, it will terminate the whole cluster and clean up.
- `Ctrl-C` signal will gracefully kill the process by terminating the Kafka cluster first.

### 3. Stop Kafka cluster

Run `stopKafkaCluster.py` to stop the cluster. Provide JSON config file path.
```bash
$ python3 stopKafkaCluster.py nexmarkJson/query1.json
```
This script tries to stop the kafka cluster and clean up the log & state.

## Kafka Producers

Steps to deploy Kafka producers. We support different types of Kafka producers e.g. nexmark, fileReader.

### 1. Define where to run producers

Provide a list of IP addresses for producers in the JSON config file e.g. `nexmarkJson/query1.json`. We allow duplicate IP addresses here to indicate running multiple producers on the same node.
```json
{
  "ProducerIPs": ["10.10.1.1", "10.10.1.1", "10.10.1.2"],
}
```
### 2. Start all producers

Run `startProducers.py` to start all producer processes. Provide producer type (e.g. nexmark, fileReader) and JSON config file path.
```bash
$ python3 startProducers.py nexmark nexmarkJson/query1.json
```
This script is a long running process that keeps connections with all producers:
- It prints out per-producer output rate
- `Ctrl-C` will automatically kill all producers on all nodes

### 3. Stop all producers

`Ctrl-C` the `startProducers.py` will stop all producers. We can also explicitly stop all producers.

```bash
$ python3 stopProducers.py nexmarkJson/query1.json
```
