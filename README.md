

# Steps to run the current version of the engine:


### __1. Define the dataflow__

   Define the streaming query at `user/user.go`. You can start with the example query at `query/examples`.

### __2. Compile__

Compile Coordinator, Worker, Client along with the user-defined query.
```
// cd to root directory of the repo
make
```

### __3. Start Coordinator__

The first step is to start the Coordinator for the system. The Coordinator listening endpoints (i) Control plane addr, (ii) API addr, and (iii) Metric collector addr can be configured in config.yaml file.

```
./bin/coordinator
```

### __4. Start Workers__

Now add Workers to the system. Each Worker can hold 1 task (an instance of the operator), so the total number of Workers added should >= total parallelism of the dataflow. To start each Worker, we need to specify (i) data-plane-port and (ii) state-comm-port as command line parameters.

```
./bin/worker [data-plane-port] [state-comm-port]
```

Each Worker automically register themselves to Coordinator.

### __5. Deploy the dataflow__

To deploy the job, we use Client to request the Coordinator. Since the dataflow is already compiled and embedded within Coordinator and Worker, Client simply notifies the Coordinator to run the job.
```
./bin/client
```
Note: to rerun the job, we need to manually remove the ``./data`` folder from the previous deployment. 

### __6. Apply reconfiguration__

We support 2 runtime reconfiguration protocols:
- Stop-and-restart: pause the dataflow and apply the topology changes with full state migration (with local state backend e.g. pebble)
- Lazy: apply the routing change without downtime using disaggregated state service (see [doc](https://docs.google.com/document/d/1HsBzwp1ElOGvlPMn3SfM97gktw0B5PldHDbhm1eMMng/edit?usp=sharing) for protocol details)

To execute runtime reconfiguration, follow the steps:
1. Choose the reconfig protocol by changing the `ReconfigProtocol` parameter in `config.yaml` file. Either use "stop-and-restart" or "lazy".
2. Start Coordinator (as shown in step 3 above).
3. Start Workers (as shown in step 4 above) - we should start sufficient number of workers for scale-up e.g. consider # of additional workers for scale-up.
4. Deploy the dataflow with reconfig flag on: `./bin/client reconfig`. - Note that now reconfig operation is hard-coded in client.go for now e.g. (i) scale up to 3 replicas for target operator, (ii) run the job for 15s before applying reconfig. These hard-coded reconfig details will be changed to configurable parameters next.

Current limitations for reconfiguration:
- Reconfig now only supports scale-up operation.
- We do not support consecutive reconfigs (more than 1) for lazy protocol.
- Watermark forwarding for lazy protocol is under implementation - window query doesn't work now
