package configuration

import (
	"encoding/json"
	"time"
)

// Each component of the system (e.g. Worker) has a different snapshot of the
// configuration
type Configuration struct {

	/**************************************************************************
								   General configs
	**************************************************************************/

	// Coordinator control plane address: IP:port
	CoordinatorAddr string `yaml:"CoordinatorAddr"`

	// Coordinator API service address: IP:port
	CoordinatorAPIAddr string `yaml:"CoordinatorAPIAddr"`

	//Coordinator metric collector service address: IP:port
	CoordinatorMetricCollectorAddr string `yaml:"CoordinatorMetricCollectorAddr"`

	// Data plane listening port
	DataPlanePort string `yaml:"DataPlanePort"`

	// State comm service listening port
	StateCommPort string `yaml:"StateCommPort"`

	// Buffer size: # of batch slots in input/output buffer
	BufferSize int64 `yaml:"BufferSize"`

	// Lock-free buffer sleep interval: waiting time when ring buffer detects
	// buffer full/empty
	BufferSleepInterval time.Duration `yaml:"BufferSleepInterval"`

	// Batch size: # of max bytes a batch can hold (after serialization)
	BatchSize int64 `yaml:"BatchSize"`

	// For pre-allocated batch in batch pool, default initial # of allocated
	// record slots
	PreAllocatedBatchInitCapacity int64 `yaml:"PreAllocatedBatchInitCapacity"`

	// Enable pending batch timeout flush
	EnablePendingBatchTimeout bool `yaml:"EnablePendingBatchTimeout"`

	// Collector pending batch timeout flush interval. This is only used when
	// EnablePendingBatchTimeout is true
	PendingBatchTimeoutInterval time.Duration `yaml:"PendingBatchTimeoutInterval"`

	// Task placement policy: "random", "custom"
	TaskPlacementPolicy string `yaml:"TaskPlacementPolicy"`

	// [custom task placement] path to custom task placement file
	CustomTaskPlacementFile string `yaml:"CustomTaskPlacementFile"`

	/**************************************************************************
								   State configs
	**************************************************************************/

	// Type of the state backend in state service: "memory", "pebble", "tikv"
	StateBackendType string `yaml:"StateBackendType"`

	// [pebble] config if to disable Write-Ahead Logging (WAL)
	PebbleDisableWAL bool `yaml:"PebbleDisableWAL"`

	// [pebble] config MemTableSize in bytes
	PebbleMemTableSize uint64 `yaml:"PebbleMemTableSize"`

	// [pebble] config if to immediately flush disk on each write
	PebbleSyncOnWrite bool `yaml:"PebbleSyncOnWrite"`

	// [pebble] Enable concurrent reads in GetMany()
	PebbleEnableConcurrentGetMany bool `yaml:"PebbleEnableConcurrentGetMany"`

	// [pebble] Max number of concurrency for GetMany() parallel reads
	PebbleGetManyMaxConcurrency int `yaml:"PebbleGetManyMaxConcurrency"`

	// [pebble] Expected batch size per GetMany() routine executor
	PebbleGetManyBatchSize int `yaml:"PebbleGetManyBatchSize"`

	// [pebble] Max number of concurrent compactions
	PebbleMaxConcurrentCompactions int `yaml:"PebbleMaxConcurrentCompactions"`

	// [pebble] Target size of the base level (L-Base) in bytes
	PebbleLBaseMaxBytes int64 `yaml:"PebbleLBaseMaxBytes"`

	// [pebble] Number of memtables before writes are stopped to allow flushing
	PebbleMemTableStopWritesThreshold int `yaml:"PebbleMemTableStopWritesThreshold"`

	// [remote-pebble] list of remote pebble addresses (host:port)
	RemotePebbleAddrs []string `yaml:"RemotePebbleAddrs"`

	// [tikv] config DB address
	TiKVAddr string `yaml:"TiKVAddr"`

	// [State Comm Service] State migration gRPC message chunk size in bytes
	// State is truncated into chunks of this size for migration
	StateMigrationChunkSize uint64 `yaml:"StateMigrationChunkSize"`

	/**************************************************************************
					  Key space partitioning related configs
	**************************************************************************/

	// Number of buckets for fixed key space partitioning
	NumBuckets int64 `yaml:"NumBuckets"`

	// Which hash function to use
	// Now only support "murmurhash"
	HashFuncType string `yaml:"HashFuncType"`

	// Key hash function seed
	KeyHashSeed uint64 `yaml:"KeyHashSeed"`

	// Partition policy: "consistent-hashing", "uniform"
	PartitionPolicy string `yaml:"PartitionPolicy"`

	// [Consistent hashing policy] number of tokens generated for each worker
	HashPartitionNumTokens int `yaml:"HashPartitionNumTokens"`

	// [Consistent hashing policy] list of seeds for hash functions
	HashPartitionSeeds []uint64 `yaml:"HashPartitionSeeds"`

	/**************************************************************************
								Metrics related config
	**************************************************************************/

	MetricsInterval time.Duration `yaml:"MetricsInterval"`

	/**************************************************************************
						    Configs for reconfiguration
	**************************************************************************/

	// Protocol used during reconfiguration: "stop-and-restart" or "lazy"
	ReconfigProtocol string `yaml:"ReconfigProtocol"`

	// If lazy protocol is enabled, we support multiple versions:
	// 1. "basic": sync bucket migration
	// 2. "optimized": async bucket migration
	// 3. "no-migration": only remote state access without bucket migration
	// 4. "drrs"
	LazyProtocolVersion string `yaml:"LazyProtocolVersion"`

	/**************************************************************************
								   Debug configs
	**************************************************************************/

	// Debug mode: print debug messages
	DebugMode bool `yaml:"DebugMode"`

	// Watermark debug mode: print watermark debug messages
	WatermarkDebug bool `yaml:"WatermarkDebug"`

	/**************************************************************************
								   	   DRRS
	**************************************************************************/

	// Number of subscale for DRRS. -1 indicates automatically calculate
	// subscales based on number of worker-to-worker paris of connection.
	NumSubscales int `yaml:"NumSubscales"`

	// Number of buckets received before triggering the wait buffer
	// -1 indicates wait until all buckets are received to report progress
	MigrationProgressGranularity int64 `yaml:"MigrationProgressGranularity"`

	// Number of DRRS batches allowed in upstream wait buffer. Larger buffer
	// enables larger out-of-orderness in processing
	WaitBufferSize int64 `yaml:"WaitBufferSize"`
}

// Default configuration values
func Default() *Configuration {
	return &Configuration{
		// Basic config
		CoordinatorAddr:                "localhost:8888",
		CoordinatorAPIAddr:             "localhost:9999",
		CoordinatorMetricCollectorAddr: "localhost:8889",
		DataPlanePort:                  "8900",
		StateCommPort:                  "8901",
		BufferSize:                     20,
		BufferSleepInterval:            time.Millisecond,
		BatchSize:                      32768,
		PreAllocatedBatchInitCapacity:  2000,
		EnablePendingBatchTimeout:      true,
		PendingBatchTimeoutInterval:    200 * time.Millisecond,
		TaskPlacementPolicy:            "random",
		CustomTaskPlacementFile:        "./scripts/taskPlacement/customPlacement.txt",
		// State service config
		StateBackendType:                  "pebble",
		PebbleDisableWAL:                  true,
		PebbleMemTableSize:                67108864,
		PebbleSyncOnWrite:                 false,
		PebbleEnableConcurrentGetMany:     false,
		PebbleGetManyMaxConcurrency:       4,
		PebbleGetManyBatchSize:            64,
		PebbleMaxConcurrentCompactions:    1,        // pebble default: 1
		PebbleLBaseMaxBytes:               67108864, // pebble default: 64MB
		PebbleMemTableStopWritesThreshold: 2,        // pebble default: 2
		TiKVAddr:                          "192.168.1.101:2379",
		StateMigrationChunkSize:           1048576,
		// Configs for key partitioning
		NumBuckets:             1024,
		HashFuncType:           "murmurhash",
		KeyHashSeed:            1234,
		PartitionPolicy:        "consistent-hashing",
		HashPartitionNumTokens: 5,
		HashPartitionSeeds:     []uint64{2345, 5678, 91011, 121314, 151617},
		// Metric
		MetricsInterval: 5 * time.Second,
		// Reconfig
		ReconfigProtocol:    "stop-and-restart",
		LazyProtocolVersion: "basic",
		// Debug
		DebugMode:      false,
		WatermarkDebug: false,
		// DRRS
		NumSubscales:                 -1,
		MigrationProgressGranularity: -1,
		WaitBufferSize:               2,
	}
}

// Deep copy the config to generate a new config with same values
func DeepCopyConfig(cfg *Configuration) *Configuration {

	data, _ := json.Marshal(cfg)
	copy := &Configuration{}
	json.Unmarshal(data, copy)
	return copy
}
