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

	// Type of the state backend in state service: "memory", "pebble", "tikv",
	// "remote-pebble"
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

	// Partition policy: "consistent-hashing", "uniform", "consistent-even"
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
	// 4. "by-key": use KeyLookupTableV2 for per-key based state migration
	LazyProtocolVersion string `yaml:"LazyProtocolVersion"`

	// [lazy-opt] Routine pool size for state flush - execute SetMany() in
	// parallel for values received from remote workers
	StateFlushRoutinePoolSize int `yaml:"StateFlushRoutinePoolSize"`

	// [lazy-opt] Routine pool size for state read - execute GetMany() and
	// RangeQuery() in parallel for async bucket fetch at remote workers
	StateReadRoutinePoolSize int `yaml:"StateReadRoutinePoolSize"`

	// [lazy-opt] Bucket migration chunk size: max # of bytes to send in one
	// StateChunk message during async bucket migration
	LazyOptBucketMigrationChunkSize uint64 `yaml:"LazyOptBucketMigrationChunkSize"`

	// [lazy-by-key] state comm API type for state fetch: "grpc" or "tcp"
	LazyByKeyStateCommAPIType string `yaml:"LazyByKeyStateCommAPIType"`

	// [lazy-by-key] Eventual migration strategy for buckets from a cancelling
	// task. Supported types:
	// - "fetch-on-demand": no eventual migration; keys remain fetched on demand
	// - "eventual": eventually migrate all keys from cancelling tasks
	LazyByKeyCancellingTaskMigrationMode string `yaml:"LazyByKeyCancellingTaskMigrationMode"`

	// [lazy-by-key] Max number of additional keys to fetch per batch during
	// eventual migration from cancelling tasks. Set to -1 to fetch all keys
	// at once. Only used when LazyByKeyCancellingTaskMigrationMode is
	// "eventual"
	LazyByKeyGradualMigrationBatchSize int `yaml:"LazyByKeyGradualMigrationBatchSize"`

	/**************************************************************************
								   Debug configs
	**************************************************************************/

	// Debug mode: print debug messages
	DebugMode bool `yaml:"DebugMode"`

	// Watermark debug mode: print watermark debug messages
	WatermarkDebug bool `yaml:"WatermarkDebug"`

	/**************************************************************************
								   Warmup configs
	**************************************************************************/

	// Whether this is a warmup run
	IsWarmup bool `yaml:"IsWarmup"`

	// Load warmup data or not
	LoadWarmupData bool `yaml:"LoadWarmupData"`

	// Path to store and read warmup data
	LookupTableWarmUpDataFolder string `yaml:"LookupTableWarmUpDataFolder"`
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
		StateBackendType:              "pebble",
		PebbleDisableWAL:              true,
		PebbleMemTableSize:            67108864,
		PebbleSyncOnWrite:             false,
		PebbleEnableConcurrentGetMany: false,
		PebbleGetManyMaxConcurrency:   4,
		PebbleGetManyBatchSize:        64,
		TiKVAddr:                      "192.168.1.101:2379",
		RemotePebbleAddrs:             []string{},
		StateMigrationChunkSize:       1048576,
		// Configs for key partitioning
		NumBuckets:             256,
		HashFuncType:           "murmurhash",
		KeyHashSeed:            1234,
		PartitionPolicy:        "consistent-even",
		HashPartitionNumTokens: 5,
		HashPartitionSeeds:     []uint64{2345, 5678, 91011, 121314, 151617},
		// Metric
		MetricsInterval: 5 * time.Second,
		// Reconfig
		ReconfigProtocol:                     "stop-and-restart",
		LazyProtocolVersion:                  "basic",
		StateFlushRoutinePoolSize:            1,
		StateReadRoutinePoolSize:             1,
		LazyOptBucketMigrationChunkSize:      4096, // 4KB
		LazyByKeyStateCommAPIType:            "grpc",
		LazyByKeyCancellingTaskMigrationMode: "fetch-on-demand",
		LazyByKeyGradualMigrationBatchSize:   100,
		// Debug
		DebugMode:      false,
		WatermarkDebug: false,
		// WarmupRelated
		IsWarmup:                    false,
		LoadWarmupData:              false,
		LookupTableWarmUpDataFolder: "warmup_data/",
	}
}

// Deep copy the config to generate a new config with same values
func DeepCopyConfig(cfg *Configuration) *Configuration {

	data, _ := json.Marshal(cfg)
	copy := &Configuration{}
	json.Unmarshal(data, copy)
	return copy
}
