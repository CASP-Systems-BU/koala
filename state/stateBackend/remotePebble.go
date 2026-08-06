package stateBackend

import (
	"context"
	"encoding/binary"
	"log"
	"net"
	"sync"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/constant"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/metric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RemotePebbleStateBackend shards state across multiple remote pebble instances
// Keys are routed by bucket index to a shard based on the routing table.
type RemotePebbleStateBackend struct {
	sync.Mutex

	// Shard ID (idx) -> remote pebble client. Each shard corresponds to one
	// remote pebble instance
	shards []remotePebbleShardClient

	// Bucket ID (idx) -> Shard ID (idx)
	routingTable []int

	// Total number of buckets
	numBuckets int

	// metric collector
	MetricCollector *metric.MetricCollector

	averageReadTimePerRequest *metric.Average

	averageWriteTimePerRequest *metric.Average
}

var _ StateBackend = (*RemotePebbleStateBackend)(nil)

func NewRemotePebbleStateBackend(
	config *configuration.Configuration,
) *RemotePebbleStateBackend {
	if len(config.RemotePebbleAddrs) <= 0 {
		log.Fatalf(
			"There are no remote pebble addresses in RemotePebbleAddrs",
		)
	}
	for _, addr := range config.RemotePebbleAddrs {
		if _, _, err := net.SplitHostPort(addr); err != nil {
			log.Fatalf(
				"RemotePebbleAddrs must be in host:port format, got %s",
				addr,
			)
		}
	}
	if config.NumBuckets <= 0 {
		log.Fatalln("Number of buckets must be positive")
	}

	numShards := len(config.RemotePebbleAddrs)
	shards := make([]remotePebbleShardClient, numShards)
	for i := range numShards {
		shards[i] = newRemotePebbleShardClient(config.RemotePebbleAddrs[i])
	}
	return &RemotePebbleStateBackend{
		shards: shards,
		routingTable: buildRemotePebbleRoutingTable(
			int(config.NumBuckets),
			numShards,
		),
		numBuckets:                 int(config.NumBuckets),
		averageReadTimePerRequest:  metric.NewAverage(),
		averageWriteTimePerRequest: metric.NewAverage(),
	}
}

type remotePebbleShardClient struct {
	addr   string
	conn   *grpc.ClientConn
	client pb.RemotePebbleServiceClient
}

func newRemotePebbleShardClient(addr string) remotePebbleShardClient {
	conn, err := grpc.NewClient(
		addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		// 🔹 HTTP/2 flow control (transport-level)
		grpc.WithInitialWindowSize(1<<18),     // 2MB per stream
		grpc.WithInitialConnWindowSize(1<<18), // 4MB per connection
		grpc.WithWriteBufferSize(1<<18),       // 1 MB
		grpc.WithReadBufferSize(1<<18),        // 1 MB
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(constant.RpcMaxMessageSize),
			grpc.MaxCallSendMsgSize(constant.RpcMaxMessageSize),
		),
	)
	if err != nil {
		log.Fatalf("Failed to connect to remote pebble at %s: %v", addr, err)
	}

	return remotePebbleShardClient{
		addr:   addr,
		conn:   conn,
		client: pb.NewRemotePebbleServiceClient(conn),
	}
}

func buildRemotePebbleRoutingTable(numBuckets int, numShards int) []int {
	if numBuckets <= 0 || numShards <= 0 {
		log.Fatalln("Invalid number of buckets or shards")
	}

	// Round-robin: assign bucket i to shard (i % numShards)
	routingTable := make([]int, numBuckets)
	for bucketIdx := range numBuckets {
		routingTable[bucketIdx] = bucketIdx % numShards
	}
	return routingTable
}

func (r *RemotePebbleStateBackend) bucketIdxForKey(key []byte) int {
	if key == nil {
		key = []byte{}
	}
	if len(key) < constant.KeyPrefixSize {
		log.Fatalln("Key is too short to contain bucket index")
	}
	// Extract bucket index from key prefix
	bucketOffset := constant.OperatorIDSize
	bucketEnd := bucketOffset + constant.BucketIdxSize
	return int(binary.BigEndian.Uint32(key[bucketOffset:bucketEnd]))
}

func (r *RemotePebbleStateBackend) shardForKey(
	key []byte,
) remotePebbleShardClient {
	bucketIdx := r.bucketIdxForKey(key)
	shardIdx := r.routingTable[bucketIdx]
	return r.shards[shardIdx]
}

/******************************************************************************
	 					Implement StateBackend interface
******************************************************************************/

func (r *RemotePebbleStateBackend) Get(key []byte) []byte {
	if key == nil {
		log.Fatalf("Get(): key is nil")
	}
	values := r.remoteRead(r.shardForKey(key), [][]byte{key})
	if len(values) == 0 {
		return nil
	}
	return values[0]
}

func (r *RemotePebbleStateBackend) Set(key []byte, value []byte) {
	if key == nil || value == nil {
		log.Fatalf("Set(): key or value is nil")
	}
	shard := r.shardForKey(key)
	r.remoteWrite(shard, [][]byte{key}, [][]byte{value}, false)
}

func (r *RemotePebbleStateBackend) GetMany(keys [][]byte) [][]byte {
	if keys == nil {
		log.Fatalf("GetMany(): keys is nil")
	}

	result := make([][]byte, len(keys))
	type shardBatch struct {
		keys    [][]byte
		indexes []int
		shard   remotePebbleShardClient
	}

	batches := make([]shardBatch, len(r.shards))
	for i, key := range keys {
		shard := r.shardForKey(key)
		shardIdx := r.routingTable[r.bucketIdxForKey(key)]
		batches[shardIdx].shard = shard
		batches[shardIdx].keys = append(batches[shardIdx].keys, key)
		batches[shardIdx].indexes = append(batches[shardIdx].indexes, i)
	}

	r.MetricCollector.UpdateRemoteReadRequestNumber(len(batches))
	r.MetricCollector.UpdateRemoteReadKeyNumber(len(keys))

	start := time.Now()
	var wg sync.WaitGroup
	for i := range batches {
		if len(batches[i].keys) == 0 {
			continue
		}
		wg.Add(1)
		shardIdx := i
		go func() {
			defer wg.Done()
			batch := batches[shardIdx]
			values := r.remoteRead(batch.shard, batch.keys)
			if len(values) != len(batch.keys) {
				log.Fatalf("Remote read returned %d values, expected %d",
					len(values),
					len(batch.keys),
				)
			}
			for j, idx := range batch.indexes {
				result[idx] = values[j]
			}
		}()
	}
	wg.Wait()
	r.MetricCollector.UpdateRemoteReadTimePerRequest(time.Duration(r.averageReadTimePerRequest.Get()))
	r.MetricCollector.UpdateRemoteReadTime(time.Since(start))

	return result
}

func (r *RemotePebbleStateBackend) SetMany(keys [][]byte, values [][]byte) {
	if keys == nil || values == nil || len(keys) != len(values) {
		log.Fatalln("SetMany(): keys or values is nil or length mismatch")
	}

	type shardBatch struct {
		keys   [][]byte
		values [][]byte
		shard  remotePebbleShardClient
	}
	batches := make([]shardBatch, len(r.shards))
	for i := range keys {
		shardIdx := r.routingTable[r.bucketIdxForKey(keys[i])]
		batches[shardIdx].keys = append(batches[shardIdx].keys, keys[i])
		batches[shardIdx].values = append(batches[shardIdx].values, values[i])
		batches[shardIdx].shard = r.shards[shardIdx]
	}

	r.MetricCollector.UpdateRemoteWriteRequestNumber(len(batches))
	r.MetricCollector.UpdateRemoteWriteKeyNumber(len(keys))

	start := time.Now()
	var wg sync.WaitGroup
	for i := range batches {
		if len(batches[i].keys) > 0 {
			wg.Add(1)
			batch := batches[i]
			go func() {
				defer wg.Done()
				r.remoteWrite(batch.shard, batch.keys, batch.values, false)
			}()
		}
	}
	wg.Wait()
	r.MetricCollector.UpdateRemoteWriteTimePerRequest(time.Duration(r.averageWriteTimePerRequest.Get()))
	r.MetricCollector.UpdateRemoteWriteTime(time.Since(start))
}

func (r *RemotePebbleStateBackend) MergeMany(keys [][]byte, values [][]byte) {
	if keys == nil || values == nil || len(keys) != len(values) {
		log.Fatalln("MergeMany(): keys or values is nil or length mismatch")
	}
	type shardBatch struct {
		keys   [][]byte
		values [][]byte
		shard  remotePebbleShardClient
	}
	batches := make([]shardBatch, len(r.shards))
	for i := range keys {
		shardIdx := r.routingTable[r.bucketIdxForKey(keys[i])]
		batches[shardIdx].keys = append(batches[shardIdx].keys, keys[i])
		batches[shardIdx].values = append(batches[shardIdx].values, values[i])
		batches[shardIdx].shard = r.shards[shardIdx]
	}

	var wg sync.WaitGroup
	for i := range batches {
		if len(batches[i].keys) > 0 {
			wg.Add(1)
			batch := batches[i]
			go func() {
				defer wg.Done()
				r.remoteWrite(batch.shard, batch.keys, batch.values, true)
			}()
		}
	}
	wg.Wait()
}

func (r *RemotePebbleStateBackend) DeleteMany(keys [][]byte) {
	if keys == nil {
		log.Fatalln("Delete(): keys is nil")
	}

	type shardBatch struct {
		keys  [][]byte
		shard remotePebbleShardClient
	}
	batches := make([]shardBatch, len(r.shards))
	for i := range keys {
		shardIdx := r.routingTable[r.bucketIdxForKey(keys[i])]
		batches[shardIdx].keys = append(batches[shardIdx].keys, keys[i])
		batches[shardIdx].shard = r.shards[shardIdx]
	}

	var wg sync.WaitGroup
	for i := range batches {
		if len(batches[i].keys) > 0 {
			wg.Add(1)
			batch := batches[i]
			go func() {
				defer wg.Done()
				r.remoteDelete(batch.shard, batch.keys)
			}()
		}
	}
	wg.Wait()
}

func (r *RemotePebbleStateBackend) Close() {
	for _, shard := range r.shards {
		if shard.conn != nil {
			if err := shard.conn.Close(); err != nil {
				log.Fatalf("Failed to close remote pebble connection: %v", err)
			}
		}
	}
}

func (r *RemotePebbleStateBackend) GetIterator() StateIterator {
	log.Fatalln("GetIterator() not supported for remote pebble backend")
	return nil
}

func (r *RemotePebbleStateBackend) RangeQuery(
	lower, upper []byte,
) ([][]byte, [][]byte) {

	// TODO: need testing for this API - now blocking its use
	log.Fatalln("RangeQuery() not supported for remote pebble backend yet")

	if lower == nil || upper == nil {
		log.Fatalln("GetByRange(): lower or upper is nil")
	}

	resKeys := make([][]byte, 0)
	resValues := make([][]byte, 0)
	for _, shard := range r.shards {
		keys, values := r.remoteRangeQuery(shard, lower, upper)
		resKeys = append(resKeys, keys...)
		resValues = append(resValues, values...)
	}

	return resKeys, resValues
}

func (r *RemotePebbleStateBackend) IsEmbeddedState() bool {
	return false
}

/******************************************************************************
	 			Remote state access methods via gRPC unary calls
******************************************************************************/

func (r *RemotePebbleStateBackend) remoteRead(
	shard remotePebbleShardClient,
	keys [][]byte,
) [][]byte {
	resp, err := shard.client.Read(
		context.Background(),
		&pb.PebbleReadRequest{Keys: keys},
	)
	if err != nil {
		log.Fatalf(
			"Failed to receive remote read response (%s): %v",
			shard.addr,
			err,
		)
	}
	r.averageReadTimePerRequest.Inc(resp.ReadTime)
	return resp.Values
}

func (r *RemotePebbleStateBackend) remoteWrite(
	shard remotePebbleShardClient,
	keys [][]byte,
	values [][]byte,
	merge bool,
) {
	resp, err := shard.client.Write(
		context.Background(),
		&pb.PebbleWriteRequest{Keys: keys, Values: values, Merge: merge},
	)
	if err != nil {
		log.Fatalf(
			"Failed to receive remote write response (%s): %v",
			shard.addr,
			err,
		)
	}
	r.averageWriteTimePerRequest.Inc(resp.WriteTime)
}

func (r *RemotePebbleStateBackend) remoteDelete(
	shard remotePebbleShardClient,
	keys [][]byte,
) {
	if _, err := shard.client.Delete(
		context.Background(),
		&pb.PebbleDeleteRequest{Keys: keys},
	); err != nil {
		log.Fatalf(
			"Failed to receive remote delete response (%s): %v",
			shard.addr,
			err,
		)
	}
}

func (r *RemotePebbleStateBackend) remoteRangeQuery(
	shard remotePebbleShardClient,
	lower []byte,
	upper []byte,
) ([][]byte, [][]byte) {
	resp, err := shard.client.RangeQuery(
		context.Background(),
		&pb.PebbleRangeQueryRequest{Lower: lower, Upper: upper},
	)
	if err != nil {
		log.Fatalf(
			"Failed to receive remote range query response (%s): %v",
			shard.addr,
			err,
		)
	}

	return resp.Keys, resp.Values
}
