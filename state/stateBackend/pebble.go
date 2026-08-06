package stateBackend

import (
	"bytes"
	"errors"
	"log"
	"sync"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/cockroachdb/pebble"
	"github.com/panjf2000/ants/v2"
)

type PebbleStateBackend struct {
	sync.Mutex

	db           *pebble.DB
	writeOptions *pebble.WriteOptions
	path         string

	// Concurrency config for GetMany() api
	enableConcurrentRead  bool
	getManyRoutinePool    *ants.Pool
	getManyMaxConcurrency int
	getManyBatchSize      int
}

var _ StateBackend = (*PebbleStateBackend)(nil)

func NewPebbleStateBackend(
	config *configuration.Configuration,
) *PebbleStateBackend {

	// Hardcode the pebble db path. DataPlanePort is added to avoid conflict on
	// concurrent local workers
	path := "data/pebble/" + config.DataPlanePort + "localPebble.DB"

	maxConcurrentCompactions := config.PebbleMaxConcurrentCompactions
	db, err := pebble.Open(path, &pebble.Options{
		DisableWAL:                config.PebbleDisableWAL,
		MemTableSize:              config.PebbleMemTableSize,
		Cache:                     pebble.NewCache(512 << 20),
		MaxConcurrentCompactions:  func() int { return maxConcurrentCompactions },
		LBaseMaxBytes:             config.PebbleLBaseMaxBytes,
		MemTableStopWritesThreshold: config.PebbleMemTableStopWritesThreshold,
	})
	if err != nil {
		log.Fatalf("Failed to open pebble db: %v", err)
	}

	pebbleStateBackend := &PebbleStateBackend{
		db: db,
		writeOptions: &pebble.WriteOptions{
			Sync: config.PebbleSyncOnWrite,
		},
		path:                 path,
		enableConcurrentRead: config.PebbleEnableConcurrentGetMany,
	}

	// Init routine pool for GetMany() if concurrency is enabled
	if config.PebbleEnableConcurrentGetMany {

		if config.PebbleGetManyMaxConcurrency <= 0 {
			log.Fatalf("Invalid PebbleGetManyMaxConcurrency: %d\n",
				config.PebbleGetManyMaxConcurrency)
		}
		if config.PebbleGetManyBatchSize <= 0 {
			log.Fatalf("Invalid PebbleGetManyBatchSize: %d\n",
				config.PebbleGetManyBatchSize)
		}
		getManyPool, err := ants.NewPool(config.PebbleGetManyMaxConcurrency)
		if err != nil {
			log.Fatalf("Failed to create pebble GetMany pool: %v", err)
		}

		pebbleStateBackend.getManyRoutinePool = getManyPool
		pebbleStateBackend.getManyMaxConcurrency = config.PebbleGetManyMaxConcurrency
		pebbleStateBackend.getManyBatchSize = config.PebbleGetManyBatchSize
	}

	return pebbleStateBackend
}

/******************************************************************************
	 					Implement StateBackend interface
******************************************************************************/

func (p *PebbleStateBackend) Get(key []byte) []byte {

	// If key is nil, pebble will take it as []byte{} automatically
	bytesValue, closer, err := p.db.Get(key)
	if err != nil {
		// If key not found, return nil. If nil is stored as value, it will
		// return []byte{} instead of nil
		if errors.Is(err, pebble.ErrNotFound) {
			return nil
		} else {
			log.Fatalf("Failed to get key: %v", err)
		}
	}

	// Copy the bytesValue to avoid the bytesValue being changed by the closer
	newBytes := bytes.Clone(bytesValue)

	err = closer.Close()
	if err != nil {
		log.Fatalf("Failed to close the closer: %v", err)
	}
	return newBytes
}

func (p *PebbleStateBackend) Set(key []byte, value []byte) {

	// If key or value is nil, pebble will take it as []byte{} automatically

	err := p.db.Set(key, value, p.writeOptions)
	if err != nil {
		log.Fatalf("Failed to set key: %v", err)
	}
}

func (p *PebbleStateBackend) GetMany(keys [][]byte) [][]byte {
	if keys == nil {
		log.Fatalf("GetMany(): keys is nil")
	}

	result := make([][]byte, len(keys))
	if len(keys) == 0 {
		return result
	}

	// Single routine read if concurrent read is disabled
	if !p.enableConcurrentRead {
		for i := range keys {
			result[i] = p.Get(keys[i])
		}
		return result
	}

	expectedBatchSize := p.getManyBatchSize
	maxRoutines := p.getManyMaxConcurrency

	// Partition the keys into batches for concurrent pebble read. The number
	// of batches is upper bounded by max routines and lower constrained by the
	// expected batch size
	numBatches := min(
		(len(keys)+expectedBatchSize-1)/expectedBatchSize,
		maxRoutines,
	)

	// Evenly distribute keys into all batches
	batchSize := (len(keys) + numBatches - 1) / numBatches

	// Concurrent read batches in goroutines
	var wg sync.WaitGroup
	wg.Add(numBatches)
	for i := 0; i < len(keys); i += batchSize {

		// Get start and end index for the batch
		batchStart := i
		batchEnd := min(i+batchSize, len(keys))

		err := p.getManyRoutinePool.Submit(func() {
			defer wg.Done()
			for j := batchStart; j < batchEnd; j++ {
				result[j] = p.Get(keys[j])
			}
		})
		if err != nil {
			log.Fatalf("Failed to submit GetMany task: %v", err)
		}
	}
	wg.Wait()
	return result
}

func (p *PebbleStateBackend) SetMany(keys [][]byte, values [][]byte) {
	if keys == nil || values == nil {
		log.Fatalln("SetMany(): keys or values is nil")
	}

	if len(keys) != len(values) {
		log.Fatalln("SetMany(): len(keys) != len(values)")
	}

	batch := p.db.NewBatch()
	var err error
	for i := range keys {
		err = batch.Set(keys[i], values[i], p.writeOptions)
		if err != nil {
			log.Fatalf("Failed to set key in batch: %v", err)
		}
	}

	err = batch.Commit(p.writeOptions)
	if err != nil {
		log.Fatalf("Failed to commit batch: %v", err)
	}
}

func (p *PebbleStateBackend) MergeMany(keys [][]byte, values [][]byte) {
	if keys == nil || values == nil {
		log.Fatalln("MergeMany(): keys or values is nil")
	}

	if len(keys) != len(values) {
		log.Fatalln("MergeMany(): len(keys) != len(values)")
	}

	batch := p.db.NewBatch()
	defer batch.Close()
	var err error
	for i := range keys {
		err = batch.Merge(keys[i], values[i], p.writeOptions)
		if err != nil {
			log.Fatalf("Failed to merge key in batch: %v", err)
		}
	}

	err = batch.Commit(p.writeOptions)
	if err != nil {
		log.Fatalf("Failed to commit batch: %v", err)
	}
}

func (p *PebbleStateBackend) DeleteMany(keys [][]byte) {
	if keys == nil {
		log.Fatalln("Delete(): keys is nil")
	}

	batch := p.db.NewBatch()
	var err error
	for i := range keys {
		err = batch.Delete(keys[i], p.writeOptions)
		if err != nil {
			log.Fatalf("Failed to delete key in batch: %v", err)
		}
	}

	err = batch.Commit(p.writeOptions)
	if err != nil {
		log.Fatalf("Failed to commit batch: %v", err)
	}
}

func (p *PebbleStateBackend) Close() {

	if p.getManyRoutinePool != nil {
		p.getManyRoutinePool.Release()
	}

	err := p.db.Close()
	if err != nil {
		log.Fatalf("Failed to close pebble db: %v", err)
	}
}

// TODO: check the concurrent access safety issue if multiple iterators are
// accessing pebble data concurrently
func (p *PebbleStateBackend) GetIterator() StateIterator {

	iter, err := p.db.NewIter(nil)
	if err != nil {
		log.Fatalf("Failed to get pebble iterator: %v", err)
	}

	return iter
}

// Range query given the lower and upper bound: [lower, upper)
func (p *PebbleStateBackend) RangeQuery(
	lower, upper []byte,
) ([][]byte, [][]byte) {

	// Check the input correctness
	if lower == nil || upper == nil {
		log.Fatalln("GetByRange(): lower or upper is nil")
	}

	// Get the iterator
	iter, err := p.db.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})

	if err != nil {
		log.Fatalf("Failed to get pebble iterator: %v", err)
	}
	defer iter.Close()

	resKeys := make([][]byte, 0)
	resValues := make([][]byte, 0)

	// Seek to the lower bound and iterate until the upper bound
	for iter.SeekGE(lower); iter.Valid(); iter.Next() {

		key := iter.Key()
		value := iter.Value()

		resKeys = append(resKeys, bytes.Clone(key))
		resValues = append(resValues, bytes.Clone(value))
	}

	if err := iter.Error(); err != nil {
		log.Fatalf("Failed to iterate pebble: %v", err)
	}

	return resKeys, resValues
}
