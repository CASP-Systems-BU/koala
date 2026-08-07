package stateBackend

import (
	"context"
	"log"
	"sync"

	"github.com/CASP-Systems-BU/koala/internal/configuration"
	tikvConfig "github.com/tikv/client-go/v2/config"
	"github.com/tikv/client-go/v2/rawkv"
)

// TODO: add tests
// TODO: test what happens nil is passed in for key or value in TiKV

type TikvStateBackend struct {
	sync.Mutex

	client *rawkv.Client
}

var _ StateBackend = (*TikvStateBackend)(nil)

func NewTikvStateBackend(
	config *configuration.Configuration,
) *TikvStateBackend {

	cli, err := rawkv.NewClient(
		context.TODO(),
		[]string{config.TiKVAddr},
		tikvConfig.DefaultConfig().Security,
	)
	if err != nil {
		log.Fatalf("Failed to connect to TiDB: %v", err)
	}

	log.Println("Successfully connected to tikv")
	return &TikvStateBackend{client: cli}
}

/******************************************************************************
	 					Implement StateBackend interface
******************************************************************************/

func (t *TikvStateBackend) Get(key []byte) []byte {

	// TODO: check behavior for key == nil
	if key == nil {
		key = []byte{}
	}

	// If key not found, TiKV returns nil directly
	// val here is safe to use as compared to Pebble
	val, err := t.client.Get(context.TODO(), key)
	if err != nil {
		log.Fatalf("Failed to get key: %v", err)
	}

	return val
}

func (t *TikvStateBackend) Set(key []byte, value []byte) {

	// TODO: check behavior for key or val == nil
	if key == nil {
		key = []byte{}
	}
	if value == nil {
		value = []byte{}
	}

	err := t.client.Put(context.TODO(), key, value)
	if err != nil {
		log.Fatalf("Failed to put key: %v", err)
	}
}

func (t *TikvStateBackend) DeleteMany(keys [][]byte) {
	err := t.client.BatchDelete(context.TODO(), keys)
	if err != nil {
		log.Fatalf("Failed to batch delete: %v", err)
	}
}

func (t *TikvStateBackend) GetMany(keys [][]byte) [][]byte {
	if keys == nil {
		log.Fatalf("GetMany(): keys is nil")
	}

	// BatchGet from tikv
	values, err := t.client.BatchGet(context.TODO(), keys)
	if err != nil {
		log.Fatalf("Failed to batch get: %v", err)
	}

	return values
}

func (t *TikvStateBackend) SetMany(keys [][]byte, values [][]byte) {
	if keys == nil || values == nil {
		log.Fatalln("SetMany(): keys or values is nil")
	}

	err := t.client.BatchPut(context.TODO(), keys, values)
	if err != nil {
		log.Fatalf("Failed to batch put: %v", err)
	}
}

func (t *TikvStateBackend) MergeMany(keys [][]byte, values [][]byte) {
	log.Fatalln("MergeMany() not supported for TiDB")
}

func (t *TikvStateBackend) Close() {

	err := t.client.Close()
	if err != nil {
		log.Fatalf("Failed to close the client: %v", err)
	}
}

func (t *TikvStateBackend) GetIterator() StateIterator {
	log.Fatalln("GetIterator() not supported for TiDB")
	return nil
}

func (t *TikvStateBackend) RangeQuery(
	start []byte,
	end []byte,
) ([][]byte, [][]byte) {
	log.Fatalln("RangeQuery() not supported for TiDB yet")
	return nil, nil
}
