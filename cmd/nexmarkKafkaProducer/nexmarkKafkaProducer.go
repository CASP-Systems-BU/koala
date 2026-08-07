package main

import (
	"os"
	"strconv"

	"github.com/CASP-Systems-BU/koala/query/nexmark/nexmarkKafkaProducer"
)

// Kafka producer executable for the Nexmark benchmark.
// It takes 2 command-line inputs:
// 1. Which Nexmark query this producer process is used for
// 2. The replicaID of this producer process

func main() {

	// Specify the Nexmark query config JSON file path. Assume the working
	// directory is the scripts/ folder. Example:
	// ./nexmarkKafkaProducer nexmarkJson/query1.json 0
	jsonPath := os.Args[1]

	// Specify the replicaID for the producer
	replicaID, _ := strconv.ParseInt(os.Args[2], 10, 64)

	producer := nexmarkKafkaProducer.NewNexmarkKafkaProducer(
		jsonPath,
		replicaID,
	)
	producer.Run()
}
