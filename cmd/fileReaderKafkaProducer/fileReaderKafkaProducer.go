package main

import (
	"os"
	"strconv"

	fileReaderKafkaProducer "github.com/CASP-Systems-BU/disaggregated-streaming/query/fileReaderKafkaProducer"
)

// Kafka producer executable for the Nexmark benchmark.
// It takes 2 command-line inputs:
// 1. Which Nexmark query this producer process is used for
// 2. The replicaID of this producer process

func main() {

	// Specify the query config JSON file path. Assume the working
	// directory is the disaggregated-streaming/ folder. Example:
	// ./fileReaderKafkaProducer scripts/filerReaderJson/taxi.json 0
	jsonPath := os.Args[1]

	// Specify the replicaID for the producer, replicaID tells the producer
	// which file to read
	replicaID, _ := strconv.ParseInt(os.Args[2], 10, 64)

	producer := fileReaderKafkaProducer.NewFileReaderKafkaProducer(
		jsonPath,
		replicaID,
	)
	producer.Run()
}
