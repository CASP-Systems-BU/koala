
PROTO_DIR := ./internal/grpc
PROTO_FILES := $(wildcard $(PROTO_DIR)/*.proto)

# Build binaries
build: build_worker build_coordinator build_client build_kafka_producer build_nexmark_kafka_producer build_file_reader_kafka_producer

build_worker: cmd/worker/worker.go
	go build -o ./bin/worker cmd/worker/worker.go

build_coordinator: cmd/coordinator/coordinator.go
	go build -o ./bin/coordinator cmd/coordinator/coordinator.go

build_client: cmd/client/client.go
	go build -o ./bin/client cmd/client/client.go

build_kafka_producer: cmd/kafkaProducer/kafkaProducer.go
	go build -o ./bin/kafkaProducer cmd/kafkaProducer/kafkaProducer.go

build_nexmark_kafka_producer: cmd/nexmarkKafkaProducer/nexmarkKafkaProducer.go
	go build -o ./bin/nexmarkKafkaProducer cmd/nexmarkKafkaProducer/nexmarkKafkaProducer.go
build_file_reader_kafka_producer: cmd/fileReaderKafkaProducer/fileReaderKafkaProducer.go
	go build -o ./bin/fileReaderKafkaProducer cmd/fileReaderKafkaProducer/fileReaderKafkaProducer.go


# Build gRPC
pb: $(PROTO_FILES)
	protoc \
		--go_out=. \
		--go_opt=paths=source_relative \
		--go-grpc_out=. \
		--go-grpc_opt=paths=source_relative \
		$^

# Clean
clean:
	rm -rf ./bin

clean_pb:
	rm -rf ./internal/controlplane/*.pb.go