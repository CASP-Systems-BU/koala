package nexmarkKafkaProducer

import "github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/config"

// This struct is read by NexmarkKafkaProducer executbale to configure how to
// produce Nexmark events to Kafka topics

// Mapping from Nexmark event type to Kafka topic name
var NexmarkEventTypeToTopic = map[config.EventType]string{
	config.Person:        "nexmark-person",
	config.Auction:       "nexmark-auction",
	config.Bid:           "nexmark-bid",
	config.ClosedAuction: "nexmark-closed-auction",
}

type NexmarkKafkaProducerConfig struct {

	// Kafka server IP addresses - we use the 1st IP as the bootstrap server
	KafkaClusterIPs []string

	// Producer IP addresses - we only care about len(ProducerIPs) as the
	// parallelism of producers. This parallelism is used for:
	// 1. Each producer replica needs the global parallelism info to only
	//    generate a disjoint subset of records
	// 2. Producer is responsible for creating the Kafka topic if it does not
	//    exist. The topic needs to be created with number of partitions equal
	//    to the producer parallelism to ensure 1-1 mapping between producer
	//    replicas and Kafka partitions
	ProducerIPs []string

	// Logical source (event type) configurations. For queries with single
	// logical source, len(NexmarkLogicalSourceConfigs) = 1, For queries with
	// 2 logical sources, len(NexmarkLogicalSourceConfigs) = 2
	NexmarkLogicalSourceConfigs []NexmarkLogicalSourceConfig
}

// A NexmarkKafkaProducer instance (process) can generate one or multiple types
// of the event e.g. Person, Auction, Bid, ClosedAuction. Each event type or a
// logical source is handled by a single routine within the process. Each event
// type corresponds to a different topic in Kafka
type NexmarkLogicalSourceConfig struct {

	// Type of the Nexmark event
	EventType config.EventType

	// Configurations for this event type
	NexmarkSourceConfig *config.NexmarkSourceConfig
}
