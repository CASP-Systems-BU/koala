package fileReaderKafkaProducer

import (
	"encoding/json"
	"errors"

	ratelimiter "github.com/CASP-Systems-BU/disaggregated-streaming/query/rateLimiter"
)

// All configuration parameters required to start a NexmarkKafkaProducer
// instance. These parameters can be read from a json file
type EventType int

const (
	Twitch EventType = iota
	Taxi
	Borg
	Azure
)

var FileReaderEventTypeToTopic = map[EventType]string{
	Twitch: "twitch",
	Taxi:   "taxi-trip",
	Borg:   "borg-task",
	Azure:  "azure",
}

type FileReaderKafkaProducerConfig struct {

	// Kafka server IP addresses - we use the 1st IP as the bootstrap server
	KafkaClusterIPs []string

	// Producer IP addresses - we only care about len(ProducerIPs)
	ProducerIPs []string

	// Type of the event
	EventType EventType
	// Ratelimit related configs
	RateLimiterConfig ratelimiter.RateLimiterConfig
	// File paths that stores the realworld data
	FilePath []string
	// TotalEvents this producer will produce, 0 means infinity
	NumEvents int64
	// StartPoint of the generator, from which event to start generate
	OutputEventNumber int64

	// Whether this producer is for warmup (producing dummy records) or not
	IsWarmUp bool	
}

/******************************************************************************
						 Custom JSON unmarshal methods
******************************************************************************/
// Convert EventType from string to int
func (e *EventType) UnmarshalJSON(data []byte) error {

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	switch s {
	case "Taxi":
		*e = Taxi
	case "Twitch":
		*e = Twitch
	case "Borg":
		*e = Borg
	case "Azure":
		*e = Azure
	default:
		return errors.New("Invalid EventType string: " + s)
	}
	return nil
}
