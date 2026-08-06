package kafka

import (
	"log"
	"net"
	"reflect"
	"time"

	ta "github.com/CASP-Systems-BU/disaggregated-streaming/api/timestampAssigner"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/buffer"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/network"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/supplier"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/utils"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/watermark"
	"github.com/CASP-Systems-BU/disaggregated-streaming/metric"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// How often to report the output rate of the Kafka producer for monitoring
const OutputRateReportInterval = 5 * time.Second

// Structure to send messages to a Kafka topic
type KafkaCollector[OUT tuple.Tuple] struct {

	// Underlying Kafka producer to send messages
	producer *kafka.Producer

	// Kafka topic to produce messages to
	topic string

	// Kafka topic partition idx this producer will send messages to. In our
	// setup, we require that producer and partition have a 1-1 mapping to
	// preserve message ordering
	partitionIdx int32

	// Codec for serializing messages
	encoder network.TupleEncoder
	sizer   network.TupleSizer

	// Metric utility to track output rate
	outputRate *metric.Rate
}

func NewKafkaCollector[OUT tuple.Tuple](
	producerConfig *kafka.ConfigMap,
	topic string,
	partitionIdx int32,
) *KafkaCollector[OUT] {

	producer, err := kafka.NewProducer(producerConfig)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}

	// Create codec for output data type
	var zero OUT
	encoder := network.CreateEncoder(reflect.TypeOf(zero).Elem())
	sizer := network.CreateSizer(reflect.TypeOf(zero).Elem())

	return &KafkaCollector[OUT]{
		producer:     producer,
		topic:        topic,
		partitionIdx: partitionIdx,
		encoder:      encoder,
		sizer:        sizer,
		outputRate:   metric.NewRate(OutputRateReportInterval),
	}
}

func (kc *KafkaCollector[OUT]) Emit(t tuple.Tuple) {

	if t != nil {
		// Serialize the tuple
		buf := make([]byte, kc.sizer(t))
		kc.encoder(t, buf)

		// Send the message to Kafka
		err := kc.producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{
				Topic:     &kc.topic,
				Partition: kc.partitionIdx,
			},
			Value: buf,
		}, nil)
		if err != nil {
			log.Fatalf("Failed to produce message: %v", err)
		}

		// Update output rate metric
		kc.outputRate.Inc(1)
	}
}

// Routine executor to periodically log the output rate of this Kafka producer
func (kc *KafkaCollector[OUT]) ReportOutputRate() {

	// Get local IP address for logging
	localIP := getLocalPrivateIP()

	for {
		time.Sleep(OutputRateReportInterval)
		log.Printf(
			"Kafka producer (%s) output rate: %v",
			localIP,
			kc.outputRate.Get(),
		)
	}
}

// Helper function to get local private IP address for logging
func getLocalPrivateIP() string {

	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Fatalln("Error getting network interfaces: ", err)
	}

	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}

		if ip == nil || ip.IsLoopback() {
			continue // skip loopback addresses
		}

		if ip.To4() != nil { // only IPv4

			ipStr := ip.String()
			// If the ip str starts with "10." or "192.", we consider it a
			// private IP
			if len(ipStr) >= 4 &&
				(ipStr[0:3] == "10." || ipStr[0:4] == "192.") {
				return ipStr
			}
		}
	}
	log.Fatalln("No private ip found for producer")
	return ""
}

/******************************************************************************
			 Dummy methods to implement the full Collector interface
******************************************************************************/

func (kc *KafkaCollector[OUT]) Setup(params *utils.OperatorSetupParas) {
	log.Fatalln("KafkaCollector.Setup() not implemented")
}

func (kc *KafkaCollector[OUT]) PauseEmit() {
	log.Fatalln("KafkaCollector.PauseEmit() not implemented")
}

func (kc *KafkaCollector[OUT]) ResumeEmit() {
	log.Fatalln("KafkaCollector.ResumeEmit() not implemented")
}

func (kc *KafkaCollector[OUT]) UpdateRouting(
	downstreamsToAdd []*pb.DownstreamInfo,
	downstreamsToRemove []*pb.DownstreamInfo,
	table *pb.SerializedPartitionTable,
) {
	log.Fatalln("KafkaCollector.UpdateRouting() not implemented")
}

func (kc *KafkaCollector[OUT]) Terminate() {
	log.Fatalln("KafkaCollector.Terminate() not implemented")
}

func (kc *KafkaCollector[OUT]) BroadcastWatermark(
	wm *buffer.Watermark,
	flag bool,
) {
	log.Fatalln("KafkaCollector.BroadcastWatermark() not implemented")
}

func (kc *KafkaCollector[OUT]) SetTsAssignerAndWatermarkGenerator(
	assigner ta.TimestampAssignerAPI,
	generator *watermark.PeriodicWatermarkGenerator,
) {
	log.Fatalln(
		"KafkaCollector.SetTsAssignerAndWatermarkGenerator() not implemented",
	)
}

func (kc *KafkaCollector[OUT]) GetWatermarkGenerator() *watermark.PeriodicWatermarkGenerator {
	log.Fatalln("KafkaCollector.GetWatermarkGenerator() not implemented")
	return nil
}

func (kc *KafkaCollector[OUT]) GetTsAssigner() (ta.TimestampAssignerAPI, bool) {
	log.Fatalln("KafkaCollector.GetTsAssigner() not implemented")
	return nil, false
}

func (kc *KafkaCollector[OUT]) SetCurrentTimestamp(ts int64) {
	log.Fatalln("KafkaCollector.SetCurrentTimestamp() not implemented")
}

func (kc *KafkaCollector[OUT]) ConstructSubSupplier() supplier.SubSupplier {
	log.Fatalln("KafkaCollector.ConstructSubSupplier() not implemented")
	return nil
}
