package user

import (
	"log"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	azure "github.com/CASP-Systems-BU/disaggregated-streaming/query/azure/query"
	borg "github.com/CASP-Systems-BU/disaggregated-streaming/query/borg/query"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/examples"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query1"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query2"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query3"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query4"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query5"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query6"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query7"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/nexmark/query/query8"
	taxi "github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/query"
	twitch "github.com/CASP-Systems-BU/disaggregated-streaming/query/twitch/query"
)

/******************************************************************************
				 This is the user space to define the dataflow
******************************************************************************/

// Get the user-defined dataflow based on command line arguments
func DefineDataflow(
	queryName string,
	queryConfigFile string,
) *dataflow.Dataflow {

	switch queryName {
	case "nexmark_query1":
		return query1.Query1Kafka(queryConfigFile)
	case "nexmark_query2":
		return query2.Query2Kafka(queryConfigFile)
	case "nexmark_query3":
		return query3.Query3Kafka(queryConfigFile)
	case "nexmark_query3_warmup":
		return query3.Query3WarmUp(queryConfigFile)
	case "nexmark_query4":
		return query4.Query4Kafka(queryConfigFile)
	case "nexmark_query4_modified":
		return query4.Query4ModKafka(queryConfigFile)
	case "nexmark_query5":
		return query5.Query5Kafka(queryConfigFile)
	case "nexmark_query6":
		return query6.Query6Kafka(queryConfigFile)
	case "nexmark_query6_modified":
		return query6.Query6ModKafka(queryConfigFile)
	case "nexmark_query7":
		return query7.Query7Kafka(queryConfigFile)
	case "nexmark_query8":
		return query8.Query8Kafka(queryConfigFile)
	case "taxi":
		return taxi.Taxi(queryConfigFile)
	case "taxi_warmup":
		return taxi.TaxiWarmup(queryConfigFile)
	case "twitch":
		return twitch.TwitchPipeline(queryConfigFile)
	case "borg":
		return borg.BusyMachine(queryConfigFile)
	case "borg_warmup":
		return borg.BusyMachineWarmup(queryConfigFile)
	case "azure":
		return azure.CPUReadings(queryConfigFile)
	case "azure_warmup":
		return azure.CPUReadingsWarmup(queryConfigFile)
	default:
		log.Fatalln("Specified query name has no match")
	}
	return nil
}

// Default dataflow if query is not specified as a command line argument
func DefaultDataflow() *dataflow.Dataflow {

	log.Println("[Job INFO] run default dataflow")
	return examples.Example_Count_Query()
}
