package main

import (
	"log"
	"os"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/dataflow"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/user"
	"github.com/CASP-Systems-BU/disaggregated-streaming/worker"
)

func main() {

	if len(os.Args) != 3 && len(os.Args) != 5 {
		log.Fatalln(
			"Usage: ./worker <data-plane-port> <state-comm-port> <optional: query-name> <optional: query-config-file>",
		)
	}
	dataPlanePort := os.Args[1]
	stateCommPort := os.Args[2]

	// Read the configuration from .yaml file
	config := configuration.ReadConfig()

	// Set the data plane port
	config.DataPlanePort = dataPlanePort

	// Set the state comm port
	config.StateCommPort = stateCommPort

	// Get the user-defined dataflow
	var df *dataflow.Dataflow
	if len(os.Args) == 5 {
		df = user.DefineDataflow(os.Args[3], os.Args[4])
	} else {
		df = user.DefaultDataflow()
	}

	w := worker.NewWorker(config, df)
	w.Run()
}
