package main

import (
	"log"
	"os"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
	"github.com/CASP-Systems-BU/koala/coordinator"
	"github.com/CASP-Systems-BU/koala/internal/configuration"
	"github.com/CASP-Systems-BU/koala/user"
)

func main() {

	if len(os.Args) != 1 && len(os.Args) != 3 {
		log.Fatalln(
			"Usage: ./coordinator <optional: query-name> <optional: query-config-file>",
		)
	}

	// Read config from config file
	config := configuration.ReadConfig()

	// Get the user-defined dataflow
	var df *dataflow.Dataflow
	if len(os.Args) == 3 {
		df = user.DefineDataflow(os.Args[1], os.Args[2])
	} else {
		df = user.DefaultDataflow()
	}

	c := coordinator.NewCoordinator(config, df)
	c.Run()
}
