package main

import (
	"log"
	"os"

	remotepebble "github.com/CASP-Systems-BU/disaggregated-streaming/cmd/remotePebble/remotePebbleServer"
	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	"github.com/CASP-Systems-BU/disaggregated-streaming/state/stateBackend"
)

func main() {

	config := configuration.ReadConfig()
	if len(os.Args) != 2 {
		log.Fatalln(
			"StateCommPort must be set for remote pebble server. Usage : ./remotePebble <remote-pebble-comm-port>",
		)
	}

	stateCommPort := os.Args[1]
	config.StateCommPort = stateCommPort
	pebbleStore := stateBackend.NewPebbleStateBackend(config)
	listenAddr := "0.0.0.0:" + stateCommPort
	remotepebble.ServeRemotePebbleServer(listenAddr, pebbleStore)
}
