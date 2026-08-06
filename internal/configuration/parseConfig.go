package configuration

import (
	"io"
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

func ReadConfig() *Configuration {

	// Open the YAML configuration file
	file, err := os.Open("./config.yaml")
	if err != nil {
		log.Fatalf("failed to open file: %v\n", err)
	}
	defer file.Close()

	// Read the contents of the file into a byte slice
	yfile, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("failed to read file: %v\n", err)
	}

	// Create a default instance of Configuration
	config := Default()

	// Unmarshal the YAML data into the config struct
	err = yaml.Unmarshal(yfile, config)
	if err != nil {
		log.Fatalf("failed to unmarshal YAML: %v\n", err)
	}

	return config
}
