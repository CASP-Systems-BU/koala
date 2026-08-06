package main

import (
	"context"
	"log"
	"os"
	"strconv"

	"github.com/CASP-Systems-BU/disaggregated-streaming/internal/configuration"
	pb "github.com/CASP-Systems-BU/disaggregated-streaming/internal/grpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	// Client sends 2 types of requests to the Coordinator API service:
	// 1. Deploy a job: Coordinator RunJob() unary RPC
	// 2. Rescale a job: Coordinator Rescale() unary RPC

	if len(os.Args) < 2 {
		log.Fatalln("Usage: ./client.go <action> <optional-args>")
		log.Fatalln("       action: deploy, rescale")
	}

	switch os.Args[1] {
	case "deploy":

		// Just deploy the job and return
		log.Println("[Client INFO] Deploy the job ...")
		deployJob()
	case "rescale":

		// Get target operator and target parallelism from command line args
		if len(os.Args) != 4 {
			log.Fatalln(
				"Usage: ./client.go rescale <target-operator> <target-parallelism>",
			)
		}
		targetOperator := os.Args[2]
		targetParallelism, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			log.Fatalf("Invalid target parallelism: %v\n", err)
		}

		// Rescale the job
		log.Println("[Client INFO] Rescale the job ...")
		rescaleJob(targetOperator, targetParallelism)
	default:
		log.Fatalf("Unknown action: %s\n", os.Args[1])
	}
}

// Deploy the job
func deployJob() {

	// Connect to the coordinator API service
	client := connectCoordinator()

	jobConfig := &pb.JobConfig{}
	resp, err := client.RunJob(context.Background(), jobConfig)
	if err != nil {
		log.Fatalf("Failed to deploy the job: %v", err)
	}

	// Print the response
	log.Printf("Job deployment response: %v\n", resp.Info)
}

// Rescale the job
func rescaleJob(targetOperator string, targetParallelism int64) {

	// Connect to the coordinator API service
	client := connectCoordinator()

	rescaleConfig := &pb.RescaleConfig{
		TargetRescaleOp:   targetOperator,
		TargetParallelism: targetParallelism,
	}
	resp, err := client.Rescale(context.Background(), rescaleConfig)
	if err != nil {
		log.Fatalf("Failed to rescale the job: %v", err)
	}

	// Print the response
	log.Printf("Job rescale response: %v\n", resp.Info)
}

// Connect to Coordinator
func connectCoordinator() pb.APIServiceClient {

	// Read config from config file
	config := configuration.ReadConfig()

	// Connect to the coordinator API service
	conn, err := grpc.NewClient(
		config.CoordinatorAPIAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("Failed to connect to the Coordinator API service: %v", err)
	}
	return pb.NewAPIServiceClient(conn)
}
