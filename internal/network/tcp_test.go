package network_test

import (
	"io"
	"log"
	"net"
	"sync"
	"testing"

	"github.com/CASP-Systems-BU/koala/internal/network"
)

// Test the connection close handling
func TestCloseConnection(t *testing.T) {

	// Wait group
	wg := sync.WaitGroup{}

	// Start server with close handling
	wg.Add(1)
	go runServer(&wg)

	// Start client, send some data, and close
	wg.Add(1)
	go runClient(&wg)

	// Wait for all routines to stop
	wg.Wait()
	log.Println("TestCloseConnection done")
}

func runClient(wg *sync.WaitGroup) {
	defer wg.Done()

	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		log.Fatalf("Error tcp dialing: %v", err)
	}

	// Send 2 bytes
	err = network.WriteAll(conn, []byte("hi"))
	if err != nil {
		log.Fatalf("Error tcp writing: %v", err)
	}

	// Close the connection
	conn.Close()
}

func runServer(wg *sync.WaitGroup) {
	defer wg.Done()

	listener, err := net.Listen("tcp", ":9999")
	if err != nil {
		log.Fatalf("Error tcp sending: %v", err)
	}
	defer listener.Close()

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("[server] server closed")
			return
		}
		wg.Add(1)
		go requestHandler(conn, listener, wg)
	}
}

func requestHandler(
	connection net.Conn,
	listener net.Listener,
	wg *sync.WaitGroup,
) {
	defer wg.Done()

	buf := make([]byte, 2)

	for {
		err := network.ReadAll(connection, buf, 2)
		if err != nil {
			if err == io.EOF {
				log.Println("[server] Connection gracefully closed")
				listener.Close()
				return
			} else {
				log.Fatalf("Error tcp reading1: %v", err)
			}
		}
		log.Println("[server] Read: ", string(buf))
	}
}
