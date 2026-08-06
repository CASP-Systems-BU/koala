package worker

import (
	"log"
	"net"
)

// Start the tcp server to establish data channels from upstream tasks
func (w *Worker) startDataPlaneService() {

	// Start listening on the data plane port
	listener, err := net.Listen("tcp", ":"+w.Config.DataPlanePort)
	if err != nil {
		log.Fatalf("Error tcp sending aaaa: %v", err)
	}
	defer listener.Close()
	log.Printf(
		"Data plane service starts listening on port %s ...\n",
		w.Config.DataPlanePort,
	)

	for {
		// Accept a connection
		conn, err := listener.Accept()
		if err != nil {
			log.Fatalf("Error tcp accepting request: %v", err)
		}

		go w.RequestHandler(conn)
	}
}

// Handler for an incoming upstream connection
// This handler routine terminates once a new upstream is added
func (w *Worker) RequestHandler(connection net.Conn) {

	// Wait until the task is successfully assigned to this Worker
	w.TaskAssigned.Wait()

	// Now we guarantee w.AssignedTask has been initialized!
	// We can proceed to add an upstream to it
	w.AssignedTask.InitNewUpstream(connection)
}
