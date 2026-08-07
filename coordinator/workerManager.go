package coordinator

import (
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/CASP-Systems-BU/koala/api/dataflow"
)

type WorkerManager struct {

	// WorkerManager operation is synchronized
	sync.Mutex

	// List of workers managed by the coordinator: map worker ID to worker
	Workers map[uint16]*ManagedWorker

	// View of workers grouped by IP address - still the same worker pointers
	// as in Workers map
	WorkersByIP map[string][]*ManagedWorker

	// The next worker ID to be assigned when new worker registers - monotonic
	// increasing
	NextWorkerId uint16
}

func NewWorkerManager() *WorkerManager {
	// Initialize the worker manager
	return &WorkerManager{
		Workers:      make(map[uint16]*ManagedWorker),
		WorkersByIP:  make(map[string][]*ManagedWorker),
		NextWorkerId: 0,
	}
}

/******************************************************************************
							  Worker Manager operations
******************************************************************************/

// Get the number of workers
func (w *WorkerManager) GetNumWorkers() int {
	w.Lock()
	defer w.Unlock()

	return len(w.Workers)
}

// Register a new Worker to the Worker list
func (w *WorkerManager) AddWorker(worker *ManagedWorker) {
	w.Lock()
	defer w.Unlock()

	// Assign a unique worker ID
	worker.WorkerId = w.NextWorkerId
	w.NextWorkerId += 1

	w.Workers[worker.WorkerId] = worker

	// Add the worker to WorkersByIP view
	workerIP := strings.Split(worker.DataPlaneAddr, ":")[0]
	w.WorkersByIP[workerIP] = append(w.WorkersByIP[workerIP], worker)
}

// Allocate required number of workers randomly
func (w *WorkerManager) AllocateRandomWorkers(
	numRequiredWorkers int,
) []*ManagedWorker {

	selectedWorkers := make([]*ManagedWorker, 0, numRequiredWorkers)

	// Find available workers: first n workers based on map traversal order
	availableWorkers := make([]*ManagedWorker, 0)
	w.Lock()
	for _, worker := range w.Workers {
		if worker.IsAvailable {
			availableWorkers = append(availableWorkers, worker)
		}
	}

	// [TEMP] now manually sort based on port number to deterministically select
	// workers that are registered earlier
	sort.Slice(availableWorkers, func(i, j int) bool {
		partsI := strings.Split(availableWorkers[i].DataPlaneAddr, ":")
		if len(partsI) != 2 {
			log.Fatalf("Invalid DataPlaneAddr format: %s", availableWorkers[i].DataPlaneAddr)
		}
		partsJ := strings.Split(availableWorkers[j].DataPlaneAddr, ":")
		if len(partsJ) != 2 {
			log.Fatalf("Invalid DataPlaneAddr format: %s", availableWorkers[j].DataPlaneAddr)
		}
		pi, _ := strconv.Atoi(partsI[1])
		pj, _ := strconv.Atoi(partsJ[1])
		return pi < pj
	})
	if len(availableWorkers) > numRequiredWorkers {
		availableWorkers = availableWorkers[:numRequiredWorkers]
	}
	for _, worker := range availableWorkers {
		worker.IsAvailable = false
	}
	w.Unlock()
	selectedWorkers = availableWorkers

	// Check if enough workers are available
	if len(selectedWorkers) != numRequiredWorkers {
		log.Fatalf(
			"Need %d workers, but only %d available\n",
			numRequiredWorkers,
			len(selectedWorkers),
		)
	}

	return selectedWorkers
}

// Allocate specified workers. targetAddrs: map[ip addr] -> count
func (w *WorkerManager) AllocateSpecifiedWorkers(
	operator dataflow.Operator,
	targetAddrs map[string]int,
) []*ManagedWorker {

	numRequiredWorkers := operator.GetParallelism()
	selectedWorkers := make([]*ManagedWorker, 0, numRequiredWorkers)

	// Check operator parallelism = sum of targetAddrs counts
	numTasksInTargetAddrs := 0
	for _, count := range targetAddrs {
		numTasksInTargetAddrs += count
	}
	if numTasksInTargetAddrs != numRequiredWorkers {
		log.Fatalf(
			"[Custom Placement ERROR] Operator %s has %d tasks, but %d specified in placement plan .txt\n",
			operator.GetName(),
			numRequiredWorkers,
			numTasksInTargetAddrs,
		)
	}

	// Enforce deterministic order for traversing target addresses. This ensures
	// we add workers to selectedWorkers list in a deterministic order
	targetAddrsList := make([]string, 0, len(targetAddrs))
	for ip := range targetAddrs {
		targetAddrsList = append(targetAddrsList, ip)
	}
	sort.Strings(targetAddrsList)

	// Traverse the given target address list and allocate workers
	w.Lock()
	for _, ip := range targetAddrsList {

		totalCount := targetAddrs[ip]
		workerListByIP, ok := w.WorkersByIP[ip]
		if !ok {
			log.Fatalf(
				"[Custom Placement ERROR] Operator %s needs %d worker(s) from IP %s, but no worker registered on this IP\n",
				operator.GetName(),
				totalCount,
				ip,
			)
		}

		remainingCount := totalCount
		availableCountForDebug := 0
		for _, worker := range workerListByIP {
			if worker.IsAvailable {
				selectedWorkers = append(selectedWorkers, worker)
				worker.IsAvailable = false
				remainingCount -= 1
				availableCountForDebug += 1

				if remainingCount == 0 {
					break
				}
			}
		}

		// Check if enough workers are available from this IP
		if remainingCount > 0 {
			log.Fatalf(
				"[Custom Placement ERROR] Operator %s needs %d worker(s) from IP %s, but only %d available workers left\n",
				operator.GetName(),
				totalCount,
				ip,
				availableCountForDebug,
			)
		}
	}
	w.Unlock()
	return selectedWorkers
}

// Get Worker by ID
func (w *WorkerManager) GetWorker(workerId uint16) *ManagedWorker {
	w.Lock()
	defer w.Unlock()

	worker, ok := w.Workers[workerId]
	if !ok {
		log.Fatalf("Worker %d not found\n", workerId)
	}

	return worker
}

// Get Worker DataPlaneAddr
func (w *WorkerManager) GetWorkerDataPlaneAddr(workerId uint16) string {
	w.Lock()
	defer w.Unlock()

	worker, ok := w.Workers[workerId]
	if !ok {
		log.Fatalf("Worker %d not found\n", workerId)
	}

	return worker.DataPlaneAddr
}

// Get Worker StateCommAddr
func (w *WorkerManager) GetWorkerStateCommAddr(workerId uint16) string {
	w.Lock()
	defer w.Unlock()

	worker, ok := w.Workers[workerId]
	if !ok {
		log.Fatalf("Worker %d not found\n", workerId)
	}

	return worker.StateCommAddr
}
