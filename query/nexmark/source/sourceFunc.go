package source

import (
	"log"
	"math/rand"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/internal/utils"
	"github.com/CASP-Systems-BU/koala/query/nexmark/config"
	"github.com/CASP-Systems-BU/koala/query/nexmark/randGenerator"
	"github.com/CASP-Systems-BU/koala/query/rateLimiter"
)

// Constant drift for random generator seed
const RandDrift = int64(50)

/*
Create source function for generating Nexmark events. This util method is used
by both NexmarkKafkaProducer and NexmarkSource operator:

1. NexmarkKafkaProducer: generate Nexmark events to Kafka topics. Each producer
instance corresponds to a unique replicaID among a total number of producers.
In this case, `replicaID` and `parallelism` are known at construction time.

2. NexmarkSource: generate Nexmark events within Source operator. In this case,
`replicaID` and `parallelism` are not known at construction time - they are
assigned at task placement time, so we need to pass in the `source` pointer at
construction time, and retrieve `source.ReplicaID` and `source.Parallelism`
during runtime.
*/
func CreateSourceFunc[OUT tuple.Tuple](
	eventType config.EventType,
	generator *randGenerator.RandGenerator,
	useRatelimit bool,
	RateLimiter *rateLimiter.RateLimiter,
	numEventsToProduce int64,
	generatorInterval utils.Duration,
	// Used in NexmarkKafkaProducer
	replicaID int64,
	parallelism int,
	// Used in NexmarkSource
	source *NexmarkSource[OUT],
) func(collector.Collector) {

	return func(co collector.Collector) {

		// If this is called by NexmarkSource (source != nil), replicaID and
		// parallelism are available/assigned at runtime. We retrieve them from
		// the source operator pointer
		if source != nil {
			replicaID = source.ReplicaID
			parallelism = source.Parallelism
		}

		// Get next event function based on event type
		var generateNextEvent func(int64, int, *rand.Rand) (tuple.Tuple, error)
		switch eventType {
		case config.Auction:
			generateNextEvent = generator.NextAuction
		case config.Bid:
			generateNextEvent = generator.NextBid
		case config.ClosedAuction:
			generateNextEvent = generator.NextClosedAuction
		case config.Person:
			generateNextEvent = generator.NextPerson
		}

		// Random generator with seed based on replicaID
		random := rand.New(rand.NewSource(replicaID + RandDrift))

		// Main loop for event generation
		outputEventNumber := int64(0)
		if useRatelimit {

			// Limit the output rate if specified
			for {
				startTime := time.Now()

				// Get the rate limit for this time interval
				targetEventNum := RateLimiter.GetRateLimit()

				// Keep generating events until we reach the rate limit
				currentEvent := 0
				for currentEvent < targetEventNum {

					// Generate and emit the next event
					event, err := generateNextEvent(
						replicaID,
						parallelism,
						random,
					)
					if err != nil {
						log.Fatalf(
							"Failed to generate next BidEvent: %v",
							err,
						)
					}
					co.Emit(event)
					outputEventNumber++
					currentEvent++

					// Terminate if we have generated all required events
					if numEventsToProduce != 0 &&
						outputEventNumber >= numEventsToProduce {
						log.Println("SourceFunc exit: all records generated")
						return
					}
				}
				elapsed := time.Since(startTime)

				// Sleep if the event generation is faster than rate limit
				if elapsed < time.Duration(generatorInterval) {
					time.Sleep(time.Duration(generatorInterval) - elapsed)

				}

				// Increment the interval counter for rate limiter
				RateLimiter.Interval++
			}
		} else {

			// Generate events as fast as possible without rate limit
			for {

				// Generate and emit the next event
				event, err := generateNextEvent(replicaID, parallelism, random)
				if err != nil {
					log.Fatalf("Failed to generate next Event: %v", err)
				}
				co.Emit(event)
				outputEventNumber++

				if numEventsToProduce != 0 && outputEventNumber >= numEventsToProduce {
					log.Println("SourceFunc exit: all records generated")
					return
				}
			}
		}
	}
}
