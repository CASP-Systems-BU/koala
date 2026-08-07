package filesourcefunc

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/CASP-Systems-BU/koala/api/collector"
	"github.com/CASP-Systems-BU/koala/api/tuple"
	"github.com/CASP-Systems-BU/koala/query"
	"github.com/CASP-Systems-BU/koala/query/azure/models"
	ratelimiter "github.com/CASP-Systems-BU/koala/query/rateLimiter"
)

func AzureFileSourceFunc[OUT tuple.Tuple](
	RateLimiter *ratelimiter.RateLimiter,
	numEvents int64,
	alreadyOutputEventNumber int64,
	useRatelimit bool,
	generatorInterval time.Duration,
	filePath string,
	isWarmUp bool,
) func(collector.Collector) {
	return func(co collector.Collector) {
		debug.SetMemoryLimit(20 * 1024 * 1024 * 1024)
		debug.SetGCPercent(50)
		startTimestamp := time.Now()
		// Open the file, and create a csv reader using buffered reader
		// for better performance.
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalln("Failed to open file", err)
		}
		reader := bufio.NewReader(file)
		csvReader := csv.NewReader(reader)

		// reuse the record to avoid extra memory allocation
		csvReader.ReuseRecord = true

		// Read all task events into memory
		totalEvents := make([]models.AzureEvent, 0, 50000000)
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			timestamp, err := strconv.ParseInt(record[0], 10, 64)
			if err != nil {
				log.Fatalln("failed to parse timestamp: ", err)
			}
			vmID := record[1]
			minCpu, err := strconv.ParseFloat(record[2], 64)
			if err != nil {
				log.Fatalln("failed to parse minCpu: ", err)
			}
			maxCpu, err := strconv.ParseFloat(record[3], 64)
			if err != nil {
				log.Fatalln("failed to parse maxCpu: ", err)
			}
			avgCpu, err := strconv.ParseFloat(record[4], 64)
			if err != nil {
				log.Fatalln("failed to parse avgCpu: ", err)
			}
			azureEvent := models.AzureEvent{
				V1: timestamp,
				V2: vmID,
				V3: minCpu,
				V4: maxCpu,
				V5: avgCpu,
			}
			totalEvents = append(totalEvents, azureEvent)
		}

		runtime.GC()
		query.PrintMemUsage("After Loading File & GC")
		totalEventNumber := len(totalEvents)
		log.Printf("Successfully loaded %d events in %v\n", totalEventNumber, time.Since(startTimestamp))
		outputEventNumber := 0
		eventIndex := int(alreadyOutputEventNumber) % len(totalEvents)
		timeBase := int64(0)
		if useRatelimit {
			elapsed := time.Since(startTimestamp)
			RateLimiter.Interval += int(elapsed / generatorInterval)
			for {
				// StartTimestamp of this time interval
				startTimestamp := time.Now()
				// RateLimit in this intervals
				targetEventNum := RateLimiter.GetRateLimit()
				// Total number of event generated in this time interval
				currentEvent := 0
				// Keep generating events till we reach the rate limit
				for currentEvent < targetEventNum {
					outputEvent := totalEvents[eventIndex]
					// Take the first event's timestamp as basetime:0
					outputEvent.V1 += timeBase - totalEvents[0].V1
					co.Emit(outputEvent.Copy())
					outputEventNumber++
					currentEvent++
					// Generate events in a loop
					eventIndex++
					if eventIndex == totalEventNumber {
						eventIndex = 0
						// Make sure that the timestamp is always increasing
						timeBase += totalEvents[totalEventNumber-1].V1 - totalEvents[0].V1 + 1
					}

					// Check if we already generated all events we need
					if outputEventNumber == int(numEvents) &&
						int(numEvents) != 0 {
						log.Println("source has generated all records")
						return
					}

					// Check if we already genreated a whole round
					if outputEventNumber%totalEventNumber == 0 {
						log.Println("source has generated a singleRound, total", totalEventNumber, "events.")
					}
				}

				elapsed := time.Since(startTimestamp)

				if elapsed < generatorInterval {
					// Sleep during the rest time of this time interval
					time.Sleep(generatorInterval - elapsed)
				}
				RateLimiter.Interval++
			}
		} else {
			for {
				outputEvent := totalEvents[eventIndex]
				// Take the first event's timestamp as basetime:0
				outputEvent.V1 += timeBase - totalEvents[0].V1
				co.Emit(outputEvent.Copy())
				outputEventNumber++
				// Generate events in a loop
				eventIndex++
				if eventIndex == totalEventNumber {
					eventIndex = 0
					timeBase += totalEvents[totalEventNumber-1].V1 - totalEvents[0].V1 + 1
				}
				if outputEventNumber == int(numEvents) && int(numEvents) != 0 {
					log.Println("source has generated all records")
					return
				}
				// Check if we already genreated a whole round
				if outputEventNumber%totalEventNumber == 0 {
					log.Println("source has generated a singleRound, total", totalEventNumber, "events.")
				}
			}
		}
	}
}
