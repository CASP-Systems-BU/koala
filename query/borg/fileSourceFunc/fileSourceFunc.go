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
	"github.com/CASP-Systems-BU/koala/query/borg/models"
	ratelimiter "github.com/CASP-Systems-BU/koala/query/rateLimiter"
)

func BorgFileSourceFunc[OUT tuple.Tuple](
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
		// StartTimestamp of this time interval
		startTimestamp := time.Now()
		// Open the file, and create a csv reader using buffered reader
		// for better performance.
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalln("Failed to opern file", err)
		}
		reader := bufio.NewReader(file)
		csvReader := csv.NewReader(reader)

		// reuse the record to avoid extra memory allocation
		csvReader.ReuseRecord = true

		// Read all task events into memory
		totalEvents := make([]models.TaskEvent, 0, 40000000)
		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			timestamp, err := parseIntOrDefault(record[0], -1)
			if err != nil {
				log.Fatalln("Failed to parse timestamp:", err)
			}
			missingInfo := record[1]
			jobID, err := parseIntOrDefault(record[2], -1)
			if err != nil {
				log.Fatalln("Failed to parse jobID: ", err)
			}
			taskIndex, err := parseIntOrDefault(record[3], -1)
			if err != nil {
				log.Fatalln("Failed to parse taskIndex: ", err)
			}
			machineID, err := parseIntOrDefault(record[4], -1)
			if err != nil {
				log.Fatalln("Failed to parse machineID: ", err)
			}
			evntType := record[5]
			userName := record[6]
			schedulingClass, err := parseIntOrDefault(record[7], -1)
			if err != nil {
				log.Fatalln("Failed to parse schedulingClass: ", err)
			}
			priority, err := parseIntOrDefault(record[8], -1)
			if err != nil {
				log.Fatalln("Failed to parse priority: ", err)
			}
			cpuRequest, err := parseFloatOrDefault(record[9], 0.0)
			if err != nil {
				log.Fatalln("Failed to parse cpuRequest: ", err)
			}
			ramcpuRequest, err := parseFloatOrDefault(record[10], 0.0)
			if err != nil {
				log.Fatalln("Failed to parse ramcpuRequest: ", err)
			}
			localDiskRequest, err := parseFloatOrDefault(record[11], 0.0)
			if err != nil {
				log.Fatalln("Failed to parse localDiskRequest: ", err)
			}
			constraint := false
			if record[12] != "" {
				constraint, err = strconv.ParseBool(record[12])
				if err != nil {
					log.Fatalln("Failed to parse constraint: ", err)
				}
			}

			taskEvent := models.TaskEvent{
				V1:  timestamp,
				V2:  missingInfo,
				V3:  jobID,
				V4:  taskIndex,
				V5:  machineID,
				V6:  evntType,
				V7:  userName,
				V8:  schedulingClass,
				V9:  priority,
				V10: cpuRequest,
				V11: ramcpuRequest,
				V12: localDiskRequest,
				V13: constraint,
			}
			totalEvents = append(totalEvents, taskEvent)
		}
		runtime.GC()
		query.PrintMemUsage("After Loading File & GC")
		totalEventNumber := len(totalEvents)
		outputEventNumber := 0
		eventIndex := int(alreadyOutputEventNumber) % totalEventNumber
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

/*
*****************************************************************************

	Helper function to deal with empty field in csv

*****************************************************************************
*/
func parseIntOrDefault(s string, def int64) (int64, error) {
	if s == "" {
		return def, nil
	}
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return def, err
	}
	return v, err
}

func parseFloatOrDefault(s string, def float64) (float64, error) {
	if s == "" {
		return def, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def, err
	}
	return v, err
}
