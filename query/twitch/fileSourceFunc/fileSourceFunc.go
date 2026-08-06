package twitch

import (
	"bufio"
	"encoding/csv"
	"log"
	"os"
	"runtime"
	"strconv"
	"time"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query"
	ratelimiter "github.com/CASP-Systems-BU/disaggregated-streaming/query/rateLimiter"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/twitch/models"
)

func TwitchFileSourceFunc[OUT tuple.Tuple](
	RateLimiter *ratelimiter.RateLimiter,
	numEvents int64,
	alreadyOutputEventNumber int64,
	useRatelimit bool,
	generatorInterval time.Duration,
	filePath string,
) func(collector.Collector) {
	return func(co collector.Collector) {
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

		// Read the header line
		_, err = csvReader.Read()
		if err != nil {
			log.Fatalln("Failed to read taix csv", err)
		}

		// Read all twitch events into memory
		records, err := csvReader.ReadAll()
		var twitchEvents []models.TwitchEvent
		for _, record := range records {
			userId := record[0]
			streamerId := record[1]
			streamerName := record[2]
			eventTime, err := strconv.ParseInt(record[3], 10, 64)
			if err != nil {
				continue
			}
			eventType := record[4]
			twitchEvent := models.TwitchEvent{
				V1: userId,
				V2: streamerId,
				V3: streamerName,
				V4: eventTime,
				V5: eventType,
			}
			twitchEvents = append(twitchEvents, twitchEvent)
		}
		if err != nil {
			panic(err)
		}
		runtime.GC()
		query.PrintMemUsage("After Loading File")

		totalEventNumber := len(twitchEvents)
		log.Printf("Successfully loaded %d events in %v\n", totalEventNumber, time.Since(startTimestamp))
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
					outputEvent := twitchEvents[eventIndex]
					// Take the first event's timestamp as basetime:0
					outputEvent.V4 += timeBase - twitchEvents[0].V4
					co.Emit(outputEvent.Copy())
					outputEventNumber++
					currentEvent++
					// Generate events in a loop
					eventIndex++
					if eventIndex == totalEventNumber {
						eventIndex = 0
						// Make sure that the timestamp is always increasing
						timeBase += twitchEvents[totalEventNumber-1].V4 - twitchEvents[0].V4 + 1
					}

					// Check if we already generated all events we need
					if outputEventNumber == int(numEvents) &&
						int(numEvents) != 0 {
						log.Println("source has generated all records")
						return
					}
					// Check if we already genreated a whole round
					if outputEventNumber%totalEventNumber == 0 {
						log.Println(
							"source has generated a singleRound, total",
							totalEventNumber,
							"events.",
						)
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
				outputEvent := twitchEvents[eventIndex]
				// Take the first event's timestamp as basetime:0
				outputEvent.V4 += timeBase - twitchEvents[0].V4
				co.Emit(outputEvent.Copy())
				outputEventNumber++
				// Generate events in a loop
				eventIndex++
				if eventIndex == totalEventNumber {
					eventIndex = 0
					timeBase += twitchEvents[totalEventNumber-1].V4 - twitchEvents[0].V4 + 1
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
