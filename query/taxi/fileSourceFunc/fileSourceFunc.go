package taxi

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

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/collector"
	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query"
	ratelimiter "github.com/CASP-Systems-BU/disaggregated-streaming/query/rateLimiter"
	"github.com/CASP-Systems-BU/disaggregated-streaming/query/taxi/models"
)

func TaxiFileSourceFunc[OUT tuple.Tuple](
	RateLimiter *ratelimiter.RateLimiter,
	numEvents int64,
	alreadyOutputEventNumber int64,
	useRatelimit bool,
	generatorInterval time.Duration,
	filePath string,
) func(collector.Collector) {
	return func(co collector.Collector) {
		debug.SetMemoryLimit(20 * 1024 * 1024 * 1024)
		debug.SetGCPercent(50)

		startTimestamp := time.Now()
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalln("Failed to open file", err)
		}
		defer file.Close()

		reader := bufio.NewReaderSize(file, 1024*1024)
		csvReader := csv.NewReader(reader)
		csvReader.ReuseRecord = true

		// Read the header line
		_, err = csvReader.Read()
		if err != nil {
			log.Fatalln("Failed to read taxi csv header", err)
		}

		totalEvents := make([]models.TaxiTrip, 0, 35000000)

		timeLayout := "2006-01-02 15:04:05"
		baseTime, _ := time.Parse(timeLayout, "2013-01-01 00:00:00")

		for {
			record, err := csvReader.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				continue
			}
			rateCode, err := strconv.ParseInt(record[3], 10, 64)
			if err != nil {
				continue
			}

			pickupTime, err := time.Parse(timeLayout, record[5])
			if err != nil {
				continue
			}

			dropoffTime, err := time.Parse(timeLayout, record[6])
			if err != nil {
				continue
			}

			passengerCount, err := strconv.ParseInt(record[7], 10, 64)
			if err != nil {
				continue
			}

			tripTimeSecs, err := strconv.ParseInt(record[8], 10, 64)
			if err != nil {
				continue
			}

			tripDistance, err := strconv.ParseFloat(record[9], 64)
			if err != nil {
				continue
			}

			pLon, err := strconv.ParseFloat(record[10], 64)
			if err != nil {
				continue
			}
			pLat, err := strconv.ParseFloat(record[11], 64)
			if err != nil {
				continue
			}
			dLon, err := strconv.ParseFloat(record[12], 64)
			if err != nil {
				continue
			}
			dLat, err := strconv.ParseFloat(record[13], 64)
			if err != nil {
				continue
			}

			totalEvents = append(totalEvents, models.TaxiTrip{
				V1:  record[0],
				V2:  record[1],
				V3:  record[2],
				V4:  rateCode,
				V5:  record[4],
				V6:  pickupTime.Sub(baseTime).Nanoseconds(),
				V7:  dropoffTime.Sub(baseTime).Nanoseconds(),
				V8:  passengerCount,
				V9:  tripTimeSecs,
				V10: tripDistance,
				V11: pLon,
				V12: pLat,
				V13: dLon,
				V14: dLat,
			})
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
					outputEvent.V7 += timeBase - totalEvents[0].V7
					co.Emit(outputEvent.Copy())
					outputEventNumber++
					currentEvent++
					// Generate events in a loop
					eventIndex++
					if eventIndex == totalEventNumber {
						eventIndex = 0
						// Make sure that the timestamp is always increasing
						timeBase += totalEvents[totalEventNumber-1].V7 - totalEvents[0].V7 + 1
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
				outputEvent.V7 += timeBase - totalEvents[0].V7
				co.Emit(outputEvent.Copy())
				outputEventNumber++
				// Generate events in a loop
				eventIndex++
				if eventIndex == totalEventNumber {
					eventIndex = 0
					timeBase += totalEvents[totalEventNumber-1].V7 - totalEvents[0].V7 + 1
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
