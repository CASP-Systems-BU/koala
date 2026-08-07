package query

import (
	_ "embed"
	"strconv"
	"strings"

	"github.com/CASP-Systems-BU/koala/query/taxi/models"
)

//go:embed dummyRouteInfo.txt
var dummyTaxiTxt []byte // Automatically loads the txt content at compile time

var dummyTaxiTrip []models.RouteInfo // The target slice to populate

// init() runs automatically when the package is imported
func init() {
	// 1. Convert the entire embedded file to a single string
	content := string(dummyTaxiTxt)

	// 2. Split the content by "}," to handle multi-line records seamlessly
	blocks := strings.Split(content, "},")

	for _, block := range blocks {
		// Clean up whitespace, newlines, and any remaining '{' or '}'
		block = strings.TrimSpace(block)
		block = strings.Trim(block, "{}")
		block = strings.TrimSpace(block)

		// Skip empty blocks (usually happens at the very end of the file)
		if block == "" {
			continue
		}

		// 3. Split the block into key-value pairs by comma
		parts := strings.Split(block, ",")

		var record models.RouteInfo

		for _, part := range parts {
			// Split key and value by the first colon
			kv := strings.SplitN(part, ":", 2)
			if len(kv) < 2 {
				continue
			}

			// Trim spaces AND newlines from the key and value
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])

			// 4. Assign parsed values based on the key type
			switch key {
			// String fields (need to strip quotes)
			case "V1":
				record.V1 = strings.Trim(val, "\"")
			case "V2":
				record.V2 = strings.Trim(val, "\"")
			case "V3":
				record.V3 = strings.Trim(val, "\"")
			case "V5":
				record.V5 = strings.Trim(val, "\"")

			// Integer fields
			case "V4":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V4 = int64(parsed)
			case "V6":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V6 = parsed
			case "V7":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V7 = parsed
			case "V8":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V8 = int64(parsed)
			case "V9":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V9 = int64(parsed)
			case "V15":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V15 = int64(parsed)
			case "V16":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V16 = int64(parsed)
			case "V17":
				parsed, _ := strconv.ParseInt(val, 10, 64)
				record.V17 = parsed

			// Float fields
			case "V10":
				record.V10, _ = strconv.ParseFloat(val, 64)
			case "V11":
				record.V11, _ = strconv.ParseFloat(val, 64)
			case "V12":
				record.V12, _ = strconv.ParseFloat(val, 64)
			case "V13":
				record.V13, _ = strconv.ParseFloat(val, 64)
			case "V14":
				record.V14, _ = strconv.ParseFloat(val, 64)
			}
		}

		// Append the fully parsed record to the slice
		dummyTaxiTrip = append(dummyTaxiTrip, record)
	}

}
