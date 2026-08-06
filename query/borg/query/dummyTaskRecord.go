package query

import (
	_ "embed"
	"strconv"
	"strings"
)

//go:embed dummyTaskRecords.txt
var dummyTaskTxt []byte // Automatically loads the txt content at compile time

// dummyTaskRecords is a slice of pointers, matching your original code
var dummyTaskRecords []*TaskRecord

// init() runs automatically when the package is imported
func init() {
	// 1. Convert the entire embedded file to a single string
	content := string(dummyTaskTxt)

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

		record := &TaskRecord{}

		for _, part := range parts {
			// Split key and value by the first colon
			kv := strings.SplitN(part, ":", 2)
			if len(kv) < 2 {
				continue
			}

			// Trim spaces AND newlines from the key and value
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])

			// 4. Parse and assign based on the key
			switch key {

			// --- String field ---
			case "V5":
				record.V5 = strings.Trim(val, "\"")

			// --- Int64 fields ---
			case "V1":
				record.V1, _ = strconv.ParseInt(val, 10, 64)
			case "V2":
				record.V2, _ = strconv.ParseInt(val, 10, 64)
			case "V3":
				record.V3, _ = strconv.ParseInt(val, 10, 64)
			case "V4":
				record.V4, _ = strconv.ParseInt(val, 10, 64)
			case "V12":
				record.V12, _ = strconv.ParseInt(val, 10, 64)

			// --- Float64 fields (scientific notation parses fine with ParseFloat) ---
			case "V6":
				record.V6, _ = strconv.ParseFloat(val, 64)
			case "V7":
				record.V7, _ = strconv.ParseFloat(val, 64)
			case "V8":
				record.V8, _ = strconv.ParseFloat(val, 64)
			case "V9":
				record.V9, _ = strconv.ParseFloat(val, 64)
			case "V10":
				record.V10, _ = strconv.ParseFloat(val, 64)
			case "V11":
				record.V11, _ = strconv.ParseFloat(val, 64)
			case "V13":
				record.V13, _ = strconv.ParseFloat(val, 64)
			case "V14":
				record.V14, _ = strconv.ParseFloat(val, 64)
			case "V15":
				record.V15, _ = strconv.ParseFloat(val, 64)
			case "V16":
				record.V16, _ = strconv.ParseFloat(val, 64)
			case "V17":
				record.V17, _ = strconv.ParseFloat(val, 64)
			case "V18":
				record.V18, _ = strconv.ParseFloat(val, 64)
			}
		}

		// Append the pointer to the slice
		dummyTaskRecords = append(dummyTaskRecords, record)
	}
}
