package query

import (
	_ "embed"
	"strconv"
	"strings"
)

//go:embed dummyCpuRecords.txt
var dummyRecordTxt []byte

var dummyCpuRecords []*CpuRecord

func init() {
	// 1. Convert the entire embedded file to a single string
	content := string(dummyRecordTxt)

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

		record := &CpuRecord{}
		for _, part := range parts {
			// Split key and value by the colon
			kv := strings.SplitN(part, ":", 2)
			if len(kv) < 2 {
				continue
			}

			// Trim spaces AND newlines from the key and value
			key := strings.TrimSpace(kv[0])
			val := strings.TrimSpace(kv[1])

			// 4. Assign value according to key
			switch key {
			case "V1":
				record.V1, _ = strconv.ParseInt(val, 10, 64)
			case "V2":
				// remove ""
				record.V2 = strings.Trim(val, "\"")
			case "V3":
				record.V3, _ = strconv.ParseFloat(val, 64)
			case "V4":
				record.V4, _ = strconv.ParseFloat(val, 64)
			case "V5":
				record.V5, _ = strconv.ParseFloat(val, 64)
			case "V6":
				record.V6, _ = strconv.ParseFloat(val, 64)
			case "V7":
				record.V7, _ = strconv.ParseFloat(val, 64)
			case "V8":
				record.V8, _ = strconv.ParseFloat(val, 64)
			case "V9":
				record.V9, _ = strconv.ParseInt(val, 10, 64)
			}
		}
		dummyCpuRecords = append(dummyCpuRecords, record)
	}
}
