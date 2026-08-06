package utils

import "math"

/******************************************************************************
	              Helper function to calculate cellID
******************************************************************************/

func GetCellID(lon, lat float64) (int64, bool) {
	const (
		lat0     = 41.474937 // Center of cell 1.1
		lon0     = -74.913585
		deltaLat = 0.004489 // ≈ 500m
		deltaLon = 0.005986 // ≈ 500m at 41.5°N
		maxIndex = 300
	)

	// Compute indices
	east := int64(math.Floor((lon-lon0)/deltaLon)) + 1
	south := int64(math.Floor((lat0-lat)/deltaLat)) + 1

	// Check bounds,
	if east < 1 || east > maxIndex || south < 1 || south > maxIndex {
		return 0, false // Outlier
	}

	// Combine to single cell_id
	cellID := south*(maxIndex+1) + east
	return cellID, true
}
