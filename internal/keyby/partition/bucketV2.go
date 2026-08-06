package partition

type BucketV2 struct {

	// Owner worker ID of the bucket
	WorkerID uint16

	// Per-key based key lookup table (key -> worker ID)
	Map map[string]uint16
}
