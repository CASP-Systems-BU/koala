package query

import (
	"fmt"
	"math/rand"
	"runtime"
	"time"
)

const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

func RandString(n int) string {
	rand.Seed(time.Now().UnixNano())
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

func PrintMemUsage(tag string) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	fmt.Printf("\n--- Memory Usage (%s) ---\n", tag)
	fmt.Printf("Alloc      = %v MiB (Heap objects currently in use)\n", bToMb(m.Alloc))
	fmt.Printf("TotalAlloc = %v MiB (Cumulative total allocated)\n", bToMb(m.TotalAlloc))
	fmt.Printf("Sys        = %v MiB (Total memory obtained from OS)\n", bToMb(m.Sys))
	fmt.Printf("NumGC      = %v (Number of GC cycles)\n", m.NumGC)
	fmt.Println("-------------------------------------------")
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}
