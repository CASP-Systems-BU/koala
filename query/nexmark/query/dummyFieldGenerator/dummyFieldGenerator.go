package dummyfieldgenerator

import (
	"math/rand"
	"time"
	"unsafe"
)

const (
	letterBytes   = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	letterIdxBits = 6
	letterIdxMask = 1<<letterIdxBits - 1
	letterIdxMax  = 63 / letterIdxBits
)

type Dummyfieldgenerator struct {
	randGenerator     *rand.Rand
	dummyFieldContent []byte
}

func NewDummyFieldGenerator(n int) *Dummyfieldgenerator {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	dummyFieldContent := make([]byte, n)
	return &Dummyfieldgenerator{
		randGenerator:     rand,
		dummyFieldContent: dummyFieldContent,
	}
}

func (df *Dummyfieldgenerator) GenerateDummyField() string {
	n := len(df.dummyFieldContent)
	for i, cache, remain := n-1, df.randGenerator.Uint64(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = df.randGenerator.Uint64(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			df.dummyFieldContent[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return unsafe.String(&df.dummyFieldContent[0], len(df.dummyFieldContent))
}
