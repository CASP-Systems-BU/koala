package tuple

import (
	"errors"
	"unsafe"
)

// Error
var (
	ErrExtraField     = errors.New("field number more than avaliable in tuple")
	ErrWrongFieldType = errors.New("wrong field type")
)

// Interface for all tuples
type Tuple interface {
	GetField(int) unsafe.Pointer
	SetField(int, any) error
	New() Tuple
	Copy() Tuple
	GetTimestamp() int64
	SetTimestamp(int64)
	Equal(Tuple) bool
}
