package network

import (
	"log"
	"reflect"
	"unsafe"

	"github.com/CASP-Systems-BU/disaggregated-streaming/api/tuple"
	"github.com/mus-format/mus-go/ord"
	"github.com/mus-format/mus-go/varint"
)

///////////////////// Codec func signatures /////////////////////

// Encoder function that encodes a tuple into byte[]. It traverses all fields of
// the tuple and encode them individually. Returns the # of bytes used
type TupleEncoder func(tuple.Tuple, []byte) int

// Individual encoding function for each tuple field
// Use unsafe.Pointer to avoid extra memory allocation when assign string (or
// other struct) to a interface{}.
type EncodeFunc func(unsafe.Pointer, []byte) int

// Decoder function that decodes byte[] to a pre-allocated tuple
type TupleDecoder func(tuple.Tuple, []byte) (int, error)

// Individual decoding function for each tuple field. Return any instead of
// pointer to avoid additional mem alloc
// Note!! return any can still cause extra mem alloc for return string. The
// value field of interface{} will store pointer pointing to the string header
// which was originally allocated on stack. It will be copied to heap first and
// store the pointer.
// Other primitive types can also cause extra mem alloc (e.g. int with a very
// large number) -  in this case go Small Object Optimization (SOO) can decide
// not to directly store value into the interface's value field but allocate
// heap mem and store pointer to it.
type DecodeFunc func([]byte) (any, int, error)

// Calculate the # of bytes of encoded tuple
type TupleSizer func(tuple.Tuple) int

// Individual size function for each tuple field
type SizeFunc func(unsafe.Pointer) int

///////////////////// Codec constructors /////////////////////

// Based on the tuple type, create a set of encoding function for each field
func CreateEncoder(tupleType reflect.Type) TupleEncoder {

	numField := tupleType.NumField() - 1 // Exclude timestamp field

	// Add individual encoding function for each tuple field
	encodeFuncs := []EncodeFunc{}
	for i := 0; i < numField; i++ {
		kind := tupleType.Field(i).Type.Kind()
		if !tuple.SupportedKind(kind) {
			log.Fatalln("Unsupported field type=" + kind.String())
		}
		encodeFuncs = append(encodeFuncs, EncodeFuncFromKind(kind))
	}

	return func(t tuple.Tuple, buf []byte) int {
		offset := 0
		// Encode each field of the tuple
		for i, f := range encodeFuncs {
			offset += f(t.GetField(i), buf[offset:])
		}
		// Encode timestamp
		offset += varint.MarshalInt64(t.GetTimestamp(), buf[offset:])
		return offset
	}
}

// Based on the tuple type, create a set of decoding function for each field
func CreateDecoder(tupleType reflect.Type) TupleDecoder {

	numField := tupleType.NumField() - 1 // Exclude timestamp field

	// Add individual decoding function for each tuple field
	decodeFuncs := []DecodeFunc{}
	for i := 0; i < numField; i++ {
		kind := tupleType.Field(i).Type.Kind()
		if !tuple.SupportedKind(kind) {
			log.Fatalln("Unsupported field type=" + kind.String())
		}
		decodeFuncs = append(decodeFuncs, DecodeFuncFromKind(kind))
	}

	return func(t tuple.Tuple, buf []byte) (int, error) {
		offset := 0
		// Decode each field of the tuple
		for i, f := range decodeFuncs {
			v, n, err := f(buf[offset:])
			offset += n
			if err != nil {
				return offset, err
			}
			t.SetField(i, v)
		}
		// Decode timestamp
		timestamp, n, err := varint.UnmarshalInt64(buf[offset:])
		offset += n
		if err != nil {
			return offset, err
		}
		t.SetTimestamp(timestamp)
		return offset, nil
	}
}

// Based on the tuple type, create a set of size function for each field
func CreateSizer(tupleType reflect.Type) TupleSizer {

	numField := tupleType.NumField() - 1 // Exclude timestamp field

	// Add individual size function for each tuple field
	sizeFuncs := []SizeFunc{}
	for i := 0; i < numField; i++ {
		kind := tupleType.Field(i).Type.Kind()
		if !tuple.SupportedKind(kind) {
			log.Fatalln("Unsupported field type=" + kind.String())
		}
		sizeFuncs = append(sizeFuncs, TupleSizeFuncFromKind(kind))
	}

	return func(t tuple.Tuple) int {
		size := 0
		// Calculate size of each field of the tuple
		for i, f := range sizeFuncs {
			size += f(t.GetField(i))
		}
		// Calculate size of timestamp
		size += varint.SizeInt64(t.GetTimestamp())
		return size
	}
}

///////////////////// Encode functions /////////////////////

func EncodeFuncFromKind(kind reflect.Kind) EncodeFunc {
	switch kind {
	case reflect.Bool:
		return EncodeBool
	case reflect.Int:
		return EncodeInt
	case reflect.Int8:
		return EncodeInt8
	case reflect.Int16:
		return EncodeInt16
	case reflect.Int32:
		return EncodeInt32
	case reflect.Int64:
		return EncodeInt64
	case reflect.Uint:
		return EncodeUint
	case reflect.Uint8:
		return EncodeUint8
	case reflect.Uint16:
		return EncodeUint16
	case reflect.Uint32:
		return EncodeUint32
	case reflect.Uint64:
		return EncodeUint64
	case reflect.Float32:
		return EncodeFloat32
	case reflect.Float64:
		return EncodeFloat64
	case reflect.String:
		return EncodeString
	default:
		panic("unknown kind=" + kind.String())
	}
}

func EncodeBool(b unsafe.Pointer, buf []byte) int {
	var n uint8
	if *(*bool)(b) {
		n = 1
	} else {
		n = 0
	}
	return varint.MarshalUint8(n, buf)
}

func EncodeInt(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalInt(*(*int)(i), buf)
}

func EncodeInt8(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalInt8(*(*int8)(i), buf)
}

func EncodeInt16(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalInt16(*(*int16)(i), buf)
}

func EncodeInt32(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalInt32(*(*int32)(i), buf)
}

func EncodeInt64(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalInt64(*(*int64)(i), buf)
}

func EncodeUint(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalUint(*(*uint)(i), buf)
}

func EncodeUint8(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalUint8(*(*uint8)(i), buf)
}

func EncodeUint16(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalUint16(*(*uint16)(i), buf)
}

func EncodeUint32(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalUint32(*(*uint32)(i), buf)
}

func EncodeUint64(i unsafe.Pointer, buf []byte) int {
	return varint.MarshalUint64(*(*uint64)(i), buf)
}

func EncodeFloat32(f unsafe.Pointer, buf []byte) int {
	return varint.MarshalFloat32(*(*float32)(f), buf)
}

func EncodeFloat64(f unsafe.Pointer, buf []byte) int {
	return varint.MarshalFloat64(*(*float64)(f), buf)
}

func EncodeString(s unsafe.Pointer, buf []byte) int {
	return ord.MarshalString(*(*string)(s), nil, buf)
}

///////////////////// Decode functions /////////////////////

func DecodeFuncFromKind(kind reflect.Kind) DecodeFunc {
	switch kind {
	case reflect.Bool:
		return DecodeBool
	case reflect.Int:
		return DecodeInt
	case reflect.Int8:
		return DecodeInt8
	case reflect.Int16:
		return DecodeInt16
	case reflect.Int32:
		return DecodeInt32
	case reflect.Int64:
		return DecodeInt64
	case reflect.Uint:
		return DecodeUint
	case reflect.Uint8:
		return DecodeUint8
	case reflect.Uint16:
		return DecodeUint16
	case reflect.Uint32:
		return DecodeUint32
	case reflect.Uint64:
		return DecodeUint64
	case reflect.Float32:
		return DecodeFloat32
	case reflect.Float64:
		return DecodeFloat64
	case reflect.String:
		return DecodeString
	default:
		panic("unknown kind=" + kind.String())
	}
}

func DecodeBool(buf []byte) (any, int, error) {
	v, n, err := varint.UnmarshalUint8(buf)
	if v == uint8(1) {
		return true, n, err
	} else {
		return false, n, err
	}
}

func DecodeInt(buf []byte) (any, int, error) {
	return varint.UnmarshalInt(buf)
}

func DecodeInt8(buf []byte) (any, int, error) {
	return varint.UnmarshalInt8(buf)
}

func DecodeInt16(buf []byte) (any, int, error) {
	return varint.UnmarshalInt16(buf)
}

func DecodeInt32(buf []byte) (any, int, error) {
	return varint.UnmarshalInt32(buf)
}

func DecodeInt64(buf []byte) (any, int, error) {
	return varint.UnmarshalInt64(buf)
}

func DecodeUint(buf []byte) (any, int, error) {
	return varint.UnmarshalUint(buf)
}

func DecodeUint8(buf []byte) (any, int, error) {
	return varint.UnmarshalUint8(buf)
}

func DecodeUint16(buf []byte) (any, int, error) {
	return varint.UnmarshalUint16(buf)
}

func DecodeUint32(buf []byte) (any, int, error) {
	return varint.UnmarshalUint32(buf)
}

func DecodeUint64(buf []byte) (any, int, error) {
	return varint.UnmarshalUint64(buf)
}

func DecodeFloat32(buf []byte) (any, int, error) {
	return varint.UnmarshalFloat32(buf)
}

func DecodeFloat64(buf []byte) (any, int, error) {
	return varint.UnmarshalFloat64(buf)
}

func DecodeString(buf []byte) (any, int, error) {
	return ord.UnmarshalString(nil, buf)
}

///////////////////// Size functions /////////////////////

func TupleSizeFuncFromKind(kind reflect.Kind) SizeFunc {
	switch kind {
	case reflect.Bool:
		return SizeBool
	case reflect.Int:
		return SizeInt
	case reflect.Int8:
		return SizeInt8
	case reflect.Int16:
		return SizeInt16
	case reflect.Int32:
		return SizeInt32
	case reflect.Int64:
		return SizeInt64
	case reflect.Uint:
		return SizeUint
	case reflect.Uint8:
		return SizeUint8
	case reflect.Uint16:
		return SizeUint16
	case reflect.Uint32:
		return SizeUint32
	case reflect.Uint64:
		return SizeUint64
	case reflect.Float32:
		return SizeFloat32
	case reflect.Float64:
		return SizeFloat64
	case reflect.String:
		return SizeString
	default:
		panic("unknown kind=" + kind.String())
	}
}

func SizeBool(b unsafe.Pointer) int {
	return varint.SizeUint8(*(*uint8)(b))
}

func SizeInt(i unsafe.Pointer) int {
	return varint.SizeInt(*(*int)(i))
}

func SizeInt8(i unsafe.Pointer) int {
	return varint.SizeInt8(*(*int8)(i))
}

func SizeInt16(i unsafe.Pointer) int {
	return varint.SizeInt16(*(*int16)(i))
}

func SizeInt32(i unsafe.Pointer) int {
	return varint.SizeInt32(*(*int32)(i))
}

func SizeInt64(i unsafe.Pointer) int {
	return varint.SizeInt64(*(*int64)(i))
}

func SizeUint(i unsafe.Pointer) int {
	return varint.SizeUint(*(*uint)(i))
}

func SizeUint8(i unsafe.Pointer) int {
	return varint.SizeUint8(*(*uint8)(i))
}

func SizeUint16(i unsafe.Pointer) int {
	return varint.SizeUint16(*(*uint16)(i))
}

func SizeUint32(i unsafe.Pointer) int {
	return varint.SizeUint32(*(*uint32)(i))
}

func SizeUint64(i unsafe.Pointer) int {
	return varint.SizeUint64(*(*uint64)(i))
}

func SizeFloat32(i unsafe.Pointer) int {
	return varint.SizeFloat32(*(*float32)(i))
}

func SizeFloat64(i unsafe.Pointer) int {
	return varint.SizeFloat64(*(*float64)(i))
}

func SizeString(i unsafe.Pointer) int {
	return ord.SizeString(*(*string)(i), nil)
}
