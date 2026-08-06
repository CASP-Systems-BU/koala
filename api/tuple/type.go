package tuple

import "reflect"

// Supported types (specific kind)
func SupportedKind(kind reflect.Kind) bool {
	return (kind == reflect.Bool) ||
		(kind == reflect.Int) ||
		(kind == reflect.Int8) ||
		(kind == reflect.Int16) ||
		(kind == reflect.Int32) ||
		(kind == reflect.Int64) ||
		(kind == reflect.Uint) ||
		(kind == reflect.Uint8) ||
		(kind == reflect.Uint16) ||
		(kind == reflect.Uint32) ||
		(kind == reflect.Uint64) ||
		(kind == reflect.Float32) ||
		(kind == reflect.Float64) ||
		(kind == reflect.String)
}
