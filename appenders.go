package quaminapb

import (
	"math"
	"strconv"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// appendVarintFn formats a varint wire value into buf, returning the extended buf and
// whether the value should be treated as a JSON number (IsNumber).
type appendVarintFn func(buf []byte, v uint64) ([]byte, bool)

// appendFixed32Fn formats a fixed32 wire value into buf.
type appendFixed32Fn func(buf []byte, v uint32) ([]byte, bool)

// appendFixed64Fn formats a fixed64 wire value into buf.
type appendFixed64Fn func(buf []byte, v uint64) ([]byte, bool)

func appendBool(buf []byte, v uint64) ([]byte, bool)   { return strconv.AppendBool(buf, v != 0), false }
func appendInt32(buf []byte, v uint64) ([]byte, bool)  { return strconv.AppendInt(buf, int64(int32(v)), 10), true }
func appendInt64(buf []byte, v uint64) ([]byte, bool)  { return strconv.AppendInt(buf, int64(v), 10), true }
func appendUint64(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendUint(buf, v, 10), true }

func appendSint32(buf []byte, v uint64) ([]byte, bool) {
	return strconv.AppendInt(buf, int64(int32(protowire.DecodeZigZag(v&0xFFFFFFFF))), 10), true
}

func appendSint64(buf []byte, v uint64) ([]byte, bool) {
	return strconv.AppendInt(buf, protowire.DecodeZigZag(v), 10), true
}

func appendFloatVal(buf []byte, v uint32) ([]byte, bool) {
	return strconv.AppendFloat(buf, float64(math.Float32frombits(v)), 'g', -1, 32), true
}

func appendFixed32Val(buf []byte, v uint32) ([]byte, bool) { return strconv.AppendUint(buf, uint64(v), 10), true }
func appendSfixed32(buf []byte, v uint32) ([]byte, bool)   { return strconv.AppendInt(buf, int64(int32(v)), 10), true }

func appendDoubleVal(buf []byte, v uint64) ([]byte, bool) {
	return strconv.AppendFloat(buf, math.Float64frombits(v), 'g', -1, 64), true
}

func appendFixed64Val(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendUint(buf, v, 10), true }
func appendSfixed64(buf []byte, v uint64) ([]byte, bool)   { return strconv.AppendInt(buf, int64(v), 10), true }

// makeEnumAppendFn returns an appendVarintFn that resolves enum value names.
func makeEnumAppendFn(enumDesc protoreflect.EnumDescriptor) appendVarintFn {
	return func(buf []byte, v uint64) ([]byte, bool) {
		ev := enumDesc.Values().ByNumber(protoreflect.EnumNumber(v))
		if ev == nil {
			return strconv.AppendInt(buf, int64(v), 10), true
		}
		buf = append(buf, '"')
		buf = append(buf, ev.Name()...)
		buf = append(buf, '"')
		return buf, false
	}
}

