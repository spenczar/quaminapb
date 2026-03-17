// Package quaminapb implements a quamina.Flattener for binary-encoded Protocol Buffer messages.
package quaminapb

import (
	"encoding/base64"
	"fmt"
	"math"
	"strconv"

	quamina "quamina.net/go/quamina/v2"
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

// Named per-kind append functions, passed to fieldHandler at schema construction time.
var (
	appendBool   appendVarintFn = func(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendBool(buf, v != 0), false }
	appendInt32  appendVarintFn = func(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendInt(buf, int64(int32(v)), 10), true }
	appendInt64  appendVarintFn = func(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendInt(buf, int64(v), 10), true }
	appendUint64 appendVarintFn = func(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendUint(buf, v, 10), true }
	appendSint32 appendVarintFn = func(buf []byte, v uint64) ([]byte, bool) {
		decoded := protowire.DecodeZigZag(v & 0xFFFFFFFF)
		return strconv.AppendInt(buf, int64(int32(decoded)), 10), true
	}
	appendSint64 appendVarintFn = func(buf []byte, v uint64) ([]byte, bool) {
		return strconv.AppendInt(buf, protowire.DecodeZigZag(v), 10), true
	}

	appendFloatVal   appendFixed32Fn = func(buf []byte, v uint32) ([]byte, bool) { return strconv.AppendFloat(buf, float64(math.Float32frombits(v)), 'g', -1, 32), true }
	appendSfixed32   appendFixed32Fn = func(buf []byte, v uint32) ([]byte, bool) { return strconv.AppendInt(buf, int64(int32(v)), 10), true }
	appendFixed32Val appendFixed32Fn = func(buf []byte, v uint32) ([]byte, bool) { return strconv.AppendUint(buf, uint64(v), 10), true }

	appendDoubleVal  appendFixed64Fn = func(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendFloat(buf, math.Float64frombits(v), 'g', -1, 64), true }
	appendSfixed64   appendFixed64Fn = func(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendInt(buf, int64(v), 10), true }
	appendFixed64Val appendFixed64Fn = func(buf []byte, v uint64) ([]byte, bool) { return strconv.AppendUint(buf, v, 10), true }
)

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

// handlerKind identifies the pre-computed dispatch path for a field.
type handlerKind uint8

const (
	hkSingularVarint  handlerKind = iota
	hkSingularFixed32             // float, fixed32, sfixed32
	hkSingularFixed64             // double, fixed64, sfixed64
	hkSingularString
	hkSingularBytes
	hkSingularMessage
	hkMapField
	hkListVarint
	hkListFixed32
	hkListFixed64
	hkListString
	hkListBytes
	hkListMessage
)

// fieldHandler bundles the pre-computed field name, dispatch kind, and any
// kind-specific data needed at call time. Built once at schema construction;
// never mutated after that.
type fieldHandler struct {
	name  []byte
	num   protowire.Number             // needed for list array tracking
	kind  handlerKind
	afv   appendVarintFn               // hkSingularVarint, hkListVarint
	af32  appendFixed32Fn              // hkSingularFixed32, hkListFixed32
	af64  appendFixed64Fn              // hkSingularFixed64, hkListFixed64
	child protoreflect.MessageDescriptor // hkSingularMessage, hkListMessage
	mapFd protoreflect.FieldDescriptor  // hkMapField
}

// msgSchema holds one handler per field number.
type msgSchema struct {
	handlers map[protowire.Number]*fieldHandler
}

// Flattener implements quamina.Flattener for binary-encoded protobuf messages.
// Construct one with New; it is not safe for concurrent use across goroutines
// without calling Copy first.
type Flattener struct {
	desc protoreflect.MessageDescriptor
	// allSchemas maps each message type's full name to its pre-built schema,
	// covering the root message and all transitively reachable nested types.
	allSchemas map[protoreflect.FullName]*msgSchema

	// reused across Flatten calls to reduce allocations
	fields      []quamina.Field
	valBuf      []byte            // backing buffer for all Val slices
	arrayPosBuf []quamina.ArrayPos // backing buffer for all ArrayTrail slices
	nextArray   int32
}

// New creates a Flattener for the given MessageDescriptor.
func New(desc protoreflect.MessageDescriptor) *Flattener {
	allSchemas := make(map[protoreflect.FullName]*msgSchema)
	buildAllSchemas(desc, allSchemas)
	return &Flattener{
		desc:       desc,
		allSchemas: allSchemas,
		fields:     make([]quamina.Field, 0, 32),
	}
}

// buildAllSchemas recursively builds msgSchema entries for desc and all transitively
// reachable message types, storing them in out. Cycles are detected via the out map.
func buildAllSchemas(desc protoreflect.MessageDescriptor, out map[protoreflect.FullName]*msgSchema) {
	if _, seen := out[desc.FullName()]; seen {
		return
	}
	fds := desc.Fields()
	schema := &msgSchema{
		handlers: make(map[protowire.Number]*fieldHandler, fds.Len()),
	}
	out[desc.FullName()] = schema
	for i := range fds.Len() {
		fd := fds.Get(i)
		num := protowire.Number(fd.Number())
		schema.handlers[num] = buildFieldHandler(fd)
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			buildAllSchemas(fd.Message(), out)
		}
	}
}

// buildFieldHandler constructs a fieldHandler for fd, selecting the right kind
// based on fd.Kind() and fd.IsList(). The type switch happens once here at
// construction time, so the hot loop pays zero branches per field.
func buildFieldHandler(fd protoreflect.FieldDescriptor) *fieldHandler {
	name := []byte(fd.Name())
	num := protowire.Number(fd.Number())

	if fd.IsMap() {
		return &fieldHandler{name: name, num: num, kind: hkMapField, mapFd: fd}
	}

	isList := fd.IsList()

	switch fd.Kind() {
	case protoreflect.BoolKind:
		k := hkSingularVarint
		if isList {
			k = hkListVarint
		}
		return &fieldHandler{name: name, num: num, kind: k, afv: appendBool}

	case protoreflect.EnumKind:
		k := hkSingularVarint
		if isList {
			k = hkListVarint
		}
		return &fieldHandler{name: name, num: num, kind: k, afv: makeEnumAppendFn(fd.Enum())}

	case protoreflect.Sint32Kind:
		k := hkSingularVarint
		if isList {
			k = hkListVarint
		}
		return &fieldHandler{name: name, num: num, kind: k, afv: appendSint32}

	case protoreflect.Sint64Kind:
		k := hkSingularVarint
		if isList {
			k = hkListVarint
		}
		return &fieldHandler{name: name, num: num, kind: k, afv: appendSint64}

	case protoreflect.Int32Kind:
		k := hkSingularVarint
		if isList {
			k = hkListVarint
		}
		return &fieldHandler{name: name, num: num, kind: k, afv: appendInt32}

	case protoreflect.Int64Kind:
		k := hkSingularVarint
		if isList {
			k = hkListVarint
		}
		return &fieldHandler{name: name, num: num, kind: k, afv: appendInt64}

	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		k := hkSingularVarint
		if isList {
			k = hkListVarint
		}
		return &fieldHandler{name: name, num: num, kind: k, afv: appendUint64}

	case protoreflect.FloatKind:
		k := hkSingularFixed32
		if isList {
			k = hkListFixed32
		}
		return &fieldHandler{name: name, num: num, kind: k, af32: appendFloatVal}

	case protoreflect.Fixed32Kind:
		k := hkSingularFixed32
		if isList {
			k = hkListFixed32
		}
		return &fieldHandler{name: name, num: num, kind: k, af32: appendFixed32Val}

	case protoreflect.Sfixed32Kind:
		k := hkSingularFixed32
		if isList {
			k = hkListFixed32
		}
		return &fieldHandler{name: name, num: num, kind: k, af32: appendSfixed32}

	case protoreflect.DoubleKind:
		k := hkSingularFixed64
		if isList {
			k = hkListFixed64
		}
		return &fieldHandler{name: name, num: num, kind: k, af64: appendDoubleVal}

	case protoreflect.Fixed64Kind:
		k := hkSingularFixed64
		if isList {
			k = hkListFixed64
		}
		return &fieldHandler{name: name, num: num, kind: k, af64: appendFixed64Val}

	case protoreflect.Sfixed64Kind:
		k := hkSingularFixed64
		if isList {
			k = hkListFixed64
		}
		return &fieldHandler{name: name, num: num, kind: k, af64: appendSfixed64}

	case protoreflect.StringKind:
		k := hkSingularString
		if isList {
			k = hkListString
		}
		return &fieldHandler{name: name, num: num, kind: k}

	case protoreflect.BytesKind:
		k := hkSingularBytes
		if isList {
			k = hkListBytes
		}
		return &fieldHandler{name: name, num: num, kind: k}

	case protoreflect.MessageKind, protoreflect.GroupKind:
		k := hkSingularMessage
		if isList {
			k = hkListMessage
		}
		return &fieldHandler{name: name, num: num, kind: k, child: fd.Message()}
	}
	return nil
}

// Copy implements quamina.Flattener. The schema tables (allSchemas) are
// immutable after construction and are shared with the copy; only the
// per-call mutable buffers get fresh allocations.
func (f *Flattener) Copy() quamina.Flattener {
	return &Flattener{
		desc:       f.desc,
		allSchemas: f.allSchemas,
		fields:     make([]quamina.Field, 0, cap(f.fields)),
	}
}

// Flatten implements quamina.Flattener.
func (f *Flattener) Flatten(event []byte, tracker quamina.SegmentsTreeTracker) ([]quamina.Field, error) {
	f.fields = f.fields[:0]
	f.valBuf = f.valBuf[:0]
	f.arrayPosBuf = f.arrayPosBuf[:0]
	f.nextArray = 0
	err := f.flattenMsg(event, f.desc, tracker, nil)
	return f.fields, err
}

// fieldArrays tracks array IDs and per-field positions for repeated fields within a
// single message. Both maps are keyed by field number: ids maps a field number to its
// unique array ID, pos maps it to the next unused position index within that array.
type fieldArrays struct {
	ids map[protowire.Number]int32
	pos map[protowire.Number]int32
}

// arrayID returns the array ID for field num, assigning one if this is the first occurrence.
func (a *fieldArrays) arrayID(num protowire.Number, nextArray *int32) int32 {
	if a.ids == nil {
		a.ids = make(map[protowire.Number]int32)
	}
	id, exists := a.ids[num]
	if !exists {
		*nextArray++
		id = *nextArray
		a.ids[num] = id
	}
	return id
}

// nextPos returns the current position index for field num and increments it.
func (a *fieldArrays) nextPos(num protowire.Number) int32 {
	if a.pos == nil {
		a.pos = make(map[protowire.Number]int32)
	}
	pos := a.pos[num]
	a.pos[num] = pos + 1
	return pos
}

// trail appends a new ArrayPos for field num to buf, copies parent into buf first,
// and returns the resulting sub-slice. buf is the Flattener's arrayPosBuf.
func (a *fieldArrays) trail(num protowire.Number, parent []quamina.ArrayPos, buf *[]quamina.ArrayPos, nextArray *int32) []quamina.ArrayPos {
	id := a.arrayID(num, nextArray)
	pos := a.nextPos(num)
	start := len(*buf)
	*buf = append(*buf, parent...)
	*buf = append(*buf, quamina.ArrayPos{Array: id, Pos: pos})
	return (*buf)[start:]
}

// flattenMsg recursively parses a protobuf-encoded message, emitting quamina Fields for
// every leaf that the tracker considers used.
func (f *Flattener) flattenMsg(
	data []byte,
	desc protoreflect.MessageDescriptor,
	tracker quamina.SegmentsTreeTracker,
	arrayTrail []quamina.ArrayPos,
) error {
	schema := f.allSchemas[desc.FullName()]
	var arrays fieldArrays

	for len(data) > 0 {
		num, typ, n := protowire.ConsumeTag(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		data = data[n:]

		h, ok := schema.handlers[num]
		if !ok {
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}
		if !tracker.IsSegmentUsed(h.name) {
			n = protowire.ConsumeFieldValue(num, typ, data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			data = data[n:]
			continue
		}
		var err error
		data, err = f.dispatchHandler(h, data, typ, tracker, arrayTrail, &arrays)
		if err != nil {
			return err
		}
	}
	return nil
}

// dispatchHandler consumes one field value from data (immediately following the
// already-consumed tag) and emits any matching quamina.Fields.
// Using a regular method call (not a func field) lets the compiler see through
// it for escape analysis, keeping tracker and arrays stack-allocated.
func (f *Flattener) dispatchHandler(
	h *fieldHandler,
	data []byte,
	typ protowire.Type,
	tracker quamina.SegmentsTreeTracker,
	arrayTrail []quamina.ArrayPos,
	arrays *fieldArrays,
) ([]byte, error) {
	switch h.kind {
	case hkSingularVarint:
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(h.name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = h.afv(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}

	case hkSingularFixed32:
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(h.name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = h.af32(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}

	case hkSingularFixed64:
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(h.name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = h.af64(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}

	case hkSingularString:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(h.name); path != nil {
			start := len(f.valBuf)
			f.valBuf = append(f.valBuf, '"')
			f.valBuf = append(f.valBuf, b...)
			f.valBuf = append(f.valBuf, '"')
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
			})
		}

	case hkSingularBytes:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(h.name); path != nil {
			start := len(f.valBuf)
			f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
			})
		}

	case hkSingularMessage:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if child, ok := tracker.Get(h.name); ok {
			if err := f.flattenMsg(b, h.child, child, arrayTrail); err != nil {
				return nil, err
			}
		}

	case hkMapField:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if err := f.flattenMapEntry(b, h.mapFd, tracker, h.name, arrayTrail); err != nil {
			return nil, err
		}

	case hkListVarint:
		if typ == protowire.BytesType {
			// packed repeated
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			if path := tracker.PathForSegment(h.name); path != nil {
				if err := f.decodePackedVarint(b, h.num, path, h.afv, arrays, arrayTrail); err != nil {
					return nil, err
				}
			}
		} else {
			// non-packed repeated
			v, n := protowire.ConsumeVarint(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			fieldTrail := arrays.trail(h.num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
			if path := tracker.PathForSegment(h.name); path != nil {
				start := len(f.valBuf)
				var isNum bool
				f.valBuf, isNum = h.afv(f.valBuf, v)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
				})
			}
		}

	case hkListFixed32:
		if typ == protowire.BytesType {
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			if path := tracker.PathForSegment(h.name); path != nil {
				if err := f.decodePackedFixed32(b, h.num, path, h.af32, arrays, arrayTrail); err != nil {
					return nil, err
				}
			}
		} else {
			v, n := protowire.ConsumeFixed32(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			fieldTrail := arrays.trail(h.num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
			if path := tracker.PathForSegment(h.name); path != nil {
				start := len(f.valBuf)
				var isNum bool
				f.valBuf, isNum = h.af32(f.valBuf, v)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
				})
			}
		}

	case hkListFixed64:
		if typ == protowire.BytesType {
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			if path := tracker.PathForSegment(h.name); path != nil {
				if err := f.decodePackedFixed64(b, h.num, path, h.af64, arrays, arrayTrail); err != nil {
					return nil, err
				}
			}
		} else {
			v, n := protowire.ConsumeFixed64(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			fieldTrail := arrays.trail(h.num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
			if path := tracker.PathForSegment(h.name); path != nil {
				start := len(f.valBuf)
				var isNum bool
				f.valBuf, isNum = h.af64(f.valBuf, v)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
				})
			}
		}

	case hkListString:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		fieldTrail := arrays.trail(h.num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
		if path := tracker.PathForSegment(h.name); path != nil {
			start := len(f.valBuf)
			f.valBuf = append(f.valBuf, '"')
			f.valBuf = append(f.valBuf, b...)
			f.valBuf = append(f.valBuf, '"')
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: false,
			})
		}

	case hkListBytes:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		fieldTrail := arrays.trail(h.num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
		if path := tracker.PathForSegment(h.name); path != nil {
			start := len(f.valBuf)
			f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: false,
			})
		}

	case hkListMessage:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		fieldTrail := arrays.trail(h.num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
		if child, ok := tracker.Get(h.name); ok {
			if err := f.flattenMsg(b, h.child, child, fieldTrail); err != nil {
				return nil, err
			}
		}

	default:
		n := protowire.ConsumeFieldValue(h.num, typ, data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
	}
	return data, nil
}

// flattenMapEntry parses a single map-entry message (key=1, value=2) and emits the value
// with a path extended by the key.
func (f *Flattener) flattenMapEntry(
	data []byte,
	mapFd protoreflect.FieldDescriptor,
	tracker quamina.SegmentsTreeTracker,
	fieldName []byte,
	arrayTrail []quamina.ArrayPos,
) error {
	entryDesc := mapFd.Message()
	keyFd := entryDesc.Fields().ByNumber(1)
	valFd := entryDesc.Fields().ByNumber(2)
	if keyFd == nil || valFd == nil {
		return fmt.Errorf("flattenpb: map entry missing key or value field")
	}

	// First pass: extract the key.
	var keyBytes []byte
	scan := data
	for len(scan) > 0 {
		num, typ, n := protowire.ConsumeTag(scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
		if num == 1 {
			kb, err := extractKeyBytes(keyFd, scan, typ)
			if err != nil {
				return err
			}
			keyBytes = kb
		}
		n = protowire.ConsumeFieldValue(num, typ, scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
	}
	if keyBytes == nil {
		// Key absent means zero-value key; use empty string for string keys.
		keyBytes = []byte{}
	}

	// Get the child tracker for the map field, then for the specific key.
	mapTracker, ok := tracker.Get(fieldName)
	if !ok {
		return nil
	}
	if !mapTracker.IsSegmentUsed(keyBytes) {
		return nil
	}

	// Second pass: extract and emit the value.
	scan = data
	for len(scan) > 0 {
		num, typ, n := protowire.ConsumeTag(scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
		if num == 2 {
			if err := f.emitMapValue(scan, typ, valFd, mapTracker, keyBytes, arrayTrail); err != nil {
				return err
			}
		}
		n = protowire.ConsumeFieldValue(num, typ, scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
	}
	return nil
}

// extractKeyBytes encodes a map key field as a []byte suitable for use as a path segment.
func extractKeyBytes(keyFd protoreflect.FieldDescriptor, data []byte, typ protowire.Type) ([]byte, error) {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		switch keyFd.Kind() {
		case protoreflect.BoolKind:
			if v != 0 {
				return []byte("true"), nil
			}
			return []byte("false"), nil
		case protoreflect.Sint32Kind:
			return []byte(strconv.FormatInt(int64(int32(protowire.DecodeZigZag(v))), 10)), nil
		case protoreflect.Sint64Kind:
			return []byte(strconv.FormatInt(protowire.DecodeZigZag(v), 10)), nil
		case protoreflect.Int32Kind, protoreflect.Sfixed32Kind:
			return []byte(strconv.FormatInt(int64(int32(v)), 10)), nil
		case protoreflect.Int64Kind, protoreflect.Sfixed64Kind:
			return []byte(strconv.FormatInt(int64(v), 10)), nil
		default:
			return []byte(strconv.FormatUint(v, 10)), nil
		}
	case protowire.BytesType:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		return b, nil
	default:
		return nil, fmt.Errorf("flattenpb: unexpected wire type %v for map key", typ)
	}
}

// emitMapValue emits the map value field, with the key as the final path segment.
func (f *Flattener) emitMapValue(
	data []byte,
	typ protowire.Type,
	valFd protoreflect.FieldDescriptor,
	mapTracker quamina.SegmentsTreeTracker,
	keyBytes []byte,
	arrayTrail []quamina.ArrayPos,
) error {
	switch typ {
	case protowire.VarintType:
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendVarint(f.valBuf, valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.Fixed32Type:
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendFixed32(f.valBuf, valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.Fixed64Type:
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = appendFixed64(f.valBuf, valFd, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
	case protowire.BytesType:
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		_ = n
		switch valFd.Kind() {
		case protoreflect.MessageKind, protoreflect.GroupKind:
			if child, ok := mapTracker.Get(keyBytes); ok {
				return f.flattenMsg(b, valFd.Message(), child, arrayTrail)
			}
		case protoreflect.StringKind:
			if path := mapTracker.PathForSegment(keyBytes); path != nil {
				start := len(f.valBuf)
				f.valBuf = append(f.valBuf, '"')
				f.valBuf = append(f.valBuf, b...)
				f.valBuf = append(f.valBuf, '"')
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
				})
			}
		case protoreflect.BytesKind:
			if path := mapTracker.PathForSegment(keyBytes); path != nil {
				start := len(f.valBuf)
				f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
				})
			}
		}
	}
	return nil
}

// decodePackedVarint decodes a packed repeated varint blob, emitting one Field per element.
func (f *Flattener) decodePackedVarint(
	packed []byte,
	num protowire.Number,
	path []byte,
	af appendVarintFn,
	arrays *fieldArrays,
	arrayTrail []quamina.ArrayPos,
) error {
	aid := arrays.arrayID(num, &f.nextArray)
	for len(packed) > 0 {
		v, n := protowire.ConsumeVarint(packed)
		if n < 0 {
			return protowire.ParseError(n)
		}
		packed = packed[n:]
		valStart := len(f.valBuf)
		var isNum bool
		f.valBuf, isNum = af(f.valBuf, v)
		pos := arrays.nextPos(num)
		trailStart := len(f.arrayPosBuf)
		f.arrayPosBuf = append(f.arrayPosBuf, arrayTrail...)
		f.arrayPosBuf = append(f.arrayPosBuf, quamina.ArrayPos{Array: aid, Pos: pos})
		f.fields = append(f.fields, quamina.Field{
			Path:       path,
			Val:        f.valBuf[valStart:],
			ArrayTrail: f.arrayPosBuf[trailStart:],
			IsNumber:   isNum,
		})
	}
	return nil
}

// decodePackedFixed32 decodes a packed repeated fixed32 blob, emitting one Field per element.
func (f *Flattener) decodePackedFixed32(
	packed []byte,
	num protowire.Number,
	path []byte,
	af appendFixed32Fn,
	arrays *fieldArrays,
	arrayTrail []quamina.ArrayPos,
) error {
	aid := arrays.arrayID(num, &f.nextArray)
	for len(packed) > 0 {
		v, n := protowire.ConsumeFixed32(packed)
		if n < 0 {
			return protowire.ParseError(n)
		}
		packed = packed[n:]
		valStart := len(f.valBuf)
		var isNum bool
		f.valBuf, isNum = af(f.valBuf, v)
		pos := arrays.nextPos(num)
		trailStart := len(f.arrayPosBuf)
		f.arrayPosBuf = append(f.arrayPosBuf, arrayTrail...)
		f.arrayPosBuf = append(f.arrayPosBuf, quamina.ArrayPos{Array: aid, Pos: pos})
		f.fields = append(f.fields, quamina.Field{
			Path:       path,
			Val:        f.valBuf[valStart:],
			ArrayTrail: f.arrayPosBuf[trailStart:],
			IsNumber:   isNum,
		})
	}
	return nil
}

// decodePackedFixed64 decodes a packed repeated fixed64 blob, emitting one Field per element.
func (f *Flattener) decodePackedFixed64(
	packed []byte,
	num protowire.Number,
	path []byte,
	af appendFixed64Fn,
	arrays *fieldArrays,
	arrayTrail []quamina.ArrayPos,
) error {
	aid := arrays.arrayID(num, &f.nextArray)
	for len(packed) > 0 {
		v, n := protowire.ConsumeFixed64(packed)
		if n < 0 {
			return protowire.ParseError(n)
		}
		packed = packed[n:]
		valStart := len(f.valBuf)
		var isNum bool
		f.valBuf, isNum = af(f.valBuf, v)
		pos := arrays.nextPos(num)
		trailStart := len(f.arrayPosBuf)
		f.arrayPosBuf = append(f.arrayPosBuf, arrayTrail...)
		f.arrayPosBuf = append(f.arrayPosBuf, quamina.ArrayPos{Array: aid, Pos: pos})
		f.fields = append(f.fields, quamina.Field{
			Path:       path,
			Val:        f.valBuf[valStart:],
			ArrayTrail: f.arrayPosBuf[trailStart:],
			IsNumber:   isNum,
		})
	}
	return nil
}

// appendVarint appends the quamina Val representation of a varint field to buf.
// Used by emitMapValue.
func appendVarint(buf []byte, fd protoreflect.FieldDescriptor, v uint64) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.BoolKind:
		return strconv.AppendBool(buf, v != 0), false

	case protoreflect.EnumKind:
		ev := fd.Enum().Values().ByNumber(protoreflect.EnumNumber(v))
		if ev == nil {
			return strconv.AppendInt(buf, int64(v), 10), true
		}
		buf = append(buf, '"')
		buf = append(buf, ev.Name()...)
		buf = append(buf, '"')
		return buf, false

	case protoreflect.Sint32Kind:
		decoded := protowire.DecodeZigZag(v & 0xFFFFFFFF)
		return strconv.AppendInt(buf, int64(int32(decoded)), 10), true

	case protoreflect.Sint64Kind:
		return strconv.AppendInt(buf, protowire.DecodeZigZag(v), 10), true

	case protoreflect.Int32Kind:
		return strconv.AppendInt(buf, int64(int32(v)), 10), true

	case protoreflect.Int64Kind:
		return strconv.AppendInt(buf, int64(v), 10), true

	default:
		return strconv.AppendUint(buf, v, 10), true
	}
}

// appendFixed32 appends the quamina Val representation of a fixed32 field to buf.
// Used by emitMapValue.
func appendFixed32(buf []byte, fd protoreflect.FieldDescriptor, v uint32) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.FloatKind:
		return strconv.AppendFloat(buf, float64(math.Float32frombits(v)), 'g', -1, 32), true
	case protoreflect.Sfixed32Kind:
		return strconv.AppendInt(buf, int64(int32(v)), 10), true
	default:
		return strconv.AppendUint(buf, uint64(v), 10), true
	}
}

// appendFixed64 appends the quamina Val representation of a fixed64 field to buf.
// Used by emitMapValue.
func appendFixed64(buf []byte, fd protoreflect.FieldDescriptor, v uint64) ([]byte, bool) {
	switch fd.Kind() {
	case protoreflect.DoubleKind:
		return strconv.AppendFloat(buf, math.Float64frombits(v), 'g', -1, 64), true
	case protoreflect.Sfixed64Kind:
		return strconv.AppendInt(buf, int64(v), 10), true
	default:
		return strconv.AppendUint(buf, v, 10), true
	}
}
