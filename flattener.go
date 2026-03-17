// Package quaminapb implements a quamina.Flattener for binary-encoded Protocol Buffer messages.
package quaminapb

import (
	"fmt"
	"strconv"

	quamina "quamina.net/go/quamina/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Flattener implements quamina.Flattener for binary-encoded protobuf messages.
// Construct one with New; it is not safe for concurrent use across goroutines
// without calling Copy first.
type Flattener struct {
	desc       protoreflect.MessageDescriptor
	allSchemas map[protoreflect.FullName]*msgSchema

	// reused across Flatten calls to reduce allocations
	fields      []quamina.Field
	valBuf      []byte            // backing buffer for all Val slices
	arrayPosBuf []quamina.ArrayPos // backing buffer for all ArrayTrail slices
	nextArray   int32

	// Per-nesting-level array tracking. Index equals the current recursion
	// depth. A fixed array means pointers into it are never invalidated by
	// growth, so closures can safely receive *fieldArrays without causing
	// escape-analysis allocations. 32 levels covers all practical proto depths.
	msgArrays [32]fieldArrays
	msgDepth  int32
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

// reset clears the maps for reuse, retaining their allocated backing storage.
func (a *fieldArrays) reset() {
	for k := range a.ids {
		delete(a.ids, k)
	}
	for k := range a.pos {
		delete(a.pos, k)
	}
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

	if f.msgDepth >= int32(len(f.msgArrays)) {
		return fmt.Errorf("quaminapb: message nesting depth exceeds %d", len(f.msgArrays))
	}
	arrays := &f.msgArrays[f.msgDepth]
	arrays.reset()
	f.msgDepth++
	defer func() { f.msgDepth-- }()

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
		data, err = h.fn(f, data, typ == protowire.BytesType, tracker, arrayTrail, arrays)
		if err != nil {
			return err
		}
	}
	return nil
}

// flattenMapEntry parses a single map-entry message (key=1, value=2) and emits the value
// with a path extended by the key. keyFd and valEmit are precomputed by makeMapHandler.
func (f *Flattener) flattenMapEntry(
	data []byte,
	keyFd protoreflect.FieldDescriptor,
	valEmit mapValueEmitFn,
	tracker quamina.SegmentsTreeTracker,
	fieldName []byte,
	arrayTrail []quamina.ArrayPos,
) error {
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
		keyBytes = []byte{}
	}

	mapTracker, ok := tracker.Get(fieldName)
	if !ok {
		return nil
	}
	if !mapTracker.IsSegmentUsed(keyBytes) {
		return nil
	}

	// Second pass: emit the value.
	scan = data
	for len(scan) > 0 {
		num, typ, n := protowire.ConsumeTag(scan)
		if n < 0 {
			return protowire.ParseError(n)
		}
		scan = scan[n:]
		if num == 2 {
			if err := valEmit(f, scan, mapTracker, keyBytes, arrayTrail); err != nil {
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

