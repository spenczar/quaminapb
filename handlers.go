package quaminapb

import (
	"encoding/base64"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	quamina "quamina.net/go/quamina/v2"
)

// fieldHandler bundles the pre-allocated field name with a type-specific fn
// built once at schema construction time. The fn encodes all type-specific
// logic as a closure — no type switches or descriptor lookups at call time.
type fieldHandler struct {
	name []byte
	fn   func(f *Flattener, data []byte,
		tracker quamina.SegmentsTreeTracker,
		arrayTrail []quamina.ArrayPos,
		arrays *fieldArrays) ([]byte, error)
}

// msgSchema holds one handler per (field number, wire type) pair.
//
// The map key is protowire.EncodeTag(num, typ) — the same uint64 varint that
// appears on the wire. The hot loop reads tags with ConsumeVarint and looks
// them up directly without calling DecodeTag, so decoding only happens in
// the skip paths where ConsumeFieldValue needs num and typ.
//
// Repeated scalar fields get two entries: one at their natural wire type
// (e.g. VarintType for int64) for the non-packed encoding, and one at
// BytesType for the packed encoding. The packed/non-packed distinction is
// resolved at construction time, not per-field at runtime.
type msgSchema struct {
	handlers map[uint64]*fieldHandler
}

// buildAllSchemas recursively builds msgSchema entries for desc and all transitively
// reachable message types, storing them in out. Cycles are detected via the out map.
func buildAllSchemas(desc protoreflect.MessageDescriptor, out map[protoreflect.FullName]*msgSchema) {
	if _, seen := out[desc.FullName()]; seen {
		return
	}
	fds := desc.Fields()
	schema := &msgSchema{
		handlers: make(map[uint64]*fieldHandler, fds.Len()),
	}
	out[desc.FullName()] = schema
	for i := range fds.Len() {
		fd := fds.Get(i)
		registerFieldHandlers(schema, fd)
		if fd.Kind() == protoreflect.MessageKind || fd.Kind() == protoreflect.GroupKind {
			buildAllSchemas(fd.Message(), out)
		}
	}
}

// registerFieldHandlers adds handler(s) for fd to schema.
// Repeated scalar fields get two entries: one for their natural wire type
// (non-packed, one element per tag) and one for BytesType (packed, all elements
// in one blob). All other fields get a single entry.
func registerFieldHandlers(schema *msgSchema, fd protoreflect.FieldDescriptor) {
	name := []byte(fd.Name())
	num := protowire.Number(fd.Number())

	if fd.IsMap() {
		schema.handlers[protowire.EncodeTag(num, protowire.BytesType)] = makeMapHandler(name, fd)
		return
	}

	isList := fd.IsList()

	switch fd.Kind() {
	case protoreflect.BoolKind:
		registerVarintHandlers(schema, name, num, isList, appendBool)
	case protoreflect.EnumKind:
		registerVarintHandlers(schema, name, num, isList, makeEnumAppendFn(fd.Enum()))
	case protoreflect.Sint32Kind:
		registerVarintHandlers(schema, name, num, isList, appendSint32)
	case protoreflect.Sint64Kind:
		registerVarintHandlers(schema, name, num, isList, appendSint64)
	case protoreflect.Int32Kind:
		registerVarintHandlers(schema, name, num, isList, appendInt32)
	case protoreflect.Int64Kind:
		registerVarintHandlers(schema, name, num, isList, appendInt64)
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		registerVarintHandlers(schema, name, num, isList, appendUint64)
	case protoreflect.FloatKind:
		registerFixed32Handlers(schema, name, num, isList, appendFloatVal)
	case protoreflect.Fixed32Kind:
		registerFixed32Handlers(schema, name, num, isList, appendFixed32Val)
	case protoreflect.Sfixed32Kind:
		registerFixed32Handlers(schema, name, num, isList, appendSfixed32)
	case protoreflect.DoubleKind:
		registerFixed64Handlers(schema, name, num, isList, appendDoubleVal)
	case protoreflect.Fixed64Kind:
		registerFixed64Handlers(schema, name, num, isList, appendFixed64Val)
	case protoreflect.Sfixed64Kind:
		registerFixed64Handlers(schema, name, num, isList, appendSfixed64)
	case protoreflect.StringKind:
		schema.handlers[protowire.EncodeTag(num, protowire.BytesType)] = makeStringHandler(name, num, isList)
	case protoreflect.BytesKind:
		schema.handlers[protowire.EncodeTag(num, protowire.BytesType)] = makeBytesHandler(name, num, isList)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		schema.handlers[protowire.EncodeTag(num, protowire.BytesType)] = makeMessageHandler(name, num, isList, fd.Message())
	}
}

func registerVarintHandlers(schema *msgSchema, name []byte, num protowire.Number, isList bool, af appendVarintFn) {
	if isList {
		schema.handlers[protowire.EncodeTag(num, protowire.VarintType)] = makeVarintElementHandler(name, num, af)
		schema.handlers[protowire.EncodeTag(num, protowire.BytesType)] = makeVarintPackedHandler(name, num, af)
	} else {
		schema.handlers[protowire.EncodeTag(num, protowire.VarintType)] = makeSingularVarintHandler(name, af)
	}
}

func registerFixed32Handlers(schema *msgSchema, name []byte, num protowire.Number, isList bool, af appendFixed32Fn) {
	if isList {
		schema.handlers[protowire.EncodeTag(num, protowire.Fixed32Type)] = makeFixed32ElementHandler(name, num, af)
		schema.handlers[protowire.EncodeTag(num, protowire.BytesType)] = makeFixed32PackedHandler(name, num, af)
	} else {
		schema.handlers[protowire.EncodeTag(num, protowire.Fixed32Type)] = makeSingularFixed32Handler(name, af)
	}
}

func registerFixed64Handlers(schema *msgSchema, name []byte, num protowire.Number, isList bool, af appendFixed64Fn) {
	if isList {
		schema.handlers[protowire.EncodeTag(num, protowire.Fixed64Type)] = makeFixed64ElementHandler(name, num, af)
		schema.handlers[protowire.EncodeTag(num, protowire.BytesType)] = makeFixed64PackedHandler(name, num, af)
	} else {
		schema.handlers[protowire.EncodeTag(num, protowire.Fixed64Type)] = makeSingularFixed64Handler(name, af)
	}
}

// --- Handler factories ---
//
// Each factory closes over the pre-computed name, field number, and
// kind-specific formatting function. The fn receives *fieldArrays pointing
// into Flattener.msgArrays (already heap-allocated), so there is no
// per-call allocation from passing the pointer through the fn field.

func makeSingularVarintHandler(name []byte, af appendVarintFn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
		return data, nil
	}}
}

func makeVarintElementHandler(name []byte, num protowire.Number, af appendVarintFn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		fieldTrail := arrays.trail(num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
			})
		}
		return data, nil
	}}
}

func makeVarintPackedHandler(name []byte, num protowire.Number, af appendVarintFn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			if err := f.decodePackedVarint(b, num, path, af, arrays, arrayTrail); err != nil {
				return nil, err
			}
		}
		return data, nil
	}}
}

func makeSingularFixed32Handler(name []byte, af appendFixed32Fn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
		return data, nil
	}}
}

func makeFixed32ElementHandler(name []byte, num protowire.Number, af appendFixed32Fn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		fieldTrail := arrays.trail(num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
			})
		}
		return data, nil
	}}
}

func makeFixed32PackedHandler(name []byte, num protowire.Number, af appendFixed32Fn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			if err := f.decodePackedFixed32(b, num, path, af, arrays, arrayTrail); err != nil {
				return nil, err
			}
		}
		return data, nil
	}}
}

func makeSingularFixed64Handler(name []byte, af appendFixed64Fn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
		return data, nil
	}}
}

func makeFixed64ElementHandler(name []byte, num protowire.Number, af appendFixed64Fn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		fieldTrail := arrays.trail(num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: isNum,
			})
		}
		return data, nil
	}}
}

func makeFixed64PackedHandler(name []byte, num protowire.Number, af appendFixed64Fn) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			if err := f.decodePackedFixed64(b, num, path, af, arrays, arrayTrail); err != nil {
				return nil, err
			}
		}
		return data, nil
	}}
}

func makeStringHandler(name []byte, num protowire.Number, isList bool) *fieldHandler {
	if isList {
		return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			fieldTrail := arrays.trail(num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
			if path := tracker.PathForSegment(name); path != nil {
				start := len(f.valBuf)
				f.valBuf = append(f.valBuf, '"')
				f.valBuf = append(f.valBuf, b...)
				f.valBuf = append(f.valBuf, '"')
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: false,
				})
			}
			return data, nil
		}}
	}
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			f.valBuf = append(f.valBuf, '"')
			f.valBuf = append(f.valBuf, b...)
			f.valBuf = append(f.valBuf, '"')
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
			})
		}
		return data, nil
	}}
}

func makeBytesHandler(name []byte, num protowire.Number, isList bool) *fieldHandler {
	if isList {
		return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			fieldTrail := arrays.trail(num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
			if path := tracker.PathForSegment(name); path != nil {
				start := len(f.valBuf)
				f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
				f.fields = append(f.fields, quamina.Field{
					Path: path, Val: f.valBuf[start:], ArrayTrail: fieldTrail, IsNumber: false,
				})
			}
			return data, nil
		}}
	}
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if path := tracker.PathForSegment(name); path != nil {
			start := len(f.valBuf)
			f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
			})
		}
		return data, nil
	}}
}

func makeMessageHandler(name []byte, num protowire.Number, isList bool, childDesc protoreflect.MessageDescriptor) *fieldHandler {
	if isList {
		return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return nil, protowire.ParseError(n)
			}
			data = data[n:]
			fieldTrail := arrays.trail(num, arrayTrail, &f.arrayPosBuf, &f.nextArray)
			if child, ok := tracker.Get(name); ok {
				if err := f.flattenMsg(b, childDesc, child, fieldTrail); err != nil {
					return nil, err
				}
			}
			return data, nil
		}}
	}
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if child, ok := tracker.Get(name); ok {
			if err := f.flattenMsg(b, childDesc, child, arrayTrail); err != nil {
				return nil, err
			}
		}
		return data, nil
	}}
}

// mapValueEmitFn emits a single map value. data is the raw wire bytes starting
// immediately after the field tag (the same slice that would be passed to a
// protowire.ConsumeXxx call). The concrete function is chosen once at schema
// construction time based on the value field's kind.
type mapValueEmitFn func(f *Flattener, data []byte,
	mapTracker quamina.SegmentsTreeTracker,
	keyBytes []byte,
	arrayTrail []quamina.ArrayPos) error

func makeMapHandler(name []byte, mapFd protoreflect.FieldDescriptor) *fieldHandler {
	entryDesc := mapFd.Message()
	keyFd := entryDesc.Fields().ByNumber(1)
	valEmit := buildMapValueEmitter(entryDesc.Fields().ByNumber(2))
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if err := f.flattenMapEntry(b, keyFd, valEmit, tracker, name, arrayTrail); err != nil {
			return nil, err
		}
		return data, nil
	}}
}

// buildMapValueEmitter selects and returns the right mapValueEmitFn for valFd's kind.
// Called once at schema construction time.
func buildMapValueEmitter(valFd protoreflect.FieldDescriptor) mapValueEmitFn {
	switch valFd.Kind() {
	case protoreflect.BoolKind:
		return makeVarintMapValue(appendBool)
	case protoreflect.EnumKind:
		return makeVarintMapValue(makeEnumAppendFn(valFd.Enum()))
	case protoreflect.Sint32Kind:
		return makeVarintMapValue(appendSint32)
	case protoreflect.Sint64Kind:
		return makeVarintMapValue(appendSint64)
	case protoreflect.Int32Kind:
		return makeVarintMapValue(appendInt32)
	case protoreflect.Int64Kind:
		return makeVarintMapValue(appendInt64)
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return makeVarintMapValue(appendUint64)
	case protoreflect.FloatKind:
		return makeFixed32MapValue(appendFloatVal)
	case protoreflect.Fixed32Kind:
		return makeFixed32MapValue(appendFixed32Val)
	case protoreflect.Sfixed32Kind:
		return makeFixed32MapValue(appendSfixed32)
	case protoreflect.DoubleKind:
		return makeFixed64MapValue(appendDoubleVal)
	case protoreflect.Fixed64Kind:
		return makeFixed64MapValue(appendFixed64Val)
	case protoreflect.Sfixed64Kind:
		return makeFixed64MapValue(appendSfixed64)
	case protoreflect.StringKind:
		return emitStringMapValue
	case protoreflect.BytesKind:
		return emitBytesMapValue
	case protoreflect.MessageKind, protoreflect.GroupKind:
		childDesc := valFd.Message()
		return func(f *Flattener, data []byte, mapTracker quamina.SegmentsTreeTracker, keyBytes []byte, arrayTrail []quamina.ArrayPos) error {
			b, n := protowire.ConsumeBytes(data)
			if n < 0 {
				return protowire.ParseError(n)
			}
			if child, ok := mapTracker.Get(keyBytes); ok {
				return f.flattenMsg(b, childDesc, child, arrayTrail)
			}
			return nil
		}
	}
	return nil
}

func makeVarintMapValue(af appendVarintFn) mapValueEmitFn {
	return func(f *Flattener, data []byte, mapTracker quamina.SegmentsTreeTracker, keyBytes []byte, arrayTrail []quamina.ArrayPos) error {
		v, n := protowire.ConsumeVarint(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
		return nil
	}
}

func makeFixed32MapValue(af appendFixed32Fn) mapValueEmitFn {
	return func(f *Flattener, data []byte, mapTracker quamina.SegmentsTreeTracker, keyBytes []byte, arrayTrail []quamina.ArrayPos) error {
		v, n := protowire.ConsumeFixed32(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
		return nil
	}
}

func makeFixed64MapValue(af appendFixed64Fn) mapValueEmitFn {
	return func(f *Flattener, data []byte, mapTracker quamina.SegmentsTreeTracker, keyBytes []byte, arrayTrail []quamina.ArrayPos) error {
		v, n := protowire.ConsumeFixed64(data)
		if n < 0 {
			return protowire.ParseError(n)
		}
		if path := mapTracker.PathForSegment(keyBytes); path != nil {
			start := len(f.valBuf)
			var isNum bool
			f.valBuf, isNum = af(f.valBuf, v)
			f.fields = append(f.fields, quamina.Field{
				Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: isNum,
			})
		}
		return nil
	}
}

func emitStringMapValue(f *Flattener, data []byte, mapTracker quamina.SegmentsTreeTracker, keyBytes []byte, arrayTrail []quamina.ArrayPos) error {
	b, n := protowire.ConsumeBytes(data)
	if n < 0 {
		return protowire.ParseError(n)
	}
	if path := mapTracker.PathForSegment(keyBytes); path != nil {
		start := len(f.valBuf)
		f.valBuf = append(f.valBuf, '"')
		f.valBuf = append(f.valBuf, b...)
		f.valBuf = append(f.valBuf, '"')
		f.fields = append(f.fields, quamina.Field{
			Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
		})
	}
	return nil
}

func emitBytesMapValue(f *Flattener, data []byte, mapTracker quamina.SegmentsTreeTracker, keyBytes []byte, arrayTrail []quamina.ArrayPos) error {
	b, n := protowire.ConsumeBytes(data)
	if n < 0 {
		return protowire.ParseError(n)
	}
	if path := mapTracker.PathForSegment(keyBytes); path != nil {
		start := len(f.valBuf)
		f.valBuf = base64.StdEncoding.AppendEncode(f.valBuf, b)
		f.fields = append(f.fields, quamina.Field{
			Path: path, Val: f.valBuf[start:], ArrayTrail: arrayTrail, IsNumber: false,
		})
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
