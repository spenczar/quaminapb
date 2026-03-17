package quaminapb

import (
	"encoding/base64"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	quamina "quamina.net/go/quamina/v2"
)

// fieldHandler bundles the pre-allocated field name with a type-specific fn
// built once at schema construction time.
type fieldHandler struct {
	name []byte
	fn   func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker,
		arrayTrail []quamina.ArrayPos,
		arrays *fieldArrays) ([]byte, error)
}

// msgSchema holds one handler per field number.
type msgSchema struct {
	handlers map[protowire.Number]*fieldHandler
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

// buildFieldHandler constructs a fieldHandler for fd, selecting the right factory
// based on fd.Kind() and fd.IsList(). The type switch happens once here at
// construction time, so the hot loop pays zero branches per field.
func buildFieldHandler(fd protoreflect.FieldDescriptor) *fieldHandler {
	name := []byte(fd.Name())
	num := protowire.Number(fd.Number())

	if fd.IsMap() {
		return makeMapHandler(name, fd)
	}

	isList := fd.IsList()

	switch fd.Kind() {
	case protoreflect.BoolKind:
		return makeVarintHandler(name, num, isList, appendBool)
	case protoreflect.EnumKind:
		return makeVarintHandler(name, num, isList, makeEnumAppendFn(fd.Enum()))
	case protoreflect.Sint32Kind:
		return makeVarintHandler(name, num, isList, appendSint32)
	case protoreflect.Sint64Kind:
		return makeVarintHandler(name, num, isList, appendSint64)
	case protoreflect.Int32Kind:
		return makeVarintHandler(name, num, isList, appendInt32)
	case protoreflect.Int64Kind:
		return makeVarintHandler(name, num, isList, appendInt64)
	case protoreflect.Uint32Kind, protoreflect.Uint64Kind:
		return makeVarintHandler(name, num, isList, appendUint64)
	case protoreflect.FloatKind:
		return makeFixed32Handler(name, num, isList, appendFloatVal)
	case protoreflect.Fixed32Kind:
		return makeFixed32Handler(name, num, isList, appendFixed32Val)
	case protoreflect.Sfixed32Kind:
		return makeFixed32Handler(name, num, isList, appendSfixed32)
	case protoreflect.DoubleKind:
		return makeFixed64Handler(name, num, isList, appendDoubleVal)
	case protoreflect.Fixed64Kind:
		return makeFixed64Handler(name, num, isList, appendFixed64Val)
	case protoreflect.Sfixed64Kind:
		return makeFixed64Handler(name, num, isList, appendSfixed64)
	case protoreflect.StringKind:
		if isList {
			return makeListStringHandler(name, num)
		}
		return makeSingularStringHandler(name)
	case protoreflect.BytesKind:
		if isList {
			return makeListBytesHandler(name, num)
		}
		return makeSingularBytesHandler(name)
	case protoreflect.MessageKind, protoreflect.GroupKind:
		if isList {
			return makeListMessageHandler(name, num, fd.Message())
		}
		return makeSingularMessageHandler(name, fd.Message())
	}
	return nil
}

// --- Handler factories ---
//
// Each factory closes over the pre-computed name, field number, and
// kind-specific formatting function. The fn receives *fieldArrays pointing
// into Flattener.msgArrays (already heap-allocated), so there is no
// per-call allocation from passing the pointer through the fn field.

func makeVarintHandler(name []byte, num protowire.Number, isList bool, af appendVarintFn) *fieldHandler {
	if isList {
		return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
			tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
			if typ == protowire.BytesType {
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
			} else {
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
			}
			return data, nil
		}}
	}
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeFixed32Handler(name []byte, num protowire.Number, isList bool, af appendFixed32Fn) *fieldHandler {
	if isList {
		return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
			tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
			if typ == protowire.BytesType {
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
			} else {
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
			}
			return data, nil
		}}
	}
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeFixed64Handler(name []byte, num protowire.Number, isList bool, af appendFixed64Fn) *fieldHandler {
	if isList {
		return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
			tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
			if typ == protowire.BytesType {
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
			} else {
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
			}
			return data, nil
		}}
	}
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeSingularStringHandler(name []byte) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeListStringHandler(name []byte, num protowire.Number) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeSingularBytesHandler(name []byte) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeListBytesHandler(name []byte, num protowire.Number) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeSingularMessageHandler(name []byte, childDesc protoreflect.MessageDescriptor) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeListMessageHandler(name []byte, num protowire.Number, childDesc protoreflect.MessageDescriptor) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
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

func makeMapHandler(name []byte, mapFd protoreflect.FieldDescriptor) *fieldHandler {
	return &fieldHandler{name: name, fn: func(f *Flattener, data []byte, typ protowire.Type,
		tracker quamina.SegmentsTreeTracker, arrayTrail []quamina.ArrayPos, arrays *fieldArrays) ([]byte, error) {
		b, n := protowire.ConsumeBytes(data)
		if n < 0 {
			return nil, protowire.ParseError(n)
		}
		data = data[n:]
		if err := f.flattenMapEntry(b, mapFd, tracker, name, arrayTrail); err != nil {
			return nil, err
		}
		return data, nil
	}}
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
