package flattenpb_test

import (
	"bytes"
	"testing"

	"github.com/spenczar/quamina-protobuf/flattenpb"
	"github.com/spenczar/quamina-protobuf/internal/testproto"
	"github.com/spenczar/quamina-protobuf/internal/testtracker"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	quamina "quamina.net/go/quamina/v2"
)

// testDesc is the MessageDescriptor for TestMsg, obtained from the generated type.
var testDesc protoreflect.MessageDescriptor

func init() {
	testDesc = (&testproto.TestMsg{}).ProtoReflect().Descriptor()
}

func mustMarshal(m proto.Message) []byte {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic("mustMarshal: " + err.Error())
	}
	return b
}

// ---------------------------------------------------------------------------
// Test helper
// ---------------------------------------------------------------------------

type tc struct {
	name      string
	data      []byte
	paths     []string // tracker paths
	wantPaths []string
	wantVals  []string
	wantErr   bool
}

func runCases(t *testing.T, cases []tc) {
	t.Helper()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fl := flattenpb.New(testDesc)
			tr := testtracker.New(c.paths...)
			fields, err := fl.Flatten(c.data, tr)
			if c.wantErr {
				if err == nil {
					t.Fatal("wanted error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			checkFields(t, fields, c.wantPaths, c.wantVals)
		})
	}
}

func checkFields(t *testing.T, got []quamina.Field, wantPaths, wantVals []string) {
	t.Helper()
	if len(got) != len(wantPaths) {
		t.Errorf("got %d fields, want %d", len(got), len(wantPaths))
		for _, f := range got {
			t.Logf("  path=%q val=%q", f.Path, f.Val)
		}
		return
	}
	for i, f := range got {
		if !bytes.Equal(f.Path, []byte(wantPaths[i])) {
			t.Errorf("[%d] path: got %q want %q", i, f.Path, wantPaths[i])
		}
		if !bytes.Equal(f.Val, []byte(wantVals[i])) {
			t.Errorf("[%d] val:  got %q want %q", i, f.Val, wantVals[i])
		}
	}
}

func TestScalars(t *testing.T) {
	runCases(t, []tc{
		{
			name:      "int32",
			data:      mustMarshal(&testproto.TestMsg{Id: 42}),
			paths:     []string{"id"},
			wantPaths: []string{"id"},
			wantVals:  []string{"42"},
		},
		{
			name:      "int32 negative",
			data:      mustMarshal(&testproto.TestMsg{Id: -7}),
			paths:     []string{"id"},
			wantPaths: []string{"id"},
			wantVals:  []string{"-7"},
		},
		{
			name:      "string",
			data:      mustMarshal(&testproto.TestMsg{Name: "hello"}),
			paths:     []string{"name"},
			wantPaths: []string{"name"},
			wantVals:  []string{`"hello"`},
		},
		{
			name:      "bool true",
			data:      mustMarshal(&testproto.TestMsg{Flag: true}),
			paths:     []string{"flag"},
			wantPaths: []string{"flag"},
			wantVals:  []string{"true"},
		},
		{
			name:      "float32",
			data:      mustMarshal(&testproto.TestMsg{Score: 3.14}),
			paths:     []string{"score"},
			wantPaths: []string{"score"},
			wantVals:  []string{"3.14"},
		},
		{
			name:      "float64",
			data:      mustMarshal(&testproto.TestMsg{Ratio: 2.718281828}),
			paths:     []string{"ratio"},
			wantPaths: []string{"ratio"},
			wantVals:  []string{"2.718281828"},
		},
		{
			name:      "bytes",
			data:      mustMarshal(&testproto.TestMsg{Data: []byte("abc")}),
			paths:     []string{"data"},
			wantPaths: []string{"data"},
			wantVals:  []string{"YWJj"}, // base64("abc")
		},
		{
			name:      "enum known",
			data:      mustMarshal(&testproto.TestMsg{Status: testproto.TestStatus_PENDING}),
			paths:     []string{"status"},
			wantPaths: []string{"status"},
			wantVals:  []string{`"PENDING"`},
		},
		{
			name:      "enum unknown value",
			data:      mustMarshal(&testproto.TestMsg{Status: testproto.TestStatus(99)}),
			paths:     []string{"status"},
			wantPaths: []string{"status"},
			wantVals:  []string{"99"},
		},
		{
			name:      "sint32 positive",
			data:      mustMarshal(&testproto.TestMsg{SignedVal: 5}),
			paths:     []string{"signed_val"},
			wantPaths: []string{"signed_val"},
			wantVals:  []string{"5"},
		},
		{
			name:      "sint32 negative",
			data:      mustMarshal(&testproto.TestMsg{SignedVal: -3}),
			paths:     []string{"signed_val"},
			wantPaths: []string{"signed_val"},
			wantVals:  []string{"-3"},
		},
		{
			name:      "uint64",
			data:      mustMarshal(&testproto.TestMsg{Big: 1 << 40}),
			paths:     []string{"big"},
			wantPaths: []string{"big"},
			wantVals:  []string{"1099511627776"},
		},
		{
			name:      "sfixed32",
			data:      mustMarshal(&testproto.TestMsg{Sf32: -100}),
			paths:     []string{"sf32"},
			wantPaths: []string{"sf32"},
			wantVals:  []string{"-100"},
		},
		{
			name:      "sfixed64",
			data:      mustMarshal(&testproto.TestMsg{Sf64: -999999999999}),
			paths:     []string{"sf64"},
			wantPaths: []string{"sf64"},
			wantVals:  []string{"-999999999999"},
		},
		{
			name:      "fixed32",
			data:      mustMarshal(&testproto.TestMsg{F32: 42}),
			paths:     []string{"f32"},
			wantPaths: []string{"f32"},
			wantVals:  []string{"42"},
		},
		{
			name:      "fixed64",
			data:      mustMarshal(&testproto.TestMsg{F64: 9999}),
			paths:     []string{"f64"},
			wantPaths: []string{"f64"},
			wantVals:  []string{"9999"},
		},
		{
			name:      "multiple scalars",
			data:      mustMarshal(&testproto.TestMsg{Id: 7, Name: "x"}),
			paths:     []string{"id", "name"},
			wantPaths: []string{"id", "name"},
			wantVals:  []string{"7", `"x"`},
		},
	})
}

func TestTrackerPruning(t *testing.T) {
	runCases(t, []tc{
		{
			name:      "field not in tracker is not emitted",
			data:      mustMarshal(&testproto.TestMsg{Id: 1, Name: "skip"}),
			paths:     []string{"id"}, // only "id" registered
			wantPaths: []string{"id"},
			wantVals:  []string{"1"},
		},
		{
			name:      "no fields registered",
			data:      mustMarshal(&testproto.TestMsg{Id: 42}),
			paths:     []string{"name"}, // unrelated path
			wantPaths: []string{},
			wantVals:  []string{},
		},
	})
}

func TestNestedMessage(t *testing.T) {
	msg := mustMarshal(&testproto.TestMsg{
		Nested: &testproto.NestedMsg{Value: "world", Count: 99},
	})

	runCases(t, []tc{
		{
			name:      "nested leaf",
			data:      msg,
			paths:     []string{"nested\nvalue"},
			wantPaths: []string{"nested\nvalue"},
			wantVals:  []string{`"world"`},
		},
		{
			name:      "nested both leaves",
			data:      msg,
			paths:     []string{"nested\nvalue", "nested\ncount"},
			wantPaths: []string{"nested\nvalue", "nested\ncount"},
			wantVals:  []string{`"world"`, "99"},
		},
		{
			name:      "nested not registered — skipped",
			data:      msg,
			paths:     []string{"id"},
			wantPaths: []string{},
			wantVals:  []string{},
		},
	})
}

func TestTwoLevelNesting(t *testing.T) {
	msg := mustMarshal(&testproto.TestMsg{
		Nested: &testproto.NestedMsg{
			Deep: &testproto.DeepMsg{Leaf: "hello"},
		},
	})

	runCases(t, []tc{
		{
			name:      "two-level nested leaf",
			data:      msg,
			paths:     []string{"nested\ndeep\nleaf"},
			wantPaths: []string{"nested\ndeep\nleaf"},
			wantVals:  []string{`"hello"`},
		},
	})
}

func TestMapInNestedMessage(t *testing.T) {
	msg := mustMarshal(&testproto.TestMsg{
		Nested: &testproto.NestedMsg{
			Attrs: map[string]string{"color": "blue"},
		},
	})

	runCases(t, []tc{
		{
			name:      "map inside nested message",
			data:      msg,
			paths:     []string{"nested\nattrs\ncolor"},
			wantPaths: []string{"nested\nattrs\ncolor"},
			wantVals:  []string{`"blue"`},
		},
	})
}

func TestRepeatedScalar(t *testing.T) {
	// tags: [10, 20, 30] — packed encoding (proto.Marshal always packs)
	packed := mustMarshal(&testproto.TestMsg{Tags: []int32{10, 20, 30}})

	runCases(t, []tc{
		{
			name:      "packed repeated int32 emits three fields",
			data:      packed,
			paths:     []string{"tags"},
			wantPaths: []string{"tags", "tags", "tags"},
			wantVals:  []string{"10", "20", "30"},
		},
	})

	// Also verify ArrayTrail is set correctly for packed fields.
	t.Run("packed ArrayTrail", func(t *testing.T) {
		fl := flattenpb.New(testDesc)
		tr := testtracker.New("tags")
		fields, err := fl.Flatten(packed, tr)
		if err != nil {
			t.Fatal(err)
		}
		if len(fields) != 3 {
			t.Fatalf("want 3 fields, got %d", len(fields))
		}
		aid := fields[0].ArrayTrail[0].Array
		for i, f := range fields {
			if len(f.ArrayTrail) != 1 {
				t.Errorf("[%d] ArrayTrail len %d, want 1", i, len(f.ArrayTrail))
			}
			if f.ArrayTrail[0].Array != aid {
				t.Errorf("[%d] Array ID mismatch", i)
			}
			if f.ArrayTrail[0].Pos != int32(i) {
				t.Errorf("[%d] Pos=%d want %d", i, f.ArrayTrail[0].Pos, i)
			}
		}
	})

	// Unpacked repeated fields (same field number appears multiple times).
	// proto.Marshal always packs, so we build this by hand.
	unpacked := func() []byte {
		var b []byte
		b = protowire.AppendTag(b, 9, protowire.VarintType)
		b = protowire.AppendVarint(b, 100)
		b = protowire.AppendTag(b, 9, protowire.VarintType)
		b = protowire.AppendVarint(b, 200)
		return b
	}()

	runCases(t, []tc{
		{
			name:      "unpacked repeated int32",
			data:      unpacked,
			paths:     []string{"tags"},
			wantPaths: []string{"tags", "tags"},
			wantVals:  []string{"100", "200"},
		},
	})
}

func TestRepeatedMessage(t *testing.T) {
	msg := mustMarshal(&testproto.TestMsg{
		Items: []*testproto.NestedMsg{
			{Value: "a", Count: 1},
			{Value: "b", Count: 2},
		},
	})

	runCases(t, []tc{
		{
			name:      "repeated message value field",
			data:      msg,
			paths:     []string{"items\nvalue"},
			wantPaths: []string{"items\nvalue", "items\nvalue"},
			wantVals:  []string{`"a"`, `"b"`},
		},
	})

	// ArrayTrail isolation: items[0].value and items[1].value must be in
	// different array positions so cross-element patterns don't match.
	t.Run("ArrayTrail cross-element isolation", func(t *testing.T) {
		fl := flattenpb.New(testDesc)
		tr := testtracker.New("items\nvalue", "items\ncount")
		fields, err := fl.Flatten(msg, tr)
		if err != nil {
			t.Fatal(err)
		}
		// Expect 4 fields: value/count for each of the two items.
		if len(fields) != 4 {
			t.Fatalf("want 4 fields, got %d", len(fields))
		}
		// Items from element 0 share Array ID and Pos=0.
		// Items from element 1 share the same Array ID but Pos=1.
		arrID := fields[0].ArrayTrail[0].Array
		for _, f := range fields {
			if f.ArrayTrail[0].Array != arrID {
				t.Errorf("array ID mismatch: %v", f.ArrayTrail)
			}
		}
		// First two fields come from item[0] (pos 0), next two from item[1] (pos 1).
		if fields[0].ArrayTrail[0].Pos != 0 || fields[1].ArrayTrail[0].Pos != 0 {
			t.Errorf("item[0] fields should have Pos=0, got %v %v", fields[0].ArrayTrail, fields[1].ArrayTrail)
		}
		if fields[2].ArrayTrail[0].Pos != 1 || fields[3].ArrayTrail[0].Pos != 1 {
			t.Errorf("item[1] fields should have Pos=1, got %v %v", fields[2].ArrayTrail, fields[3].ArrayTrail)
		}
	})
}

// TestRepeatedMsgInSingularNested verifies that a repeated message field
// inside a singular nested message gets correct ArrayTrail positions.
// NestedMsg.parts uses field number 10, same as TestMsg.items, so this also
// confirms that per-call fieldArrays scoping prevents ID collision.
func TestRepeatedMsgInSingularNested(t *testing.T) {
	msg := mustMarshal(&testproto.TestMsg{
		Nested: &testproto.NestedMsg{
			Parts: []*testproto.DeepMsg{
				{Leaf: "x"},
				{Leaf: "y"},
			},
		},
	})

	runCases(t, []tc{
		{
			name:      "repeated msg inside singular nested",
			data:      msg,
			paths:     []string{"nested\nparts\nleaf"},
			wantPaths: []string{"nested\nparts\nleaf", "nested\nparts\nleaf"},
			wantVals:  []string{`"x"`, `"y"`},
		},
	})

	t.Run("ArrayTrail positions", func(t *testing.T) {
		fl := flattenpb.New(testDesc)
		tr := testtracker.New("nested\nparts\nleaf")
		fields, err := fl.Flatten(msg, tr)
		if err != nil {
			t.Fatal(err)
		}
		if len(fields) != 2 {
			t.Fatalf("want 2 fields, got %d", len(fields))
		}
		// Each leaf is one level deep in a repeated field; trail length must be 1.
		for i, f := range fields {
			if len(f.ArrayTrail) != 1 {
				t.Errorf("[%d] ArrayTrail len %d, want 1", i, len(f.ArrayTrail))
			}
		}
		aid := fields[0].ArrayTrail[0].Array
		if fields[1].ArrayTrail[0].Array != aid {
			t.Errorf("array IDs differ across parts elements: %d vs %d", aid, fields[1].ArrayTrail[0].Array)
		}
		if fields[0].ArrayTrail[0].Pos != 0 {
			t.Errorf("parts[0] Pos=%d, want 0", fields[0].ArrayTrail[0].Pos)
		}
		if fields[1].ArrayTrail[0].Pos != 1 {
			t.Errorf("parts[1] Pos=%d, want 1", fields[1].ArrayTrail[0].Pos)
		}
	})
}

// TestDoublyNestedRepeated tests items[*].parts[*].leaf where both fields use
// field number 10 in their respective message types. The critical invariant is
// that items[0].parts and items[1].parts receive *different* inner array IDs,
// because each items[i] invocation creates a fresh fieldArrays scope.
func TestDoublyNestedRepeated(t *testing.T) {
	msg := mustMarshal(&testproto.TestMsg{
		Items: []*testproto.NestedMsg{
			{Parts: []*testproto.DeepMsg{{Leaf: "a"}, {Leaf: "b"}}},
			{Parts: []*testproto.DeepMsg{{Leaf: "c"}}},
		},
	})

	runCases(t, []tc{
		{
			name:  "doubly nested repeated leaf values",
			data:  msg,
			paths: []string{"items\nparts\nleaf"},
			wantPaths: []string{
				"items\nparts\nleaf",
				"items\nparts\nleaf",
				"items\nparts\nleaf",
			},
			wantVals: []string{`"a"`, `"b"`, `"c"`},
		},
	})

	t.Run("ArrayTrail structure", func(t *testing.T) {
		fl := flattenpb.New(testDesc)
		tr := testtracker.New("items\nparts\nleaf")
		fields, err := fl.Flatten(msg, tr)
		if err != nil {
			t.Fatal(err)
		}
		// 3 leaves total: items[0] contributes 2, items[1] contributes 1.
		if len(fields) != 3 {
			t.Fatalf("want 3 fields, got %d", len(fields))
		}
		for i, f := range fields {
			// Each leaf is two levels deep in repeated fields.
			if len(f.ArrayTrail) != 2 {
				t.Errorf("[%d] ArrayTrail len %d, want 2: %v", i, len(f.ArrayTrail), f.ArrayTrail)
			}
		}

		outerID := fields[0].ArrayTrail[0].Array
		innerID0 := fields[0].ArrayTrail[1].Array // items[0].parts array ID
		innerID1 := fields[2].ArrayTrail[1].Array // items[1].parts array ID

		// All three fields share the same outer (items) array ID.
		for i, f := range fields {
			if f.ArrayTrail[0].Array != outerID {
				t.Errorf("[%d] outer array ID %d, want %d", i, f.ArrayTrail[0].Array, outerID)
			}
		}

		// items[0].parts and items[1].parts must have distinct inner array IDs
		// because fieldArrays is scoped per flattenMsg call.
		if innerID0 == innerID1 {
			t.Errorf("items[0].parts and items[1].parts share array ID %d; expected independent IDs", innerID0)
		}

		// items[0] fields: outer Pos=0, inner Pos=0 and 1.
		if fields[0].ArrayTrail[0].Pos != 0 || fields[1].ArrayTrail[0].Pos != 0 {
			t.Errorf("items[0] leaves should have outer Pos=0: %v %v", fields[0].ArrayTrail, fields[1].ArrayTrail)
		}
		if fields[0].ArrayTrail[1].Pos != 0 {
			t.Errorf("items[0].parts[0] inner Pos=%d, want 0", fields[0].ArrayTrail[1].Pos)
		}
		if fields[1].ArrayTrail[1].Pos != 1 {
			t.Errorf("items[0].parts[1] inner Pos=%d, want 1", fields[1].ArrayTrail[1].Pos)
		}

		// items[1] field: outer Pos=1, inner Pos=0.
		if fields[2].ArrayTrail[0].Pos != 1 {
			t.Errorf("items[1] leaf outer Pos=%d, want 1", fields[2].ArrayTrail[0].Pos)
		}
		if fields[2].ArrayTrail[1].Pos != 0 {
			t.Errorf("items[1].parts[0] inner Pos=%d, want 0", fields[2].ArrayTrail[1].Pos)
		}
	})
}

func TestMapField(t *testing.T) {
	msg := mustMarshal(&testproto.TestMsg{
		Labels: map[string]string{"env": "prod", "region": "us-east"},
	})

	runCases(t, []tc{
		{
			name:      "map entry by key",
			data:      msg,
			paths:     []string{"labels\nenv"},
			wantPaths: []string{"labels\nenv"},
			wantVals:  []string{`"prod"`},
		},
		{
			name:      "two map keys",
			data:      msg,
			paths:     []string{"labels\nenv", "labels\nregion"},
			wantPaths: []string{"labels\nenv", "labels\nregion"},
			wantVals:  []string{`"prod"`, `"us-east"`},
		},
		{
			name:      "unregistered map key not emitted",
			data:      msg,
			paths:     []string{"labels\nother"},
			wantPaths: []string{},
			wantVals:  []string{},
		},
	})
}

func TestErrors(t *testing.T) {
	runCases(t, []tc{
		{
			name:    "truncated tag",
			data:    []byte{0xff, 0xff}, // invalid/truncated varint tag
			paths:   []string{"id"},
			wantErr: true,
		},
		{
			name:    "truncated varint value",
			data:    func() []byte { return protowire.AppendTag(nil, 1, protowire.VarintType) }(), // tag with no value
			paths:   []string{"id"},
			wantErr: true,
		},
	})
}

func TestCopy(t *testing.T) {
	orig := flattenpb.New(testDesc)
	cpy := orig.Copy()

	data := mustMarshal(&testproto.TestMsg{Id: 7})
	tr := testtracker.New("id")

	f1, err := orig.Flatten(data, tr)
	if err != nil {
		t.Fatal(err)
	}
	f2, err := cpy.Flatten(data, tr)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(f1[0].Val, f2[0].Val) {
		t.Errorf("Copy produced different result: %q vs %q", f1[0].Val, f2[0].Val)
	}
}

// TestCopyIndependence verifies that concurrent use of orig and copy does not
// corrupt either's output. It alternates Flatten calls with different messages
// so that any shared mutable state (fields, valBuf, arrayPosBuf) would cause
// one result to overwrite the other.
func TestCopyIndependence(t *testing.T) {
	orig := flattenpb.New(testDesc)
	cpy := orig.Copy()

	msgA := mustMarshal(&testproto.TestMsg{Id: 1, Name: "alice"})
	msgB := mustMarshal(&testproto.TestMsg{Id: 2, Name: "bob"})
	trA := testtracker.New("id", "name")
	trB := testtracker.New("id", "name")

	// Flatten with orig, then copy, then orig again — if buffers are shared the
	// second orig call would see stale or overwritten data.
	fa1, err := orig.Flatten(msgA, trA)
	if err != nil {
		t.Fatal(err)
	}
	// Save the values before the next call can clobber them (they alias valBuf).
	id1 := string(fa1[0].Val)
	name1 := string(fa1[1].Val)

	_, err = cpy.Flatten(msgB, trB)
	if err != nil {
		t.Fatal(err)
	}

	fa2, err := orig.Flatten(msgA, trA)
	if err != nil {
		t.Fatal(err)
	}

	if string(fa2[0].Val) != id1 {
		t.Errorf("orig id corrupted after copy use: got %q, want %q", fa2[0].Val, id1)
	}
	if string(fa2[1].Val) != name1 {
		t.Errorf("orig name corrupted after copy use: got %q, want %q", fa2[1].Val, name1)
	}
}

func TestWithFlattener(t *testing.T) {
	fl := flattenpb.New(testDesc)
	q, err := quamina.New(quamina.WithFlattener(fl))
	if err != nil {
		t.Fatal(err)
	}

	// Add a pattern matching id = 42
	if err := q.AddPattern("hit", `{"id": [42]}`); err != nil {
		t.Fatal(err)
	}
	if err := q.AddPattern("miss", `{"id": [99]}`); err != nil {
		t.Fatal(err)
	}

	event := mustMarshal(&testproto.TestMsg{Id: 42})
	matches, err := q.MatchesForEvent(event)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 || matches[0] != quamina.X("hit") {
		t.Errorf("expected [hit], got %v", matches)
	}
}
