package flattenpb_test

import (
	"testing"

	"github.com/spenczar/quamina-protobuf/flattenpb"
	"github.com/spenczar/quamina-protobuf/internal/testproto"
	"github.com/spenczar/quamina-protobuf/internal/testtracker"
)

// benchEvent is a wire-encoded TestMsg with a mix of scalar, nested, and
// repeated fields — representative of a moderately complex real message.
var benchEvent []byte

func init() {
	benchEvent = mustMarshal(&testproto.TestMsg{
		Id:     12345,
		Name:   "benchmark-user",
		Flag:   true,
		Score:  9.81,
		Ratio:  3.141592653589793,
		Data:   []byte("binarydata"),
		Status: testproto.TestStatus_ACTIVE,
		Nested: &testproto.NestedMsg{Value: "child-value", Count: 7},
		Tags:   []int32{1, 2, 3, 4, 5},
		Items: []*testproto.NestedMsg{
			{Value: "item-a", Count: 10},
			{Value: "item-b", Count: 20},
		},
		Labels: map[string]string{"env": "prod"},
	})
}

// Benchmark_ProtoFlattener_FewFields benchmarks a tracker that only cares
// about one or two leaf paths — the common case in a selective pattern.
func Benchmark_ProtoFlattener_FewFields(b *testing.B) {
	fl := flattenpb.New(testDesc)
	tr := testtracker.New("id", "nested\nvalue")

	b.ReportAllocs()
	b.ResetTimer()
	var sink any
	for i := 0; i < b.N; i++ {
		fields, err := fl.Flatten(benchEvent, tr)
		if err != nil {
			b.Fatal(err)
		}
		sink = fields
	}
	_ = sink
}

// Benchmark_ProtoFlattener_ManyFields benchmarks a tracker that cares about
// all paths — the worst case for the pruning optimisation.
func Benchmark_ProtoFlattener_ManyFields(b *testing.B) {
	fl := flattenpb.New(testDesc)
	tr := testtracker.New(
		"id", "name", "flag", "score", "ratio", "data", "status",
		"nested\nvalue", "nested\ncount",
		"tags",
		"items\nvalue", "items\ncount",
		"labels\nenv",
	)

	b.ReportAllocs()
	b.ResetTimer()
	var sink any
	for i := 0; i < b.N; i++ {
		fields, err := fl.Flatten(benchEvent, tr)
		if err != nil {
			b.Fatal(err)
		}
		sink = fields
	}
	_ = sink
}
