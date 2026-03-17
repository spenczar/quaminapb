package quaminapb_test

import (
	"testing"

	quamina "quamina.net/go/quamina/v2"

	quaminapb "github.com/spenczar/quaminapb"
	"github.com/spenczar/quaminapb/internal/testproto"
	"github.com/spenczar/quaminapb/internal/testtracker"
)

// benchEvent is a wire-encoded TestMsg with a mix of scalar, nested, and
// repeated fields — representative of a moderately complex real message.
var benchEvent []byte

// benchEventMiss is identical to benchEvent except the name field is different,
// so patterns that match benchEvent by name will not match this one.
var benchEventMiss []byte

func init() {
	msg := &testproto.TestMsg{
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
	}
	benchEvent = mustMarshal(msg)

	benchEventMiss = mustMarshal(&testproto.TestMsg{
		Id:     12345,
		Name:   "no-match-user",
		Flag:   true,
		Score:  9.81,
		Ratio:  3.141592653589793,
		Data:   []byte("binarydata"),
		Status: testproto.TestStatus_PENDING,
		Nested: &testproto.NestedMsg{Value: "other-value", Count: 7},
		Tags:   []int32{1, 2, 3, 4, 5},
		Items: []*testproto.NestedMsg{
			{Value: "item-a", Count: 10},
			{Value: "item-b", Count: 20},
		},
		Labels: map[string]string{"env": "staging"},
	})
}

// Benchmark_ProtoFlattener_FewFields benchmarks a tracker that only cares
// about one or two leaf paths — the common case in a selective pattern.
func Benchmark_ProtoFlattener_FewFields(b *testing.B) {
	fl := quaminapb.New(testDesc)
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
	fl := quaminapb.New(testDesc)
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

// newBenchQuamina constructs a quamina.Quamina instance with the given patterns
// pre-loaded.  The helper is intentionally outside b.N so setup cost is excluded.
func newBenchQuamina(b *testing.B, patterns map[string]string) *quamina.Quamina {
	b.Helper()
	fl := quaminapb.New(testDesc)
	q, err := quamina.New(quamina.WithFlattener(fl))
	if err != nil {
		b.Fatal(err)
	}
	for name, pat := range patterns {
		if err := q.AddPattern(quamina.X(name), pat); err != nil {
			b.Fatalf("AddPattern(%q): %v", name, err)
		}
	}
	return q
}

// Benchmark_QuaminaMatch_FewFields_Hit measures the full match pipeline when
// the event matches all patterns.
func Benchmark_QuaminaMatch_FewFields_Hit(b *testing.B) {
	q := newBenchQuamina(b, map[string]string{
		"name-match": `{"name": ["benchmark-user"]}`,
		"id-match":   `{"id": [12345]}`,
	})

	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(benchEvent)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

// Benchmark_QuaminaMatch_FewFields_Miss measures the full match pipeline when
// the event matches none of the patterns.
func Benchmark_QuaminaMatch_FewFields_Miss(b *testing.B) {
	q := newBenchQuamina(b, map[string]string{
		"name-match": `{"name": ["benchmark-user"]}`,
		"id-match":   `{"id": [12345]}`,
	})

	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(benchEventMiss)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

// Benchmark_QuaminaMatch_ManyFields_Hit measures the full match pipeline with
// a rich pattern set when the event matches.
func Benchmark_QuaminaMatch_ManyFields_Hit(b *testing.B) {
	q := newBenchQuamina(b, map[string]string{
		"name-match":   `{"name": ["benchmark-user"]}`,
		"id-match":     `{"id": [12345]}`,
		"status-match": `{"status": ["ACTIVE"]}`,
		"nested-match": `{"nested": {"value": ["child-value"]}}`,
		"label-match":  `{"labels": {"env": ["prod"]}}`,
	})

	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(benchEvent)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}

// Benchmark_QuaminaMatch_ManyFields_Miss measures the full match pipeline with
// a rich pattern set when the event matches none of the patterns.
func Benchmark_QuaminaMatch_ManyFields_Miss(b *testing.B) {
	q := newBenchQuamina(b, map[string]string{
		"name-match":   `{"name": ["benchmark-user"]}`,
		"id-match":     `{"id": [12345]}`,
		"status-match": `{"status": ["ACTIVE"]}`,
		"nested-match": `{"nested": {"value": ["child-value"]}}`,
		"label-match":  `{"labels": {"env": ["prod"]}}`,
	})

	b.ReportAllocs()
	b.ResetTimer()
	var sink []quamina.X
	for i := 0; i < b.N; i++ {
		matches, err := q.MatchesForEvent(benchEventMiss)
		if err != nil {
			b.Fatal(err)
		}
		sink = matches
	}
	_ = sink
}
