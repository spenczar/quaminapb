// Package integrationtest runs end-to-end tests against a real quamina
// instance configured with a ProtoFlattener. Unit tests in flattenpb/ verify
// what fields are emitted; these tests verify that the full match pipeline
// produces the right hits (and non-hits) for realistic patterns and messages.
package integrationtest

import (
	"testing"

	quamina "quamina.net/go/quamina/v2"

	"github.com/spenczar/quamina-protobuf/flattenpb"
	"github.com/spenczar/quamina-protobuf/internal/testproto"
	"google.golang.org/protobuf/proto"
)

func mustMarshal(m proto.Message) []byte {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(m)
	if err != nil {
		panic("mustMarshal: " + err.Error())
	}
	return b
}

func newQuamina(t *testing.T) *quamina.Quamina {
	t.Helper()
	desc := (&testproto.TestMsg{}).ProtoReflect().Descriptor()
	fl := flattenpb.New(desc)
	q, err := quamina.New(quamina.WithFlattener(fl))
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func addPattern(t *testing.T, q *quamina.Quamina, name, pattern string) {
	t.Helper()
	if err := q.AddPattern(quamina.X(name), pattern); err != nil {
		t.Fatalf("AddPattern(%q, %q): %v", name, pattern, err)
	}
}

func assertMatches(t *testing.T, q *quamina.Quamina, event []byte, want ...string) {
	t.Helper()
	got, err := q.MatchesForEvent(event)
	if err != nil {
		t.Fatalf("MatchesForEvent: %v", err)
	}
	wantSet := make(map[quamina.X]bool, len(want))
	for _, w := range want {
		wantSet[quamina.X(w)] = true
	}
	gotSet := make(map[quamina.X]bool, len(got))
	for _, g := range got {
		gotSet[g] = true
	}
	for w := range wantSet {
		if !gotSet[w] {
			t.Errorf("expected match %q not found in %v", w, got)
		}
	}
	for g := range gotSet {
		if !wantSet[g] {
			t.Errorf("unexpected match %q in results", g)
		}
	}
}

// TestScalarMatching covers basic scalar field patterns.
func TestScalarMatching(t *testing.T) {
	q := newQuamina(t)
	addPattern(t, q, "id-match", `{"id": [42]}`)
	addPattern(t, q, "name-match", `{"name": ["alice"]}`)
	addPattern(t, q, "flag-match", `{"flag": [true]}`)
	addPattern(t, q, "status-active", `{"status": ["ACTIVE"]}`)

	t.Run("id hit", func(t *testing.T) {
		assertMatches(t, q, mustMarshal(&testproto.TestMsg{Id: 42}), "id-match")
	})
	t.Run("id miss", func(t *testing.T) {
		assertMatches(t, q, mustMarshal(&testproto.TestMsg{Id: 99}))
	})
	t.Run("name hit", func(t *testing.T) {
		assertMatches(t, q, mustMarshal(&testproto.TestMsg{Name: "alice"}), "name-match")
	})
	t.Run("flag hit", func(t *testing.T) {
		assertMatches(t, q, mustMarshal(&testproto.TestMsg{Flag: true}), "flag-match")
	})
	t.Run("enum hit", func(t *testing.T) {
		assertMatches(t, q, mustMarshal(&testproto.TestMsg{Status: testproto.TestStatus_ACTIVE}), "status-active")
	})
	t.Run("enum miss", func(t *testing.T) {
		assertMatches(t, q, mustMarshal(&testproto.TestMsg{Status: testproto.TestStatus_PENDING}))
	})
	t.Run("multiple fields both matching", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{Id: 42, Name: "alice"}),
			"id-match", "name-match",
		)
	})
}

// TestNestedMessageMatching covers patterns that descend into a singular nested message.
func TestNestedMessageMatching(t *testing.T) {
	q := newQuamina(t)
	addPattern(t, q, "nested-value", `{"nested": {"value": ["child"]}}`)
	addPattern(t, q, "nested-deep", `{"nested": {"deep": {"leaf": ["hello"]}}}`)

	t.Run("nested value hit", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Nested: &testproto.NestedMsg{Value: "child"},
			}),
			"nested-value",
		)
	})
	t.Run("nested value miss", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Nested: &testproto.NestedMsg{Value: "other"},
			}),
		)
	})
	t.Run("two-level nested hit", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Nested: &testproto.NestedMsg{
					Deep: &testproto.DeepMsg{Leaf: "hello"},
				},
			}),
			"nested-deep",
		)
	})
}

// TestMapMatching covers patterns that match by map key.
func TestMapMatching(t *testing.T) {
	q := newQuamina(t)
	addPattern(t, q, "env-prod", `{"labels": {"env": ["prod"]}}`)

	t.Run("map key hit", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Labels: map[string]string{"env": "prod"},
			}),
			"env-prod",
		)
	})
	t.Run("map key miss — wrong value", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Labels: map[string]string{"env": "staging"},
			}),
		)
	})
	t.Run("map key miss — different key", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Labels: map[string]string{"region": "prod"},
			}),
		)
	})
}

// TestRepeatedScalarMatching covers patterns against packed repeated scalars.
func TestRepeatedScalarMatching(t *testing.T) {
	q := newQuamina(t)
	addPattern(t, q, "has-3", `{"tags": [3]}`)

	t.Run("tag present", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{Tags: []int32{1, 2, 3}}),
			"has-3",
		)
	})
	t.Run("tag absent", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{Tags: []int32{1, 2, 4}}),
		)
	})
}

// TestRepeatedMessageMatching covers patterns against repeated message fields.
// The key invariant: a pattern that constrains two fields must match both
// conditions within the same element, not across elements.
func TestRepeatedMessageMatching(t *testing.T) {
	q := newQuamina(t)
	addPattern(t, q, "any-value-a", `{"items": {"value": ["a"]}}`)
	addPattern(t, q, "value-a-count-1", `{"items": {"value": ["a"], "count": [1]}}`)

	t.Run("single field match", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Items: []*testproto.NestedMsg{
					{Value: "a", Count: 10},
				},
			}),
			"any-value-a",
		)
	})

	t.Run("both fields match same element", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Items: []*testproto.NestedMsg{
					{Value: "a", Count: 1},
				},
			}),
			"any-value-a", "value-a-count-1",
		)
	})

	// This is the cross-element isolation check: value="a" is in element 0,
	// count=1 is in element 1. The two-field pattern must NOT match.
	t.Run("fields split across elements do not match two-field pattern", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Items: []*testproto.NestedMsg{
					{Value: "a", Count: 99},
					{Value: "z", Count: 1},
				},
			}),
			"any-value-a", // single-field pattern still matches element 0
			// "value-a-count-1" must NOT appear
		)
	})
}

// TestDoublyNestedRepeatedMatching covers patterns into items[*].parts[*].leaf,
// where both items and parts use field number 10 in their respective messages.
func TestDoublyNestedRepeatedMatching(t *testing.T) {
	q := newQuamina(t)
	addPattern(t, q, "has-leaf-c", `{"items": {"parts": {"leaf": ["c"]}}}`)

	t.Run("leaf present in second item", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Items: []*testproto.NestedMsg{
					{Parts: []*testproto.DeepMsg{{Leaf: "a"}, {Leaf: "b"}}},
					{Parts: []*testproto.DeepMsg{{Leaf: "c"}}},
				},
			}),
			"has-leaf-c",
		)
	})
	t.Run("leaf absent", func(t *testing.T) {
		assertMatches(t, q,
			mustMarshal(&testproto.TestMsg{
				Items: []*testproto.NestedMsg{
					{Parts: []*testproto.DeepMsg{{Leaf: "a"}}},
				},
			}),
		)
	})
}
