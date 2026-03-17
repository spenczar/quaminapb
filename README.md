# quamina-protobuf

[![Go Reference](https://pkg.go.dev/badge/github.com/spenczar/quaminapb.svg)](https://pkg.go.dev/github.com/spenczar/quaminapb)

A [quamina](https://github.com/timbray/quamina) `Flattener` for binary-encoded Protocol Buffer messages.

Quamina matches patterns against events by first "flattening" the event into a list of `(path, value)` pairs. This library provides that flattening step for proto3 wire-format messages, so you can use quamina's fast pattern-matching engine directly against binary protobuf data.

## Usage

```go
import (
    "quamina.net/go/quamina/v2"
    "github.com/spenczar/quaminapb"
)

// Build a flattener from any generated message descriptor.
desc := (&mypb.MyMessage{}).ProtoReflect().Descriptor()
fl := quaminapb.New(desc)

// Wire it into quamina.
q, err := quamina.New(quamina.WithFlattener(fl))

// Add patterns using the same nested-object syntax as JSON quamina patterns.
// Array fields are transparent — just descend through them as if singular.
q.AddPattern("alert", `{"status": ["ERROR"], "region": ["us-east"]}`)
q.AddPattern("high-priority", `{"task": {"priority": [1]}}`)

// Match against wire-encoded messages.
matches, err := q.MatchesForEvent(wireBytes)
```

Field paths follow the proto field names. 

Repeated fields and map fields are supported; scalar values are rendered as their JSON equivalents (strings quoted, numbers bare, booleans `true`/`false`, enums as their string name).

`Flattener` is not safe for concurrent use. Call `Copy()` to get an independent copy that shares the read-only schema tables but has its own buffers — intended for use with quamina's built-in concurrency support.

## Implementation

Proto wire format encodes fields by number, not by name. `New` walks the descriptor at construction time and compiles a per-message-type dispatch table: for each `(field number, wire type)` pair it builds a pre-baked handler — a closure that contains everything needed to parse and emit that field with no further type inspection. This compilation is done once for the root message and all transitively reachable nested types; the descriptor is never consulted again after construction.

The hot loop in `Flatten` is therefore very thin: read the raw tag varint, look it up directly in the handler table (the table is keyed by the same encoded form, so no decoding is needed), check quamina's `SegmentsTreeTracker` to see if any active pattern cares about this field, and if so call the handler. If nothing matches — either no handler exists for that tag, or no pattern references the field — the bytes are skipped entirely without parsing the value.

Internal buffers (`fields`, `valBuf`, `arrayPosBuf`) are reset and reused across `Flatten` calls, giving zero allocations in steady state. `Copy()` creates a new `Flattener` with fresh buffers but shares the immutable handler tables with the original.

## Performance

Zero allocations in steady state, just like quamina's built-in JSON flattener. Proto binary is at least as fast as JSON for equivalent events, and substantially faster for large events where most fields are irrelevant to the active patterns. See [BENCHMARKS.md](BENCHMARKS.md) for numbers.

## Development

Test proto definitions live in `internal/testproto/`. After editing `testproto.proto`, regenerate the Go bindings with:

```
buf generate
```

This uses the remote `buf.build/protocolbuffers/go` plugin (no local `protoc` install needed) and writes the output back into `internal/testproto/`. Commit the generated `.pb.go` alongside the `.proto`.

End-to-end integration tests live in `internal/integrationtest/` and exercise the full pipeline from proto message through flattening to quamina pattern matching.
