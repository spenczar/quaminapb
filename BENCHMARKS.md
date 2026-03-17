# Benchmarks

All benchmarks run on an AMD Ryzen 5 7600X, `-benchtime=3s -count=3`.

## Full-pipeline (proto only)

Against a moderately complex message with a mix of scalar, nested, and repeated fields (~15 fields total):

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| FewFields Hit (2 patterns, event matches) | ~505 | 0 | 0 |
| FewFields Miss (2 patterns, no match) | ~476 | 0 | 0 |
| ManyFields Hit (5 patterns, event matches) | ~1055 | 0 | 0 |
| ManyFields Miss (5 patterns, no match) | ~895 | 0 | 0 |

The `FewFields` case reflects typical quamina usage where patterns only care about a small subset of fields — the flattener skips unused paths entirely. Zero allocations in steady state because quamina and the flattener reuse their internal buffers.

## JSON vs proto comparison

Side-by-side comparison of identical logical data encoded as JSON vs proto3 binary, run through quamina's full match pipeline. "Hit" means the event matches all patterns; "Miss" means it matches none.

**CityLot** (~600-byte GeoJSON feature, 1 pattern matching a string property):

| Encoding | Hit ns/op | Miss ns/op | Event size |
|---|---|---|---|
| JSON | ~341 | ~286 | ~600 bytes |
| Proto | ~330 | ~274 | ~150 bytes |

**Status** (9.4 KB Twitter-like event, 3 patterns; miss event is a minimal ~100-byte stub with non-matching values):

| Encoding | Hit ns/op | Miss ns/op | Event size |
|---|---|---|---|
| JSON | ~6193 | ~387 | 9.4 KB |
| Proto | ~1130 | ~326 | ~350 bytes |

The Status hit benchmark shows the largest difference: proto binary is ~5.5× faster than JSON for a 9.4 KB event because the proto flattener skips irrelevant bytes in the wire encoding without parsing them, while the JSON flattener must scan every character. Miss times converge because quamina can bail out early once a pattern fails, long before the full event is consumed.

Run benchmarks yourself:

```
go test ./... -bench=. -benchmem
```
