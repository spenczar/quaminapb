<!-- TODO -->

### Development

Test proto definitions live in `internal/testproto/`. After editing `testproto.proto`, regenerate the Go bindings with:

```
buf generate
```

This uses the remote `buf.build/protocolbuffers/go` plugin (no local `protoc` install needed) and writes the output back into `internal/testproto/`. Commit the generated `.pb.go` alongside the `.proto`.
