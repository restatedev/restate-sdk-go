# protoc-gen-go-grpc

This tool generates Go language bindings of `service`s in protobuf definition
files for Restate.

An example of their use can be found in [examples/codegen](../examples/codegen)

## Usage
Via protoc:
```shell
go install github.com/restatedev/sdk-go/protoc-gen-go-restate@latest
protoc --go_out=. --go_opt=paths=source_relative \
--go-restate_out=. --go-restate_opt=paths=source_relative service.proto
```

Via [buf](https://buf.build/):
```yaml
# buf.gen.yaml
plugins:
  - remote: buf.build/protocolbuffers/go:v1.34.2
    out: .
    opt: paths=source_relative
  - local:
      - docker
      - run
      - --pull=always
      - -i
      - ghcr.io/restatedev/protoc-gen-go-restate:latest
    # alternatively if you prefer to install the binary:
    # local: protoc-gen-go-restate
    out: .
    opt: paths=source_relative
```

# Providing options
This protoc plugin supports the service and method extensions defined in
[proto/dev/restate/sdk/go.proto](../proto/dev/restate/sdk/go.proto).
You will need to use these extensions to define virtual objects in proto.

You can import the extensions with the statement `import "dev/restate/sdk/go.proto";`. Protoc will expect an equivalent directory
structure containing the go.proto file either locally, or under any of the
paths provided with `--proto_path`. It may be easier to use
[buf](https://buf.build/docs/bsr/module/dependency-management) to import:
```yaml
# buf.yaml
version: v2
deps:
  - buf.build/restatedev/sdk-go
```

# Building a docker image
```
KO_DOCKER_REPO=ghcr.io/restatedev ko build --platform=all -B
```
