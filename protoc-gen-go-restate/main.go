// protoc-gen-go-restate is a plugin for the Google protocol buffer compiler to
// generate Restate servers and clients. Install it by building this program and
// making it accessible within your PATH with the name:
//
//	protoc-gen-go-restate
//
// The 'go-restate' suffix becomes part of the argument for the protocol compiler,
// such that it can be invoked as:
//
//	protoc --go-restate_out=. path/to/file.proto
//
// This generates Restate service definitions for the protocol buffer defined by
// file.proto.  With that input, the output will be written to:
//
//	path/to/file_restate.pb.go
//
// Lots of code copied from protoc-gen-go-grpc:
// https://github.com/grpc/grpc-go/tree/master/cmd/protoc-gen-go-grpc
// ! License Apache-2.0
package main

import (
	"flag"
	"fmt"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"
)

var version = "0.1"

var requireUnimplemented *bool
var useGenericStreams *bool

func main() {
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()
	if *showVersion {
		fmt.Printf("protoc-gen-go-grpc %v\n", version)
		return
	}

	var flags flag.FlagSet
	requireUnimplemented = flags.Bool("require_unimplemented_servers", false, "set to true to disallow servers that have unimplemented fields")

	protogen.Options{
		ParamFunc: flags.Set,
	}.Run(func(gen *protogen.Plugin) error {
		gen.SupportedFeatures = uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL)
		for _, f := range gen.Files {
			if !f.Generate {
				continue
			}
			generateFile(gen, f)
		}
		return nil
	})
}
