package restate

import (
	"github.com/restatedev/sdk-go/encoding"
	"github.com/restatedev/sdk-go/internal/options"
)

// re-export for use in generated code
type CallOption = options.CallOption
type ServiceOption = options.ServiceOption
type ObjectOption = options.ObjectOption

type withCodec struct {
	codec encoding.Codec
}

var _ options.GetOption = withCodec{}
var _ options.SetOption = withCodec{}
var _ options.RunOption = withCodec{}
var _ options.AwakeableOption = withCodec{}
var _ options.ResolveAwakeableOption = withCodec{}
var _ options.CallOption = withCodec{}

func (w withCodec) BeforeGet(opts *options.GetOptions)             { opts.Codec = w.codec }
func (w withCodec) BeforeSet(opts *options.SetOptions)             { opts.Codec = w.codec }
func (w withCodec) BeforeRun(opts *options.RunOptions)             { opts.Codec = w.codec }
func (w withCodec) BeforeAwakeable(opts *options.AwakeableOptions) { opts.Codec = w.codec }
func (w withCodec) BeforeResolveAwakeable(opts *options.ResolveAwakeableOptions) {
	opts.Codec = w.codec
}
func (w withCodec) BeforeCall(opts *options.CallOptions) { opts.Codec = w.codec }

// WithCodec is an option that can be provided to many different functions that perform (de)serialisation
// in order to specify a custom codec with which to (de)serialise instead of the default of JSON.
//
// See also [WithProto], [WithBinary], [WithJSON].
func WithCodec(codec encoding.Codec) withCodec {
	return withCodec{codec}
}

type withPayloadCodec struct {
	withCodec
	codec encoding.PayloadCodec
}

var _ options.ServiceHandlerOption = withPayloadCodec{}
var _ options.ServiceOption = withPayloadCodec{}
var _ options.ObjectHandlerOption = withPayloadCodec{}
var _ options.ObjectOption = withPayloadCodec{}

func (w withPayloadCodec) BeforeServiceHandler(opts *options.ServiceHandlerOptions) {
	opts.Codec = w.codec
}
func (w withPayloadCodec) BeforeObjectHandler(opts *options.ObjectHandlerOptions) {
	opts.Codec = w.codec
}
func (w withPayloadCodec) BeforeService(opts *options.ServiceOptions) {
	opts.DefaultCodec = w.codec
}
func (w withPayloadCodec) BeforeObject(opts *options.ObjectOptions) {
	opts.DefaultCodec = w.codec
}

// WithPayloadCodec is an option that can be provided to handler/service options
// in order to specify a custom [encoding.PayloadCodec] with which to (de)serialise and
// set content-types instead of the default of JSON.
//
// See also [WithProto], [WithBinary], [WithJSON].
func WithPayloadCodec(codec encoding.PayloadCodec) withPayloadCodec {
	return withPayloadCodec{withCodec{codec}, codec}
}

// WithProto is an option to specify the use of [encoding.ProtoCodec] for (de)serialisation
var WithProto = WithPayloadCodec(encoding.ProtoCodec)

// WithProtoJSON is an option to specify the use of [encoding.ProtoJSONCodec] for (de)serialisation
var WithProtoJSON = WithPayloadCodec(encoding.ProtoJSONCodec)

// WithBinary is an option to specify the use of [encoding.BinaryCodec] for (de)serialisation
var WithBinary = WithPayloadCodec(encoding.BinaryCodec)

// WithJSON is an option to specify the use of [encoding.JsonCodec] for (de)serialisation
var WithJSON = WithPayloadCodec(encoding.JSONCodec)

type withHeaders struct {
	headers map[string]string
}

var _ options.CallOption = withHeaders{}

func (w withHeaders) BeforeCall(opts *options.CallOptions) {
	opts.Headers = w.headers
}

// WithHeaders is an option to specify outgoing headers when making a call
func WithHeaders(headers map[string]string) withHeaders {
	return withHeaders{headers}
}
