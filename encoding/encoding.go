package encoding

import (
	"encoding/json"
	"fmt"
	"reflect"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Void is a placeholder to signify 'no value' where a type is otherwise needed
// It implements [RestateMarshaler] and [RestateUnmarshaler] to ensure that no marshaling or unmarshaling ever happens
// on this type.
type Void struct{}

var (
	// BinaryCodec marshals []byte and unmarshals into *[]byte
	// In handlers, it uses a content type of application/octet-stream
	BinaryCodec PayloadCodec = binaryCodec{}
	// ProtoCodec marshals proto.Message and unmarshals into proto.Message or pointers to types that implement proto.Message
	// In handlers, it uses a content-type of application/proto
	ProtoCodec PayloadCodec = protoCodec{}
	// ProtoJSONCodec marshals proto.Message and unmarshals into proto.Message or pointers to types that implement proto.Message
	// It uses the protojson package to marshal and unmarshal
	// In handlers, it uses a content-type of application/json
	ProtoJSONCodec PayloadCodec = protoJSONCodec{}
	// JSONCodec marshals any json.Marshallable type and unmarshals into any json.Unmarshallable type
	// In handlers, it uses a content-type of application/json
	JSONCodec PayloadCodec = jsonCodec{}

	_ RestateMarshaler   = Void{}
	_ RestateUnmarshaler = Void{}
	_ RestateUnmarshaler = &Void{}
)

func (v Void) RestateUnmarshal(codec Codec, data []byte) error {
	return nil
}

func (v Void) InputPayload(codec Codec) *InputPayload {
	return &InputPayload{}
}

func (v Void) RestateMarshal(codec Codec) ([]byte, error) {
	return nil, nil
}

func (v Void) OutputPayload(codec Codec) *OutputPayload {
	return &OutputPayload{}
}

// RestateUnmarshaler can be implemented by types that want to control their own unmarshaling
type RestateUnmarshaler interface {
	RestateUnmarshal(codec Codec, data []byte) error
	InputPayload(codec Codec) *InputPayload
}

// InputPayloadFor determines the InputPayload for the type stored in i, respecting [RestateUnmarshaler] implementors
func InputPayloadFor(codec PayloadCodec, i any) *InputPayload {
	ru, ok := i.(RestateUnmarshaler)
	if ok {
		return ru.InputPayload(codec)
	}
	return codec.InputPayload(i)
}

// OutputPayloadFor determines the OutputPayload for the type stored in o, respecting [RestateMarshaler] implementors
func OutputPayloadFor(codec PayloadCodec, o any) *OutputPayload {
	ru, ok := o.(RestateMarshaler)
	if ok {
		return ru.OutputPayload(codec)
	}
	return codec.OutputPayload(o)
}

// RestateMarshaler can be implemented by types that want to control their own marshaling
type RestateMarshaler interface {
	RestateMarshal(codec Codec) ([]byte, error)
	OutputPayload(codec Codec) *OutputPayload
}

// Codec is a mechanism for serialising and deserialising a wide range of types.
// Care should be taken to ensure that only valid types are passed to a codec, eg proto.Message for [ProtoCodec].
type Codec interface {
	Marshal(v any) ([]byte, error)
	Unmarshal(data []byte, v any) error
}

// Marshal converts its input v into []byte using the codec, respecting [RestateMarshaler] implementors
func Marshal(codec Codec, v any) ([]byte, error) {
	if marshaler, ok := v.(RestateMarshaler); ok {
		return marshaler.RestateMarshal(codec)
	}
	return codec.Marshal(v)
}

// Unmarshal converts its input data and stores the result into v using the codec, respecting [RestateUnmarshaler] implementors
// v is expected to be a mutable concrete type, generally a pointer.
func Unmarshal(codec Codec, data []byte, v any) error {
	if marshaler, ok := v.(RestateUnmarshaler); ok {
		return marshaler.RestateUnmarshal(codec, data)
	}
	return codec.Unmarshal(data, v)
}

// PayloadCodec is implemented by a [Codec] that can also be used in handlers, and so must provide a [InputPayload] and [OutputPayload]
// i and o are zero values of the input/output types, which the codec may use to influence its response.
type PayloadCodec interface {
	InputPayload(i any) *InputPayload
	OutputPayload(o any) *OutputPayload
	Codec
}

// InputPayload is provided to Restate upon handler discovery, to teach the ingress how to validate incoming
// request bodies.
type InputPayload struct {
	Required    bool        `json:"required"`
	ContentType *string     `json:"contentType,omitempty"`
	JsonSchema  interface{} `json:"jsonSchema,omitempty"`
}

// OutputPayload is provided to Restate upon handler discovery, to teach the ingress how to annotate outgoing
// response bodies.
type OutputPayload struct {
	ContentType           *string     `json:"contentType,omitempty"`
	SetContentTypeIfEmpty bool        `json:"setContentTypeIfEmpty"`
	JsonSchema            interface{} `json:"jsonSchema,omitempty"`
}

type binaryCodec struct{}

func (j binaryCodec) InputPayload(_ any) *InputPayload {
	// Required false because 0 bytes is a valid input
	return &InputPayload{Required: false, ContentType: proto.String("application/octet-stream")}
}

func (j binaryCodec) OutputPayload(_ any) *OutputPayload {
	// SetContentTypeIfEmpty true because 0 bytes is a valid output
	return &OutputPayload{ContentType: proto.String("application/octet-stream"), SetContentTypeIfEmpty: true}
}

func (j binaryCodec) Unmarshal(data []byte, input any) (err error) {
	switch input := input.(type) {
	case *[]byte:
		*input = data
		return nil
	default:
		return fmt.Errorf("BinaryCodec.Unmarshal called with a type that is not *[]byte")
	}
}

func (j binaryCodec) Marshal(output any) ([]byte, error) {
	switch output := output.(type) {
	case []byte:
		return output, nil
	default:
		return nil, fmt.Errorf("BinaryCodec.Marshal called with a type that is not []byte")
	}
}

type jsonCodec struct{}

func (j jsonCodec) InputPayload(_ any) *InputPayload {
	return &InputPayload{Required: true, ContentType: proto.String("application/json")}
}

func (j jsonCodec) OutputPayload(_ any) *OutputPayload {
	return &OutputPayload{ContentType: proto.String("application/json")}
}

func (j jsonCodec) Unmarshal(data []byte, input any) (err error) {
	return json.Unmarshal(data, input)
}

func (j jsonCodec) Marshal(output any) ([]byte, error) {
	return json.Marshal(output)
}

type protoCodec struct{}

func (p protoCodec) InputPayload(_ any) *InputPayload {
	return &InputPayload{Required: true, ContentType: proto.String("application/proto")}
}

func (p protoCodec) OutputPayload(_ any) *OutputPayload {
	return &OutputPayload{ContentType: proto.String("application/proto")}
}

func (p protoCodec) Unmarshal(data []byte, input any) (err error) {
	switch input := input.(type) {
	case proto.Message:
		// called with a *Message
		return proto.Unmarshal(data, input)
	default:
		msg, err := allocateProtoMessage("ProtoCodec", input)
		if err != nil {
			return err
		}
		return proto.Unmarshal(data, msg)
	}
}

func (p protoCodec) Marshal(output any) (data []byte, err error) {
	switch output := output.(type) {
	case proto.Message:
		return proto.Marshal(output)
	default:
		return nil, fmt.Errorf("ProtoCodec.Marshal called with a type that is not a proto.Message")
	}
}

type protoJSONCodec struct{}

func (j protoJSONCodec) InputPayload(_ any) *InputPayload {
	return &InputPayload{Required: true, ContentType: proto.String("application/json")}
}

func (j protoJSONCodec) OutputPayload(_ any) *OutputPayload {
	return &OutputPayload{ContentType: proto.String("application/json")}
}

func (j protoJSONCodec) Unmarshal(data []byte, input any) (err error) {
	switch input := input.(type) {
	case proto.Message:
		// called with a *Message
		return protojson.Unmarshal(data, input)
	default:
		msg, err := allocateProtoMessage("ProtoJSONCodec", input)
		if err != nil {
			return err
		}
		return protojson.Unmarshal(data, msg)
	}
}

func (j protoJSONCodec) Marshal(output any) ([]byte, error) {
	switch output := output.(type) {
	case proto.Message:
		return protojson.Marshal(output)
	default:
		return nil, fmt.Errorf("ProtoJSONCodec.Marshal called with a type that is not a proto.Message")
	}
}

// we must support being called with a **Message where *Message is nil because this is the result of new(I) where I is a proto.Message
// new(I) is really the only generic approach for allocating. Hitting this code path is meaningfully slower
// for protobuf decoding, but the effect is minimal for protojson
func allocateProtoMessage(codecName string, input any) (proto.Message, error) {
	value := reflect.ValueOf(input)
	if value.Kind() != reflect.Pointer || value.IsNil() || value.Elem().Kind() != reflect.Pointer {
		return nil, fmt.Errorf("%s.Unmarshal called with neither a proto.Message nor a non-nil pointer to a type that implements proto.Message.", codecName)
	}
	elem := value.Elem() // hopefully a *Message
	if elem.IsNil() {
		// allocate a &Message and swap this in
		elem.Set(reflect.New(elem.Type().Elem()))
	}
	switch elemI := elem.Interface().(type) {
	case proto.Message:
		return elemI, nil
	default:
		return nil, fmt.Errorf("%s.Unmarshal called with neither a proto.Message nor a non-nil pointer to a type that implements proto.Message.", codecName)
	}
}
