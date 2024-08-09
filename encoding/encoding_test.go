package encoding

import (
	"encoding/base64"
	"testing"

	protocol "github.com/restatedev/sdk-go/generated/dev/restate/service"
)

func willPanic(t *testing.T, do func()) {
	defer func() {
		switch recover() {
		case nil:
			t.Fatalf("expected panic but didn't find one")
		default:
			return
		}
	}()

	do()
}

func willSucceed(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func checkMessage(t *testing.T, msg *protocol.AwakeableEntryMessage) {
	if msg.Name != "foobar" {
		t.Fatalf("unexpected msg.Name: %s", msg.Name)
	}
}

func TestProto(t *testing.T) {
	p := ProtoCodec

	_, err := Marshal(p, protocol.AwakeableEntryMessage{Name: "foobar"})
	if err == nil {
		t.Fatalf("expected error when marshaling non-pointer proto Message")
	}

	bytes, err := Marshal(p, &protocol.AwakeableEntryMessage{Name: "foobar"})
	if err != nil {
		t.Fatal(err)
	}

	{
		msg := &protocol.AwakeableEntryMessage{}
		willSucceed(t, Unmarshal(p, bytes, msg))
		checkMessage(t, msg)
	}

	{
		inner := &protocol.AwakeableEntryMessage{}
		msg := &inner
		willSucceed(t, Unmarshal(p, bytes, msg))
		checkMessage(t, *msg)
	}

	{
		msg := new(*protocol.AwakeableEntryMessage)
		willSucceed(t, Unmarshal(p, bytes, msg))
		checkMessage(t, *msg)
	}

	{
		var msg *protocol.AwakeableEntryMessage
		willPanic(t, func() {
			Unmarshal(p, bytes, msg)
		})
	}
}

func TestVoid(t *testing.T) {
	codecs := map[string]Codec{
		"json":      JSONCodec,
		"proto":     ProtoCodec,
		"protojson": ProtoJSONCodec,
		"binary":    BinaryCodec,
	}
	for name, codec := range codecs {
		t.Run(name, func(t *testing.T) {
			bytes, err := Marshal(codec, Void{})
			if err != nil {
				t.Fatal(err)
			}

			if bytes != nil {
				t.Fatalf("expected bytes to be nil, found %v", bytes)
			}

			if err := Unmarshal(codec, []byte{1, 2, 3}, &Void{}); err != nil {
				t.Fatal(err)
			}

			if err := Unmarshal(codec, []byte{1, 2, 3}, Void{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func BenchmarkProto(b *testing.B) {
	// protoscope -s <(echo '1: {4 5 6 7}') | base64
	data, err := base64.StdEncoding.DecodeString("CgQEBQYH")
	if err != nil {
		b.Fatal(err)
	}
	benchmarkProto(b, ProtoCodec, data)
}

func BenchmarkProtoJSON(b *testing.B) {
	benchmarkProto(b, ProtoJSONCodec, []byte(`{"entryIndexes": [1,2,3]}`))
}

func benchmarkProto(b *testing.B, codec Codec, data []byte) {
	b.Run("non-nil proto.Message", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			a := new(protocol.SuspensionMessage)
			if err := codec.Unmarshal(data, a); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("non-nil pointer to non-nil proto.Message", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			a := new(protocol.SuspensionMessage)
			if err := codec.Unmarshal(data, &a); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("non-nil pointer to nil proto.Message", func(b *testing.B) {
		for n := 0; n < b.N; n++ {
			var a *protocol.SuspensionMessage
			if err := codec.Unmarshal(data, &a); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkAllocateProtoMessage(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var a *protocol.SuspensionMessage
		allocateProtoMessage("", &a)
	}
}
