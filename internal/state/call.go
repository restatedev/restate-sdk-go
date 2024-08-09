package state

import (
	"bytes"
	"cmp"
	"fmt"
	"slices"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/encoding"
	protocol "github.com/restatedev/sdk-go/generated/dev/restate/service"
	"github.com/restatedev/sdk-go/internal/errors"
	"github.com/restatedev/sdk-go/internal/futures"
	"github.com/restatedev/sdk-go/internal/options"
	"github.com/restatedev/sdk-go/internal/wire"
)

type serviceCall struct {
	options options.CallOptions
	machine *Machine
	service string
	key     string
	method  string
}

// RequestFuture makes a call and returns a handle on the response
func (c *serviceCall) RequestFuture(input any) (restate.ResponseFuture, error) {
	bytes, err := encoding.Marshal(c.options.Codec, input)
	if err != nil {
		return nil, errors.NewTerminalError(fmt.Errorf("failed to marshal RequestFuture input: %w", err))
	}

	entry, entryIndex := c.machine.doCall(c.service, c.key, c.method, c.options.Headers, bytes)

	return decodingResponseFuture{
		futures.NewResponseFuture(c.machine.suspensionCtx, entry, entryIndex, func(err error) any { return c.machine.newProtocolViolation(entry, err) }),
		c.options,
	}, nil
}

type decodingResponseFuture struct {
	*futures.ResponseFuture
	options options.CallOptions
}

func (d decodingResponseFuture) Response(output any) (err error) {
	bytes, err := d.ResponseFuture.Response()
	if err != nil {
		return err
	}

	if err := encoding.Unmarshal(d.options.Codec, bytes, output); err != nil {
		return errors.NewTerminalError(fmt.Errorf("failed to unmarshal Call response into output: %w", err))
	}

	return nil
}

// Request makes a call and blocks on the response
func (c *serviceCall) Request(input any, output any) error {
	fut, err := c.RequestFuture(input)
	if err != nil {
		return err
	}
	return fut.Response(output)
}

// Send runs a call in the background after delay duration
func (c *serviceCall) Send(input any, delay time.Duration) error {
	bytes, err := encoding.Marshal(c.options.Codec, input)
	if err != nil {
		return errors.NewTerminalError(fmt.Errorf("failed to marshal Send input: %w", err))
	}
	c.machine.sendCall(c.service, c.key, c.method, c.options.Headers, bytes, delay)
	return nil
}

func (m *Machine) doCall(service, key, method string, headersMap map[string]string, params []byte) (*wire.CallEntryMessage, uint32) {
	headers := headersToProto(headersMap)

	entry, entryIndex := replayOrNew(
		m,
		func(entry *wire.CallEntryMessage) *wire.CallEntryMessage {
			if entry.ServiceName != service ||
				entry.Key != key ||
				entry.HandlerName != method ||
				!headersEqual(entry.Headers, headers) ||
				!bytes.Equal(entry.Parameter, params) {
				panic(m.newEntryMismatch(&wire.CallEntryMessage{
					CallEntryMessage: protocol.CallEntryMessage{
						ServiceName: service,
						HandlerName: method,
						Headers:     headers,
						Parameter:   params,
						Key:         key,
					},
				}, entry))
			}

			return entry
		}, func() *wire.CallEntryMessage {
			return m._doCall(service, key, method, headers, params)
		})
	return entry, entryIndex
}

func (m *Machine) _doCall(service, key, method string, headers []*protocol.Header, params []byte) *wire.CallEntryMessage {
	msg := &wire.CallEntryMessage{
		CallEntryMessage: protocol.CallEntryMessage{
			ServiceName: service,
			HandlerName: method,
			Parameter:   params,
			Headers:     headers,
			Key:         key,
		},
	}
	m.Write(msg)

	return msg
}

func headersEqual(left, right []*protocol.Header) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i].Key != right[i].Key || left[i].Value != right[i].Value {
			return false
		}
	}
	return true
}

func headersToProto(headers map[string]string) []*protocol.Header {
	if len(headers) == 0 {
		return nil
	}

	h := make([]*protocol.Header, 0, len(headers))
	for k, v := range headers {
		h = append(h, &protocol.Header{Key: k, Value: v})
	}

	slices.SortFunc(h, func(a, b *protocol.Header) int {
		return cmp.Compare(a.Key, b.Key)
	})

	return h
}

func (m *Machine) sendCall(service, key, method string, headersMap map[string]string, body []byte, delay time.Duration) {
	headers := headersToProto(headersMap)

	_, _ = replayOrNew(
		m,
		func(entry *wire.OneWayCallEntryMessage) restate.Void {
			if entry.ServiceName != service ||
				entry.Key != key ||
				entry.HandlerName != method ||
				!headersEqual(entry.Headers, headers) ||
				!bytes.Equal(entry.Parameter, body) {
				panic(m.newEntryMismatch(&wire.OneWayCallEntryMessage{
					OneWayCallEntryMessage: protocol.OneWayCallEntryMessage{
						ServiceName: service,
						HandlerName: method,
						Headers:     headers,
						Parameter:   body,
						Key:         key,
					},
				}, entry))
			}

			return restate.Void{}
		},
		func() restate.Void {
			m._sendCall(service, key, method, headers, body, delay)
			return restate.Void{}
		},
	)
}

func (c *Machine) _sendCall(service, key, method string, headers []*protocol.Header, params []byte, delay time.Duration) {
	var invokeTime uint64
	if delay != 0 {
		invokeTime = uint64(time.Now().Add(delay).UnixMilli())
	}

	c.Write(&wire.OneWayCallEntryMessage{
		OneWayCallEntryMessage: protocol.OneWayCallEntryMessage{
			ServiceName: service,
			HandlerName: method,
			Headers:     headers,
			Parameter:   params,
			Key:         key,
			InvokeTime:  invokeTime,
		},
	})
}
