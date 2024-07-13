package state

import (
	"bytes"
	"encoding/json"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/generated/proto/protocol"
	"github.com/restatedev/sdk-go/internal/futures"
	"github.com/restatedev/sdk-go/internal/wire"
)

var (
	_ restate.ServiceClient     = (*serviceProxy)(nil)
	_ restate.ServiceSendClient = (*serviceSendProxy)(nil)
	_ restate.CallClient        = (*serviceCall)(nil)
	_ restate.SendClient        = (*serviceSend)(nil)
)

type serviceProxy struct {
	machine *Machine
	service string
	key     string
}

func (c *serviceProxy) Method(fn string) restate.CallClient {
	return &serviceCall{
		machine: c.machine,
		service: c.service,
		key:     c.key,
		method:  fn,
	}
}

type serviceSendProxy struct {
	machine *Machine
	service string
	key     string
	delay   time.Duration
}

func (c *serviceSendProxy) Method(fn string) restate.SendClient {
	return &serviceSend{
		machine: c.machine,
		service: c.service,
		key:     c.key,
		method:  fn,
		delay:   c.delay,
	}
}

type serviceCall struct {
	machine *Machine
	service string
	key     string
	method  string
}

// Do makes a call and wait for the response
func (c *serviceCall) Request(input any) restate.ResponseFuture {
	if entry, entryIndex, err := c.machine.doDynCall(c.service, c.key, c.method, input); err != nil {
		return futures.NewFailedResponseFuture(err)
	} else {
		return futures.NewResponseFuture(c.machine.suspensionCtx, entry, entryIndex)
	}
}

type serviceSend struct {
	machine *Machine
	service string
	key     string
	method  string

	delay time.Duration
}

// Send runs a call in the background after delay duration
func (c *serviceSend) Request(input any) error {
	return c.machine.sendCall(c.service, c.key, c.method, input, c.delay)
}

func (m *Machine) doDynCall(service, key, method string, input any) (*wire.CallEntryMessage, uint32, error) {
	params, err := json.Marshal(input)
	if err != nil {
		return nil, 0, err
	}

	entry, entryIndex := m.doCall(service, key, method, params)
	return entry, entryIndex, nil
}

func (m *Machine) doCall(service, key, method string, params []byte) (*wire.CallEntryMessage, uint32) {
	entry, entryIndex := replayOrNew(
		m,
		func(entry *wire.CallEntryMessage) *wire.CallEntryMessage {
			if entry.ServiceName != service ||
				entry.Key != key ||
				entry.HandlerName != method ||
				!bytes.Equal(entry.Parameter, params) {
				panic(m.newEntryMismatch(&wire.CallEntryMessage{
					CallEntryMessage: protocol.CallEntryMessage{
						ServiceName: service,
						HandlerName: method,
						Parameter:   params,
						Key:         key,
					},
				}, entry))
			}

			return entry
		}, func() *wire.CallEntryMessage {
			return m._doCall(service, key, method, params)
		})
	return entry, entryIndex
}

func (m *Machine) _doCall(service, key, method string, params []byte) *wire.CallEntryMessage {
	msg := &wire.CallEntryMessage{
		CallEntryMessage: protocol.CallEntryMessage{
			ServiceName: service,
			HandlerName: method,
			Parameter:   params,
			Key:         key,
		},
	}
	m.Write(msg)

	return msg
}

func (m *Machine) sendCall(service, key, method string, body any, delay time.Duration) error {
	params, err := json.Marshal(body)
	if err != nil {
		return err
	}

	_, _ = replayOrNew(
		m,
		func(entry *wire.OneWayCallEntryMessage) restate.Void {
			if entry.ServiceName != service ||
				entry.Key != key ||
				entry.HandlerName != method ||
				!bytes.Equal(entry.Parameter, params) {
				panic(m.newEntryMismatch(&wire.OneWayCallEntryMessage{
					OneWayCallEntryMessage: protocol.OneWayCallEntryMessage{
						ServiceName: service,
						HandlerName: method,
						Parameter:   params,
						Key:         key,
					},
				}, entry))
			}

			return restate.Void{}
		},
		func() restate.Void {
			m._sendCall(service, key, method, params, delay)
			return restate.Void{}
		},
	)

	return nil
}

func (c *Machine) _sendCall(service, key, method string, params []byte, delay time.Duration) {
	var invokeTime uint64
	if delay != 0 {
		invokeTime = uint64(time.Now().Add(delay).UnixMilli())
	}

	c.Write(&wire.OneWayCallEntryMessage{
		OneWayCallEntryMessage: protocol.OneWayCallEntryMessage{
			ServiceName: service,
			HandlerName: method,
			Parameter:   params,
			Key:         key,
			InvokeTime:  invokeTime,
		},
	})
}
