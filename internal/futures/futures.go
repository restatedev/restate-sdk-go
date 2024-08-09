package futures

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"

	protocol "github.com/restatedev/sdk-go/generated/dev/restate/service"
	"github.com/restatedev/sdk-go/internal/errors"
	"github.com/restatedev/sdk-go/internal/wire"
)

var (
	_ Selectable = (*After)(nil)
	_ Selectable = (*Awakeable)(nil)
	_ Selectable = (*ResponseFuture)(nil)
)

type After struct {
	suspensionCtx context.Context
	entry         *wire.SleepEntryMessage
	entryIndex    uint32
}

func NewAfter(suspensionCtx context.Context, entry *wire.SleepEntryMessage, entryIndex uint32) *After {
	return &After{suspensionCtx, entry, entryIndex}
}

func (a *After) Done() error {
	a.entry.Await(a.suspensionCtx, a.entryIndex)
	switch result := a.entry.Result.(type) {
	case *protocol.SleepEntryMessage_Empty:
		return nil
	case *protocol.SleepEntryMessage_Failure:
		return errors.ErrorFromFailure(result.Failure)
	default:
		return fmt.Errorf("sleep entry had invalid result: %v", a.entry.Result)
	}
}

func (a *After) getEntry() (wire.CompleteableMessage, uint32) {
	return a.entry, a.entryIndex
}

const AWAKEABLE_IDENTIFIER_PREFIX = "prom_1"

type Awakeable struct {
	suspensionCtx context.Context
	invocationID  []byte
	entry         *wire.AwakeableEntryMessage
	entryIndex    uint32
}

func NewAwakeable(suspensionCtx context.Context, invocationID []byte, entry *wire.AwakeableEntryMessage, entryIndex uint32) *Awakeable {
	return &Awakeable{suspensionCtx, invocationID, entry, entryIndex}
}

func (c *Awakeable) Id() string { return awakeableID(c.invocationID, c.entryIndex) }
func (c *Awakeable) Result() ([]byte, error) {
	c.entry.Await(c.suspensionCtx, c.entryIndex)

	switch result := c.entry.Result.(type) {
	case *protocol.AwakeableEntryMessage_Value:
		return result.Value, nil
	case *protocol.AwakeableEntryMessage_Failure:
		return nil, errors.ErrorFromFailure(result.Failure)
	default:
		return nil, fmt.Errorf("unexpected result in completed awakeable entry: %v", c.entry.Result)
	}
}
func (c *Awakeable) getEntry() (wire.CompleteableMessage, uint32) {
	return c.entry, c.entryIndex
}

func awakeableID(invocationID []byte, entryIndex uint32) string {
	bytes := make([]byte, 0, len(invocationID)+4)
	bytes = append(bytes, invocationID...)
	bytes = binary.BigEndian.AppendUint32(bytes, entryIndex)
	return "prom_1" + base64.RawURLEncoding.EncodeToString(bytes)
}

type ResponseFuture struct {
	suspensionCtx        context.Context
	entry                *wire.CallEntryMessage
	entryIndex           uint32
	newProtocolViolation func(error) any
}

func NewResponseFuture(suspensionCtx context.Context, entry *wire.CallEntryMessage, entryIndex uint32, newProtocolViolation func(error) any) *ResponseFuture {
	return &ResponseFuture{suspensionCtx, entry, entryIndex, newProtocolViolation}
}

func (r *ResponseFuture) Response() ([]byte, error) {
	r.entry.Await(r.suspensionCtx, r.entryIndex)

	switch result := r.entry.Result.(type) {
	case *protocol.CallEntryMessage_Failure:
		return nil, errors.ErrorFromFailure(result.Failure)
	case *protocol.CallEntryMessage_Value:
		return result.Value, nil
	default:
		panic(r.newProtocolViolation(fmt.Errorf("call entry had invalid result: %v", r.entry.Result)))
	}
}

func (r *ResponseFuture) getEntry() (wire.CompleteableMessage, uint32) {
	return r.entry, r.entryIndex
}
