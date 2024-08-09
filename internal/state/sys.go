package state

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	restate "github.com/restatedev/sdk-go"
	protocol "github.com/restatedev/sdk-go/generated/dev/restate/service"
	"github.com/restatedev/sdk-go/internal/errors"
	"github.com/restatedev/sdk-go/internal/futures"
	"github.com/restatedev/sdk-go/internal/wire"
	"google.golang.org/protobuf/proto"
)

type entryMismatch struct {
	entryIndex uint32
	// this can be satisfied by a nil pointer in the case that there is an entry type mismatch
	expectedEntry wire.Message
	actualEntry   wire.Message
}

func (m *Machine) newEntryMismatch(expectedEntry wire.Message, actualEntry wire.Message) *entryMismatch {
	e := &entryMismatch{m.entryIndex, expectedEntry, actualEntry}
	m.failure = e
	return e
}

type protocolViolation struct {
	entryIndex uint32
	entry      wire.Message
	err        error
}

func (m *Machine) newProtocolViolation(entry wire.Message, err error) *protocolViolation {
	e := &protocolViolation{m.entryIndex, entry, err}
	m.failure = e
	return e
}

func (m *Machine) set(key string, value []byte) {
	_, _ = replayOrNew(
		m,
		func(entry *wire.SetStateEntryMessage) (void restate.Void) {
			if string(entry.Key) != key || !bytes.Equal(entry.Value, value) {
				panic(m.newEntryMismatch(&wire.SetStateEntryMessage{
					SetStateEntryMessage: protocol.SetStateEntryMessage{
						Key:   []byte(key),
						Value: value,
					},
				}, entry))
			}
			return
		}, func() (void restate.Void) {
			m._set(key, value)
			return void
		})

	m.current[key] = value
}

func (m *Machine) _set(key string, value []byte) {
	m.Write(
		&wire.SetStateEntryMessage{
			SetStateEntryMessage: protocol.SetStateEntryMessage{
				Key:   []byte(key),
				Value: value,
			},
		})
}

func (m *Machine) clear(key string) {
	_, _ = replayOrNew(
		m,
		func(entry *wire.ClearStateEntryMessage) (void restate.Void) {
			if string(entry.Key) != key {
				panic(m.newEntryMismatch(&wire.ClearStateEntryMessage{
					ClearStateEntryMessage: protocol.ClearStateEntryMessage{
						Key: []byte(key),
					},
				}, entry))
			}

			return
		}, func() restate.Void {
			m._clear(key)
			return restate.Void{}
		},
	)

	delete(m.current, key)
}

func (m *Machine) _clear(key string) {
	m.Write(
		&wire.ClearStateEntryMessage{
			ClearStateEntryMessage: protocol.ClearStateEntryMessage{
				Key: []byte(key),
			},
		},
	)
}

func (m *Machine) clearAll() {
	_, _ = replayOrNew(
		m,
		func(entry *wire.ClearAllStateEntryMessage) (void restate.Void) {
			return
		}, func() restate.Void {
			m._clearAll()
			return restate.Void{}
		},
	)
	m.current = map[string][]byte{}
	m.partial = false
}

// clearAll drops all associated keys
func (m *Machine) _clearAll() {
	m.Write(
		&wire.ClearAllStateEntryMessage{},
	)
}

func (m *Machine) get(key string) []byte {
	entry, entryIndex := replayOrNew(
		m,
		func(entry *wire.GetStateEntryMessage) *wire.GetStateEntryMessage {
			if string(entry.Key) != key {
				panic(m.newEntryMismatch(&wire.GetStateEntryMessage{
					GetStateEntryMessage: protocol.GetStateEntryMessage{
						Key: []byte(key),
					},
				}, entry))
			}
			return entry
		}, func() *wire.GetStateEntryMessage {
			return m._get(key)
		})

	entry.Await(m.suspensionCtx, entryIndex)

	switch value := entry.Result.(type) {
	case *protocol.GetStateEntryMessage_Empty:
		return nil
	case *protocol.GetStateEntryMessage_Value:
		m.current[key] = value.Value
		return value.Value
	default:
		panic(m.newProtocolViolation(entry, fmt.Errorf("get state entry had invalid result: %v", entry.Result)))
	}
}

func (m *Machine) _get(key string) *wire.GetStateEntryMessage {
	msg := &wire.GetStateEntryMessage{
		GetStateEntryMessage: protocol.GetStateEntryMessage{
			Key: []byte(key),
		},
	}

	value, ok := m.current[key]

	if ok {
		// value in map, we still send the current
		// value to the runtime
		msg.Complete(&protocol.CompletionMessage{Result: &protocol.CompletionMessage_Value{Value: value}})

		m.Write(msg)

		return msg
	}

	// key is not in map! there are 2 cases.
	if !m.partial {
		// current is complete. we need to return nil to the user
		// but also send an empty get state entry message
		msg.Complete(&protocol.CompletionMessage{Result: &protocol.CompletionMessage_Empty{Empty: &protocol.Empty{}}})

		m.Write(msg)

		return msg
	}

	// we didn't see the value and we don't know for sure there isn't one; ask the runtime for it

	m.Write(msg)

	return msg
}

func (m *Machine) keys() []string {
	entry, entryIndex := replayOrNew(
		m,
		func(entry *wire.GetStateKeysEntryMessage) *wire.GetStateKeysEntryMessage {
			return entry
		},
		m._keys,
	)

	entry.Await(m.suspensionCtx, entryIndex)

	switch value := entry.Result.(type) {
	case *protocol.GetStateKeysEntryMessage_Value:
		values := make([]string, 0, len(value.Value.Keys))
		for _, key := range value.Value.Keys {
			values = append(values, string(key))
		}

		return values
	default:
		panic(m.newProtocolViolation(entry, fmt.Errorf("get state keys entry had invalid result: %v", entry.Result)))
	}
}

func (m *Machine) _keys() *wire.GetStateKeysEntryMessage {
	msg := &wire.GetStateKeysEntryMessage{}
	if !m.partial {
		keys := make([]string, 0, len(m.current))
		for k := range m.current {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		byteKeys := make([][]byte, len(keys))
		for i := range keys {
			byteKeys[i] = []byte(keys[i])
		}

		stateKeys := &protocol.GetStateKeysEntryMessage_StateKeys{Keys: byteKeys}
		value, err := proto.Marshal(stateKeys)
		if err != nil {
			panic(err) // this is pretty much impossible
		}

		// we can return keys entirely from cache
		msg.Complete(&protocol.CompletionMessage{Result: &protocol.CompletionMessage_Value{
			Value: value,
		}})
	}

	m.Write(msg)

	return msg
}

func (m *Machine) after(d time.Duration) *futures.After {
	entry, entryIndex := replayOrNew(
		m,
		func(entry *wire.SleepEntryMessage) *wire.SleepEntryMessage {
			// we shouldn't verify the time because this would be different every time
			return entry
		}, func() *wire.SleepEntryMessage {
			return m._sleep(d)
		},
	)

	return futures.NewAfter(m.suspensionCtx, entry, entryIndex)
}

func (m *Machine) sleep(d time.Duration) error {
	return m.after(d).Done()
}

// _sleep creating a new sleep entry.
func (m *Machine) _sleep(d time.Duration) *wire.SleepEntryMessage {
	msg := &wire.SleepEntryMessage{
		SleepEntryMessage: protocol.SleepEntryMessage{
			WakeUpTime: uint64(time.Now().Add(d).UnixMilli()),
		},
	}
	m.Write(msg)

	return msg
}

func (m *Machine) run(fn func(restate.RunContext) ([]byte, error)) ([]byte, error) {
	entry, entryIndex := replayOrNew(
		m,
		func(entry *wire.RunEntryMessage) *wire.RunEntryMessage {
			return entry
		},
		func() *wire.RunEntryMessage {
			return m._run(fn)
		},
	)

	// run entry must be acknowledged before proceeding
	entry.Await(m.suspensionCtx, entryIndex)

	switch result := entry.Result.(type) {
	case *protocol.RunEntryMessage_Failure:
		return nil, errors.ErrorFromFailure(result.Failure)
	case *protocol.RunEntryMessage_Value:
		return result.Value, nil
	case nil:
		// Empty result is valid
		return nil, nil
	default:
		panic(m.newProtocolViolation(entry, fmt.Errorf("run entry had invalid result: %v", entry.Result)))
	}
}

type runContext struct {
	context.Context
	log     *slog.Logger
	request *restate.Request
}

func (r runContext) Log() *slog.Logger         { return r.log }
func (r runContext) Request() *restate.Request { return r.request }

func (m *Machine) _run(fn func(restate.RunContext) ([]byte, error)) *wire.RunEntryMessage {
	bytes, err := fn(runContext{m.ctx, m.userLog, &m.request})

	if err != nil {
		if restate.IsTerminalError(err) {
			msg := &wire.RunEntryMessage{
				RunEntryMessage: protocol.RunEntryMessage{
					Result: &protocol.RunEntryMessage_Failure{
						Failure: &protocol.Failure{
							Code:    uint32(restate.ErrorCode(err)),
							Message: err.Error(),
						},
					},
				},
			}
			m.Write(msg)

			return msg
		} else {
			panic(m.newRunFailure(err))
		}
	} else {
		msg := &wire.RunEntryMessage{
			RunEntryMessage: protocol.RunEntryMessage{
				Result: &protocol.RunEntryMessage_Value{
					Value: bytes,
				},
			},
		}
		m.Write(msg)

		return msg
	}
}

type runFailure struct {
	entryIndex uint32
	err        error
}

func (m *Machine) newRunFailure(err error) *runFailure {
	s := &runFailure{m.entryIndex, err}
	m.failure = s
	return s
}
