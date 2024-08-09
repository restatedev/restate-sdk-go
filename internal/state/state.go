package state

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"log/slog"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/encoding"
	protocol "github.com/restatedev/sdk-go/generated/dev/restate/service"
	"github.com/restatedev/sdk-go/internal/errors"
	"github.com/restatedev/sdk-go/internal/futures"
	"github.com/restatedev/sdk-go/internal/log"
	"github.com/restatedev/sdk-go/internal/options"
	"github.com/restatedev/sdk-go/internal/rand"
	"github.com/restatedev/sdk-go/internal/wire"
	"github.com/restatedev/sdk-go/rcontext"
)

const (
	Version = 1
)

var (
	ErrInvalidVersion = fmt.Errorf("invalid version number")
)

type Context struct {
	context.Context
	userLogger *slog.Logger
	machine    *Machine
}

var _ restate.ObjectContext = &Context{}
var _ restate.ObjectSharedContext = &Context{}
var _ restate.Context = &Context{}
var _ restate.RunContext = &Context{}

func (c *Context) Log() *slog.Logger {
	return c.machine.userLog
}

func (c *Context) Request() *restate.Request {
	return &c.machine.request
}

func (c *Context) Rand() *rand.Rand {
	return c.machine.rand
}

func (c *Context) Set(key string, value any, opts ...options.SetOption) error {
	o := options.SetOptions{}
	for _, opt := range opts {
		opt.BeforeSet(&o)
	}
	if o.Codec == nil {
		o.Codec = encoding.JSONCodec
	}

	bytes, err := encoding.Marshal(o.Codec, value)
	if err != nil {
		return errors.NewTerminalError(fmt.Errorf("failed to marshal Set value: %w", err))
	}

	c.machine.set(key, bytes)
	return nil
}

func (c *Context) Clear(key string) {
	c.machine.clear(key)

}

// ClearAll drops all associated keys
func (c *Context) ClearAll() {
	c.machine.clearAll()

}

func (c *Context) Get(key string, output any, opts ...options.GetOption) error {
	o := options.GetOptions{}
	for _, opt := range opts {
		opt.BeforeGet(&o)
	}
	if o.Codec == nil {
		o.Codec = encoding.JSONCodec
	}

	bytes := c.machine.get(key)
	if len(bytes) == 0 {
		return errors.ErrKeyNotFound
	}

	if err := encoding.Unmarshal(o.Codec, bytes, output); err != nil {
		return errors.NewTerminalError(fmt.Errorf("failed to unmarshal Get state into output: %w", err))
	}

	return nil
}

func (c *Context) Keys() []string {
	return c.machine.keys()
}

func (c *Context) Sleep(d time.Duration) error {
	return c.machine.sleep(d)
}

func (c *Context) After(d time.Duration) restate.After {
	return c.machine.after(d)
}

func (c *Context) Service(service, method string, opts ...options.CallOption) restate.CallClient {
	o := options.CallOptions{}
	for _, opt := range opts {
		opt.BeforeCall(&o)
	}
	if o.Codec == nil {
		o.Codec = encoding.JSONCodec
	}

	return &serviceCall{
		options: o,
		machine: c.machine,
		service: service,
		method:  method,
	}
}

func (c *Context) Object(service, key, method string, opts ...options.CallOption) restate.CallClient {
	o := options.CallOptions{}
	for _, opt := range opts {
		opt.BeforeCall(&o)
	}
	if o.Codec == nil {
		o.Codec = encoding.JSONCodec
	}

	return &serviceCall{
		options: o,
		machine: c.machine,
		service: service,
		key:     key,
		method:  method,
	}
}

func (c *Context) Run(fn func(ctx restate.RunContext) (any, error), output any, opts ...options.RunOption) error {
	o := options.RunOptions{}
	for _, opt := range opts {
		opt.BeforeRun(&o)
	}
	if o.Codec == nil {
		o.Codec = encoding.JSONCodec
	}

	bytes, err := c.machine.run(func(ctx restate.RunContext) ([]byte, error) {
		output, err := fn(ctx)
		if err != nil {
			return nil, err
		}

		bytes, err := encoding.Marshal(o.Codec, output)
		if err != nil {
			return nil, errors.NewTerminalError(fmt.Errorf("failed to marshal Run output: %w", err))
		}

		return bytes, nil
	})
	if err != nil {
		return err
	}

	if err := encoding.Unmarshal(o.Codec, bytes, output); err != nil {
		return errors.NewTerminalError(fmt.Errorf("failed to unmarshal Run output: %w", err))
	}

	return nil
}

type awakeableOptions struct {
	codec encoding.Codec
}

type AwakeableOption interface {
	beforeAwakeable(*awakeableOptions)
}

func (c *Context) Awakeable(opts ...options.AwakeableOption) restate.Awakeable {
	o := options.AwakeableOptions{}
	for _, opt := range opts {
		opt.BeforeAwakeable(&o)
	}
	if o.Codec == nil {
		o.Codec = encoding.JSONCodec
	}
	return decodingAwakeable{c.machine.awakeable(), o.Codec}
}

type decodingAwakeable struct {
	*futures.Awakeable
	codec encoding.Codec
}

func (d decodingAwakeable) Id() string { return d.Awakeable.Id() }
func (d decodingAwakeable) Result(output any) (err error) {
	bytes, err := d.Awakeable.Result()
	if err != nil {
		return err
	}
	if err := encoding.Unmarshal(d.codec, bytes, output); err != nil {
		return errors.NewTerminalError(fmt.Errorf("failed to unmarshal Awakeable result into output: %w", err))
	}
	return
}

func (c *Context) ResolveAwakeable(id string, value any, opts ...options.ResolveAwakeableOption) error {
	o := options.ResolveAwakeableOptions{}
	for _, opt := range opts {
		opt.BeforeResolveAwakeable(&o)
	}
	if o.Codec == nil {
		o.Codec = encoding.JSONCodec
	}
	bytes, err := encoding.Marshal(o.Codec, value)
	if err != nil {
		return errors.NewTerminalError(fmt.Errorf("failed to marshal ResolveAwakeable value: %w", err))
	}
	c.machine.resolveAwakeable(id, bytes)
	return nil
}

func (c *Context) RejectAwakeable(id string, reason error) {
	c.machine.rejectAwakeable(id, reason)
}

func (c *Context) Select(futs ...restate.Selectable) restate.Selector {
	return c.machine.selector(futs...)
}

func (c *Context) Key() string {
	return c.machine.key
}

func newContext(inner context.Context, machine *Machine) *Context {
	// will be cancelled when the http2 stream is cancelled
	// but NOT when we just suspend - just because we can't get completions doesn't mean we can't make
	// progress towards producing an output message
	ctx := &Context{
		Context: inner,
		machine: machine,
	}

	return ctx
}

type Machine struct {
	ctx           context.Context
	suspensionCtx context.Context
	suspend       func(error)

	handler  restate.Handler
	protocol *wire.Protocol

	// state
	key     string
	request restate.Request

	partial bool
	current map[string][]byte

	entries    []wire.Message
	entryIndex uint32
	entryMutex sync.Mutex

	log            *slog.Logger
	userLog        *slog.Logger
	userLogContext atomic.Pointer[rcontext.LogContext]

	pendingCompletions map[uint32]wire.CompleteableMessage
	pendingAcks        map[uint32]wire.AckableMessage
	pendingMutex       sync.RWMutex

	rand *rand.Rand

	failure any
}

func NewMachine(handler restate.Handler, conn io.ReadWriter, attemptHeaders map[string][]string) *Machine {
	m := &Machine{
		handler:            handler,
		current:            make(map[string][]byte),
		pendingAcks:        map[uint32]wire.AckableMessage{},
		pendingCompletions: map[uint32]wire.CompleteableMessage{},
		request: restate.Request{
			AttemptHeaders: attemptHeaders,
		},
	}
	m.protocol = wire.NewProtocol(conn)
	return m
}

func (m *Machine) Log() *slog.Logger { return m.log }

// Start starts the state machine
func (m *Machine) Start(inner context.Context, dropReplayLogs bool, logHandler slog.Handler) error {
	msg, _, err := m.protocol.Read()
	if err != nil {
		return err
	}

	start, ok := msg.(*wire.StartMessage)
	if !ok {
		// invalid negotiation
		return wire.ErrUnexpectedMessage
	}

	m.ctx = inner
	m.suspensionCtx, m.suspend = context.WithCancelCause(m.ctx)
	m.request.ID = start.Id
	m.rand = rand.New(m.request.ID)
	m.key = start.Key

	logHandler = logHandler.WithAttrs([]slog.Attr{slog.String("invocationID", start.DebugId)})

	m.log = slog.New(log.NewRestateContextHandler(logHandler))
	m.userLog = slog.New(log.NewUserContextHandler(&m.userLogContext, dropReplayLogs, logHandler))

	ctx := newContext(inner, m)

	return m.process(ctx, start)
}

func (m *Machine) invoke(ctx *Context, outputSeen bool) error {
	// always terminate the invocation with
	// an end message.
	// this will always terminate the connection

	defer func() {
		// recover will return a non-nil object
		// if there was a panic
		//
		recovered := recover()

		switch typ := recovered.(type) {
		case nil:
			// nothing to do, just exit
			return
		case *protocolViolation:
			m.log.LogAttrs(m.ctx, slog.LevelError, "Protocol violation", log.Error(typ.err))

			if err := m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
				ErrorMessage: protocol.ErrorMessage{
					Code:              uint32(errors.ErrProtocolViolation),
					Message:           fmt.Sprintf("Protocol violation: %v", typ.err),
					RelatedEntryIndex: &typ.entryIndex,
					RelatedEntryType:  wire.MessageType(typ.entry).UInt32(),
				},
			}); err != nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Error sending failure message", log.Error(err))
			}
		case *concurrentContextUse:
			m.log.LogAttrs(m.ctx, slog.LevelError, "Concurrent context use detected; either a Context method was used while a Run() is in progress, or Context methods are being called from multiple goroutines. Failing invocation.", slog.Uint64("entryType", uint64(typ.entryType)))

			if err := m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
				ErrorMessage: protocol.ErrorMessage{
					Code:             uint32(errors.ErrProtocolViolation),
					Message:          "Concurrent context use detected; either a Context method was used while a Run() is in progress, or Context methods are being called from multiple goroutines.",
					RelatedEntryType: typ.entryType.UInt32(),
				},
			}); err != nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Error sending failure message", log.Error(err))
			}
		case *entryMismatch:
			expected, _ := json.Marshal(typ.expectedEntry)
			actual, _ := json.Marshal(typ.actualEntry)

			m.log.LogAttrs(m.ctx, slog.LevelError, "Journal mismatch: Replayed journal entries did not correspond to the user code. The user code has to be deterministic!",
				log.Type("expectedType", typ.expectedEntry),
				slog.String("expectedMessage", string(expected)),
				log.Type("actualType", typ.actualEntry),
				slog.String("actualMessage", string(actual)))

			// journal entry mismatch
			if err := m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
				ErrorMessage: protocol.ErrorMessage{
					Code: uint32(errors.ErrJournalMismatch),
					Message: fmt.Sprintf(`Journal mismatch: Replayed journal entries did not correspond to the user code. The user code has to be deterministic!
The journal entry at position %d was:
- In the user code: type: %T, message: %s
- In the replayed messages: type: %T, message %s`,
						typ.entryIndex, typ.expectedEntry, string(expected), typ.actualEntry, string(actual)),
					RelatedEntryIndex: &typ.entryIndex,
					RelatedEntryType:  wire.MessageType(typ.actualEntry).UInt32(),
				},
			}); err != nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Error sending failure message", log.Error(err))
			}

			return
		case *writeError:
			m.log.LogAttrs(m.ctx, slog.LevelError, "Failed to write entry to Restate, shutting down state machine", log.Error(typ.err))
			// don't even check for failure here because most likely the http2 conn is closed anyhow
			_ = m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
				ErrorMessage: protocol.ErrorMessage{
					Code:              uint32(errors.ErrProtocolViolation),
					Message:           typ.err.Error(),
					RelatedEntryIndex: &typ.entryIndex,
					RelatedEntryType:  wire.MessageType(typ.entry).UInt32(),
				},
			})

			return
		case *runFailure:
			m.log.LogAttrs(m.ctx, slog.LevelError, "Run returned a failure, returning error to Restate", log.Error(typ.err))

			if err := m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
				ErrorMessage: protocol.ErrorMessage{
					Code:              uint32(restate.ErrorCode(typ.err)),
					Message:           typ.err.Error(),
					RelatedEntryIndex: &typ.entryIndex,
					RelatedEntryType:  wire.AwakeableEntryMessageType.UInt32(),
				},
			}); err != nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Error sending failure message", log.Error(typ.err))
			}

			return
		case *wire.SuspensionPanic:
			if m.ctx.Err() != nil {
				m.log.WarnContext(m.ctx, "Cancelling invocation as the incoming request was cancelled")
				return
			}
			if stderrors.Is(typ.Err, io.EOF) {
				m.log.LogAttrs(m.ctx, slog.LevelInfo, "Suspending invocation", slog.Any("entryIndexes", typ.EntryIndexes))

				if err := m.protocol.Write(wire.SuspensionMessageType, &wire.SuspensionMessage{
					SuspensionMessage: protocol.SuspensionMessage{
						EntryIndexes: typ.EntryIndexes,
					},
				}); err != nil {
					m.log.LogAttrs(m.ctx, slog.LevelError, "Error sending suspension message", log.Error(err))
				}
			} else {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Unexpected error reading completions; shutting down state machine", log.Error(typ.Err), slog.Any("entryIndexes", typ.EntryIndexes))

				// don't check for error here, most likely we will fail to send if we are in such a bad state
				_ = m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
					ErrorMessage: protocol.ErrorMessage{
						Code:    uint32(restate.ErrorCode(typ.Err)),
						Message: fmt.Sprintf("problem reading completions: %v", typ.Err),
					},
				})
			}

			return
		default:
			m.log.LogAttrs(m.ctx, slog.LevelError, "Invocation panicked, returning error to Restate", slog.Any("err", typ))

			// unknown panic!
			// send an error message (retryable)
			if err := m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
				ErrorMessage: protocol.ErrorMessage{
					Code:        500,
					Message:     fmt.Sprint(typ),
					Description: string(debug.Stack()),
				},
			}); err != nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Error sending failure message", log.Error(err))
			}

			return
		}
	}()

	m.log.InfoContext(m.ctx, "Handling invocation")

	if outputSeen {
		m.log.WarnContext(m.ctx, "Invocation already completed; ending immediately")

		return m.protocol.Write(wire.EndMessageType, &wire.EndMessage{})
	}

	var bytes []byte
	var err error
	switch handler := m.handler.(type) {
	case restate.ObjectHandler:
		bytes, err = handler.Call(ctx, m.request.Body)
	case restate.ServiceHandler:
		bytes, err = handler.Call(ctx, m.request.Body)
	}

	if err != nil && restate.IsTerminalError(err) {
		m.log.LogAttrs(m.ctx, slog.LevelError, "Invocation returned a terminal failure", log.Error(err))

		// terminal errors.
		if err := m.protocol.Write(wire.OutputEntryMessageType, &wire.OutputEntryMessage{
			OutputEntryMessage: protocol.OutputEntryMessage{
				Result: &protocol.OutputEntryMessage_Failure{
					Failure: &protocol.Failure{
						Code:    uint32(restate.ErrorCode(err)),
						Message: err.Error(),
					},
				},
			},
		}); err != nil {
			return err
		}
		return m.protocol.Write(wire.EndMessageType, &wire.EndMessage{})
	} else if err != nil {
		m.log.LogAttrs(m.ctx, slog.LevelError, "Invocation returned a non-terminal failure", log.Error(err))

		// non terminal error - no end message
		return m.protocol.Write(wire.ErrorMessageType, &wire.ErrorMessage{
			ErrorMessage: protocol.ErrorMessage{
				Code:    uint32(restate.ErrorCode(err)),
				Message: err.Error(),
			},
		})
	} else {
		m.log.InfoContext(m.ctx, "Invocation completed successfully")

		if err := m.protocol.Write(wire.OutputEntryMessageType, &wire.OutputEntryMessage{
			OutputEntryMessage: protocol.OutputEntryMessage{
				Result: &protocol.OutputEntryMessage_Value{
					Value: bytes,
				},
			},
		}); err != nil {
			return err
		}

		return m.protocol.Write(wire.EndMessageType, &wire.EndMessage{})
	}
}

func (m *Machine) process(ctx *Context, start *wire.StartMessage) error {
	for _, entry := range start.StateMap {
		m.current[string(entry.Key)] = entry.Value
	}
	m.partial = start.PartialState

	// expect input message
	msg, _, err := m.protocol.Read()
	if err != nil {
		return err
	}

	inputMsg, ok := msg.(*wire.InputEntryMessage)
	if !ok {
		return wire.ErrUnexpectedMessage
	}

	m.log.LogAttrs(m.ctx, log.LevelTrace, "Received input message", slog.Uint64("knownEntries", uint64(start.KnownEntries)))
	m.entries = make([]wire.Message, 0, start.KnownEntries-1)
	if start.KnownEntries > 1 {
		// more than just an input message; will be at least one replay
		m.userLogContext.Store(&rcontext.LogContext{Source: rcontext.LogSourceUser, IsReplaying: true})
	} else {
		// only an input message; no replayed messages
		m.userLogContext.Store(&rcontext.LogContext{Source: rcontext.LogSourceUser, IsReplaying: false})
	}

	outputSeen := false

	// we don't track the poll input entry
	for i := uint32(1); i < start.KnownEntries; i++ {
		msg, typ, err := m.protocol.Read()
		if err != nil {
			return fmt.Errorf("failed to read entry: %w", err)
		}

		m.log.LogAttrs(m.ctx, log.LevelTrace, "Received replay journal entry from runtime", log.Stringer("type", typ), slog.Uint64("index", uint64(i)))

		m.entries = append(m.entries, msg)

		if _, ok := msg.(*wire.OutputEntryMessage); ok {
			outputSeen = true
		}
	}

	go m.handleCompletionsAcks()

	m.request.Body = inputMsg.GetValue()

	if len(inputMsg.GetHeaders()) > 0 {
		m.request.Headers = make(map[string]string, len(inputMsg.Headers))
		for _, header := range inputMsg.Headers {
			m.request.Headers[header.Key] = header.Value
		}
	}

	return m.invoke(ctx, outputSeen)
}

func (c *Machine) currentEntry() (wire.Message, bool) {
	if c.entryIndex <= uint32(len(c.entries)) {
		return c.entries[c.entryIndex-1], true
	}

	return nil, false
}

// replayOrNew is a utility function to easily either
// replay a log entry, or create a new one if one
// does not exist
//
// this should be an instance method on Machine but unfortunately
// go does not support generics on instance methods
//
// the idea is when called, it will check if there is a log
// entry at current index, then compare the entry message type
// if not matching, that's obviously an error with the code version
// (code has changed and now doesn't match the play log)
//
// if type is okay, the function will then call a `replay“ callback.
// the replay callback just need to extract the result from the entry
//
// otherwise this function will call a `new` callback to create a new entry in the log
// by sending the proper runtime messages
func replayOrNew[M wire.Message, O any](
	m *Machine,
	replay func(msg M) O,
	new func() O,
) (output O, entryIndex uint32) {
	// lock around preparing the entry, but we would never await an ack or completion with this held.
	if !m.entryMutex.TryLock() {
		var msg M
		panic(m.newConcurrentContextUse(wire.MessageType(msg)))
	}
	defer m.entryMutex.Unlock()

	if m.failure != nil {
		// maybe the user will try to catch our panics, but we will just keep producing them
		panic(m.failure)
	}

	m.entryIndex += 1
	if m.entryIndex == uint32(len(m.entries)) {
		// this is a replay, but the next entry will not be a replay; log should now be allowed
		m.userLogContext.Store(&rcontext.LogContext{Source: rcontext.LogSourceUser, IsReplaying: false})
	}

	// check if there is an entry as this index
	entry, ok := m.currentEntry()

	// if entry exists, we need to replay it
	// by calling the replay function
	if ok {
		if entry, ok := entry.(M); !ok {
			// will be eg *wire.CallEntryMessage(nil)
			var expectedEntry M
			panic(m.newEntryMismatch(expectedEntry, entry))
		} else {
			return replay(entry), m.entryIndex
		}
	}

	// other wise call the new function
	return new(), m.entryIndex
}

type concurrentContextUse struct {
	entryType wire.Type
}

func (m *Machine) newConcurrentContextUse(entry wire.Type) *concurrentContextUse {
	c := &concurrentContextUse{entry}
	m.failure = c
	return c
}
