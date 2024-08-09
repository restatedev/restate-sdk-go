package state

import (
	"context"
	"log/slog"

	"github.com/restatedev/sdk-go/internal/log"
	"github.com/restatedev/sdk-go/internal/wire"
)

func (m *Machine) completable(entryIndex uint32) wire.CompleteableMessage {
	if entryIndex <= uint32(len(m.entries)) {
		// completion for replayed entry
		cm, ok := m.entries[entryIndex-1].(wire.CompleteableMessage)
		if !ok {
			return nil
		}
		return cm
	}

	// completion for an outgoing entry
	m.pendingMutex.RLock()
	defer m.pendingMutex.RUnlock()
	return m.pendingCompletions[entryIndex]
}

func (m *Machine) ackable(entryIndex uint32) wire.AckableMessage {
	m.pendingMutex.RLock()
	defer m.pendingMutex.RUnlock()
	return m.pendingAcks[entryIndex]
}

func (m *Machine) Write(message wire.Message) {
	if m.ctx.Err() != nil {
		// the main context being cancelled means the client is no longer interested in our response
		// and so creating new entries is pointless and we should shut down the state machine.
		panic(m.newClientGoneAway(context.Cause(m.ctx)))
	}
	if message, ok := message.(wire.CompleteableMessage); ok && !message.Completed() {
		m.pendingMutex.Lock()
		m.pendingCompletions[m.entryIndex] = message
		m.pendingMutex.Unlock()
	}
	if message, ok := message.(wire.AckableMessage); ok && !message.Acked() {
		m.pendingMutex.Lock()
		m.pendingAcks[m.entryIndex] = message
		m.pendingMutex.Unlock()
	}
	typ := wire.MessageType(message)
	m.log.LogAttrs(m.ctx, log.LevelTrace, "Sending message to runtime", log.Stringer("type", typ))
	if err := m.protocol.Write(typ, message); err != nil {
		panic(m.newWriteError(message, err))
	}
}

type writeError struct {
	entryIndex uint32
	entry      wire.Message
	err        error
}

func (m *Machine) newWriteError(entry wire.Message, err error) *writeError {
	w := &writeError{m.entryIndex, entry, err}
	m.failure = w
	return w
}

func (m *Machine) handleCompletionsAcks() {
	for {
		msg, _, err := m.protocol.Read()
		if err != nil {
			if m.ctx.Err() == nil {
				m.log.LogAttrs(m.ctx, log.LevelTrace, "Request body closed; next blocking operation will suspend")
				m.suspend(err)
			}
			return
		}
		switch msg := msg.(type) {
		case *wire.CompletionMessage:
			completable := m.completable(msg.EntryIndex)
			if completable == nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Failed to find pending completion at index", slog.Uint64("index", uint64(msg.EntryIndex)))
				continue
			}
			if err := completable.Complete(&msg.CompletionMessage); err != nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Failed to process completion", log.Error(err), slog.Uint64("index", uint64(msg.EntryIndex)))
			} else {
				m.log.LogAttrs(m.ctx, slog.LevelDebug, "Processed completion", slog.Uint64("index", uint64(msg.EntryIndex)))
			}
		case *wire.EntryAckMessage:
			ackable := m.ackable(msg.EntryIndex)
			if ackable == nil {
				m.log.LogAttrs(m.ctx, slog.LevelError, "Failed to find pending ack at index", slog.Uint64("index", uint64(msg.EntryIndex)))
				continue
			}
			ackable.Ack()
			m.log.LogAttrs(m.ctx, slog.LevelDebug, "Processed ack", slog.Uint64("index", uint64(msg.EntryIndex)))
		default:
			m.log.LogAttrs(m.ctx, slog.LevelError, "Unexpected non-completion non-ack message during invocation", log.Type("type", msg))
			continue
		}
	}
}
