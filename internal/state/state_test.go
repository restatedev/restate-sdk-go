package state

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/generated/proto/protocol"
	"github.com/restatedev/sdk-go/internal/errors"
	"github.com/restatedev/sdk-go/internal/wire"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
)

type testParams struct {
	input  chan<- wire.Message
	output <-chan wire.Message
	wait   func() error
	cancel func()
}

var clientDisconnectError = fmt.Errorf("client disconnected")

func testHandler(handler restate.Handler) testParams {
	machine := NewMachine(handler, nil, nil)
	inputC := make(chan wire.Message)
	outputC := make(chan wire.Message)
	ctx, cancel := context.WithCancelCause(context.Background())
	machine.protocol = mockProtocol{input: inputC, output: outputC}

	eg := errgroup.Group{}
	eg.Go(func() error {
		return machine.Start(ctx, false, slog.Default().Handler())
	})

	return testParams{inputC, outputC, eg.Wait, func() { cancel(clientDisconnectError) }}
}

// closed request body should lead to suspension the next time we need a completion or ack
func TestRequestClosed(t *testing.T) {
	var ctxErr error
	var seenPanic any
	var tp testParams
	tp = testHandler(restate.NewServiceHandler(func(ctx restate.Context, _ restate.Void) (restate.Void, error) {
		close(tp.input)

		// writing out journal entries still works - this shouldnt panic
		after := ctx.After(time.Minute)

		ctxErr = ctx.Err()

		func() {
			defer func() {
				seenPanic = recover()
				if seenPanic != nil {
					panic(seenPanic)
				}
			}()

			// this should panic as it needs a completion
			after.Done()
		}()

		return restate.Void{}, nil
	}))

	tp.input <- &wire.StartMessage{
		StartMessage: protocol.StartMessage{
			Id:           []byte("abc"),
			DebugId:      "abc",
			KnownEntries: 1,
		},
	}
	tp.input <- &wire.InputEntryMessage{InputEntryMessage: protocol.InputEntryMessage{}}

	_ = <-tp.output // sleep
	_ = <-tp.output // suspension

	require.NoError(t, tp.wait())
	require.NoError(t, ctxErr, "invocation context was cancelled")
	require.IsType(t, &wire.SuspensionPanic{}, seenPanic, "awaiting the sleep didn't create suspension panic")
}

// closed http2 context (ie, client went away) should cancel the context provided and will lead to a panic on the
// next operation (write or await on previous entry)
func TestResponseClosed(t *testing.T) {
	type test struct {
		name            string
		beforeCancel    func(ctx restate.Context) any
		producedEntries int
		afterCancel     func(ctx restate.Context, setupState any)
		expectedPanic   any
	}

	tests := []test{
		{
			name: "awakeable should lead to client gone away panic",
			afterCancel: func(ctx restate.Context, _ any) {
				ctx.Awakeable()
			},
			expectedPanic: &clientGoneAway{},
		},
		{
			name: "starting run should lead to client gone away panic",
			afterCancel: func(ctx restate.Context, _ any) {
				ctx.Run(func(ctx restate.RunContext) (any, error) {
					panic("run should not be executed")
				}, restate.Void{})
			},
			expectedPanic: &clientGoneAway{},
		},
		{
			name: "awaiting sleep should lead to suspension panic",
			beforeCancel: func(ctx restate.Context) any {
				return ctx.After(time.Minute)
			},
			afterCancel: func(ctx restate.Context, setupState any) {
				setupState.(restate.After).Done()
			},
			producedEntries: 1,
			expectedPanic:   &wire.SuspensionPanic{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var tp testParams
			var ctxErr error
			var seenPanic any
			var state any
			tp = testHandler(restate.NewServiceHandler(func(ctx restate.Context, _ restate.Void) (restate.Void, error) {
				if test.beforeCancel != nil {
					state = test.beforeCancel(ctx)
				}

				tp.cancel()

				ctxErr = ctx.Err()

				func() {
					defer func() {
						seenPanic = recover()
						if seenPanic != nil {
							panic(seenPanic)
						}
					}()

					test.afterCancel(ctx, state)
				}()

				return restate.Void{}, nil
			}))
			tp.input <- &wire.StartMessage{
				StartMessage: protocol.StartMessage{
					Id:           []byte("abc"),
					DebugId:      "abc",
					KnownEntries: 1,
				},
			}
			tp.input <- &wire.InputEntryMessage{InputEntryMessage: protocol.InputEntryMessage{}}
			for i := 0; i < test.producedEntries; i++ {
				<-tp.output
			}

			require.NoError(t, tp.wait())
			require.Equal(t, context.Canceled, ctxErr, "invocation context wasnt cancelled")
			require.IsType(t, test.expectedPanic, seenPanic, "unexpected panic")
		})
	}
}

// disconnect mid-run should cancel the run context and panic with a write error
func TestInFlightRunDisconnect(t *testing.T) {
	var beforeCancelErr, afterCancelErr error
	var seenPanic any
	var tp testParams
	tp = testHandler(restate.NewServiceHandler(func(ctx restate.Context, _ restate.Void) (restate.Void, error) {
		func() {
			defer func() {
				seenPanic = recover()
				if seenPanic != nil {
					panic(seenPanic)
				}
			}()

			_ = ctx.Run(func(ctx restate.RunContext) (any, error) {
				beforeCancelErr = ctx.Err()
				tp.cancel()
				afterCancelErr = ctx.Err()

				return nil, nil
			}, restate.Void{})
		}()

		return restate.Void{}, nil
	}))

	tp.input <- &wire.StartMessage{
		StartMessage: protocol.StartMessage{
			Id:           []byte("abc"),
			DebugId:      "abc",
			KnownEntries: 1,
		},
	}
	tp.input <- &wire.InputEntryMessage{InputEntryMessage: protocol.InputEntryMessage{}}

	require.NoError(t, tp.wait())
	require.Nil(t, beforeCancelErr, "run context should not be cancelled early")
	require.Equal(t, context.Canceled, afterCancelErr, "run context should be cancelled")
	require.IsType(t, &clientGoneAway{}, seenPanic, "after the run should lead to a client gone away panic")
}

// suspension mid-run should commit the run result to the runtime, but then panic with suspension when
// trying to get the ack.
func TestInFlightRunSuspension(t *testing.T) {
	var beforeCancelErr, afterCancelErr error
	var seenPanic any
	var tp testParams
	tp = testHandler(restate.NewServiceHandler(func(ctx restate.Context, _ restate.Void) (restate.Void, error) {
		func() {
			defer func() {
				seenPanic = recover()
				if seenPanic != nil {
					panic(seenPanic)
				}
			}()

			_ = ctx.Run(func(ctx restate.RunContext) (any, error) {
				beforeCancelErr = ctx.Err()
				close(tp.input)
				afterCancelErr = ctx.Err()

				return nil, nil
			}, restate.Void{})
		}()

		return restate.Void{}, nil
	}))

	tp.input <- &wire.StartMessage{
		StartMessage: protocol.StartMessage{
			Id:           []byte("abc"),
			DebugId:      "abc",
			KnownEntries: 1,
		},
	}
	tp.input <- &wire.InputEntryMessage{InputEntryMessage: protocol.InputEntryMessage{}}

	<-tp.output // run
	<-tp.output // output

	require.NoError(t, tp.wait())
	require.Nil(t, beforeCancelErr, "run context should not be cancelled before request closed")
	require.Nil(t, afterCancelErr, "run context should not be cancelled after request closed")
	require.IsType(t, &wire.SuspensionPanic{}, seenPanic, "after the run should lead to a suspension panic")
}

func TestInvocationCanceled(t *testing.T) {
	type test struct {
		name string
		fn   func(ctx restate.Context) error
	}

	tests := []test{
		{
			name: "awakeable should return canceled error",
			fn: func(ctx restate.Context) error {
				awakeable := ctx.Awakeable()
				return awakeable.Result(restate.Void{})
			},
		},
		{
			name: "sleep should return canceled error",
			fn: func(ctx restate.Context) error {
				after := ctx.After(time.Minute)
				return after.Done()
			},
		},
		{
			name: "call should return cancelled error",
			fn: func(ctx restate.Context) error {
				fut, err := ctx.Service("foo", "bar").RequestFuture(restate.Void{})
				if err != nil {
					return err
				}
				return fut.Response(restate.Void{})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var seenErr error
			tp := testHandler(restate.NewServiceHandler(func(ctx restate.Context, _ restate.Void) (restate.Void, error) {
				seenErr = test.fn(ctx)
				return restate.Void{}, seenErr
			}))
			tp.input <- &wire.StartMessage{
				StartMessage: protocol.StartMessage{
					Id:           []byte("abc"),
					DebugId:      "abc",
					KnownEntries: 1,
				},
			}
			tp.input <- &wire.InputEntryMessage{InputEntryMessage: protocol.InputEntryMessage{}}
			entry := <-tp.output // awakeable, sleep, or call entry
			require.Implements(t, (*wire.CompleteableMessage)(nil), entry)

			// complete it with a cancellation
			tp.input <- &wire.CompletionMessage{CompletionMessage: protocol.CompletionMessage{
				EntryIndex: 1,
				Result: &protocol.CompletionMessage_Failure{
					Failure: &protocol.Failure{
						Code:    409,
						Message: "canceled",
					},
				},
			}}

			<-tp.output // output
			<-tp.output // end

			require.NoError(t, tp.wait())
			require.Equal(t, &errors.CodeError{
				Code: 409,
				Inner: &errors.TerminalError{
					Inner: fmt.Errorf("canceled"),
				},
			}, seenErr)
		})
	}
}

type mockProtocol struct {
	input  <-chan wire.Message
	output chan<- wire.Message
}

var _ wire.Protocol = mockProtocol{}

func (m mockProtocol) Read() (wire.Message, wire.Type, error) {
	msg, ok := <-m.input
	if !ok {
		return nil, 0, io.EOF
	}

	return msg, wire.MessageType(msg), nil
}

func (m mockProtocol) Write(_ wire.Type, message wire.Message) error {
	m.output <- message
	return nil
}
