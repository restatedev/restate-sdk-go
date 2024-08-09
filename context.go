package restate

import (
	"context"
	"log/slog"
	"time"

	"github.com/restatedev/sdk-go/internal/futures"
	"github.com/restatedev/sdk-go/internal/options"
	"github.com/restatedev/sdk-go/internal/rand"
)

// Context is the base set of operations that all Restate handlers may perform.
type Context interface {
	RunContext

	// Rand returns a random source which will give deterministic results for a given invocation
	// The source wraps the stdlib rand.Rand but with some extra helper methods
	// This source is not safe for use inside .Run()
	Rand() *rand.Rand

	// Sleep for the duration d. Can return a terminal error in the case where the invocation was cancelled mid-sleep.
	Sleep(d time.Duration) error
	// After is an alternative to Context.Sleep which allows you to complete other tasks concurrently
	// with the sleep. This is particularly useful when combined with Context.Select to race between
	// the sleep and other Selectable operations.
	After(d time.Duration) After

	// Service gets a Service accessor by service and method name
	// Note: use the CallAs helper function to deserialise return values
	Service(service, method string, opts ...options.CallOption) CallClient

	// Object gets a Object accessor by name, key and method name
	// Note: use the CallAs helper function to receive serialised values
	Object(object, key, method string, opts ...options.CallOption) CallClient

	// Run runs the function (fn), storing final results (including terminal errors)
	// durably in the journal, or otherwise for transient errors stopping execution
	// so Restate can retry the invocation. Replays will produce the same value, so
	// all non-deterministic operations (eg, generating a unique ID) *must* happen
	// inside Run blocks.
	// Note: use the RunAs helper function to get typed output values instead of providing an output pointer
	Run(fn func(ctx RunContext) (any, error), output any, opts ...options.RunOption) error

	// Awakeable returns a Restate awakeable; a 'promise' to a future
	// value or error, that can be resolved or rejected by other services.
	// Note: use the AwakeableAs helper function to avoid having to pass a output pointer to Awakeable.Result()
	Awakeable(options ...options.AwakeableOption) Awakeable
	// ResolveAwakeable allows an awakeable (not necessarily from this service) to be
	// resolved with a particular value.
	ResolveAwakeable(id string, value any, options ...options.ResolveAwakeableOption) error
	// ResolveAwakeable allows an awakeable (not necessarily from this service) to be
	// rejected with a particular error.
	RejectAwakeable(id string, reason error)

	// Select returns an iterator over blocking Restate operations (sleep, call, awakeable)
	// which allows you to safely run them in parallel. The Selector will store the order
	// that things complete in durably inside Restate, so that on replay the same order
	// can be used. This avoids non-determinism. It is *not* safe to use goroutines or channels
	// outside of Context.Run functions, as they do not behave deterministically.
	Select(futs ...Selectable) Selector
}

// Selectable is implemented by types that may be passed to Context.Select
type Selectable = futures.Selectable

// Awakeable is the Go representation of a Restate awakeable; a 'promise' to a future
// value or error, that can be resolved or rejected by other services.
type Awakeable interface {
	// Id returns the awakeable ID, which can be stored or sent to a another service
	Id() string
	// Result blocks on receiving the result of the awakeable, storing the value it was
	// resolved with in output or otherwise returning the error it was rejected with.
	// It is *not* safe to call this in a goroutine - use Context.Select if you
	// want to wait on multiple results at once.
	// Note: use the AwakeableAs helper function to avoid having to pass a output pointer
	Result(output any) error
	Selectable
}

// CallClient represents all the different ways you can invoke a particular service/key/method tuple.
type CallClient interface {
	// RequestFuture makes a call and returns a handle on a future response
	RequestFuture(input any) (ResponseFuture, error)
	// Request makes a call and blocks on getting the response which is stored in output
	Request(input any, output any) error
	// Send makes a one-way call which is executed in the background
	Send(input any, delay time.Duration) error
}

// ResponseFuture is a handle on a potentially not-yet completed outbound call.
type ResponseFuture interface {
	// Response blocks on the response to the call and stores it in output, or returns the associated error
	// It is *not* safe to call this in a goroutine - use Context.Select if you
	// want to wait on multiple results at once.
	Response(output any) error
	Selectable
}

// Selector is an iterator over a list of blocking Restate operations that are running
// in the background.
type Selector interface {
	// Remaining returns whether there are still operations that haven't been returned by Select().
	// There will always be exactly the same number of results as there were operations
	// given to Context.Select
	Remaining() bool
	// Select blocks on the next completed operation or returns nil if there are none left
	Select() Selectable
}

// RunContext methods are the only methods of [Context] that are safe to call from inside a .Run()
// Calling any other method inside a Run() will panic.
type RunContext interface {
	context.Context

	// Log obtains a handle on a slog.Logger which already has some useful fields (invocationID and method)
	// By default, this logger will not output messages if the invocation is currently replaying
	// The log handler can be set with `.WithLogger()` on the server object
	Log() *slog.Logger

	// Request gives extra information about the request that started this invocation
	Request() *Request
}

type Request struct {
	// The unique id that identifies the current function invocation. This id is guaranteed to be
	// unique across invocations, but constant across reties and suspensions.
	ID []byte
	// Request headers - the following headers capture the original invocation headers, as provided to
	// the ingress.
	Headers map[string]string
	// Attempt headers - the following headers are sent by the restate runtime.
	// These headers are attempt specific, generated by the restate runtime uniquely for each attempt.
	// These headers might contain information such as the W3C trace context, and attempt specific information.
	AttemptHeaders map[string][]string
	// Raw unparsed request body
	Body []byte
}

// After is a handle on a Sleep operation which allows you to do other work concurrently
// with the sleep.
type After interface {
	// Done blocks waiting on the remaining duration of the sleep.
	// It is *not* safe to call this in a goroutine - use Context.Select if you want to wait on multiple
	// results at once. Can return a terminal error in the case where the invocation was cancelled mid-sleep,
	// hence Done() should always be called, even after using Context.Select.
	Done() error
	Selectable
}

// ObjectContext is an extension of [Context] which can be used in exclusive-mode Virtual Object handlers,
// giving mutable access to state.
type ObjectContext interface {
	Context
	KeyValueReader
	KeyValueWriter
}

// ObjectContext is an extension of [Context] which can be used in shared-mode Virtual Object handlers,
// giving read-only access to a snapshot of state.
type ObjectSharedContext interface {
	Context
	KeyValueReader
}

// KeyValueReader is the set of read-only methods which can be used in all Virtual Object handlers.
type KeyValueReader interface {
	// Get gets value associated with key and stores it in value
	// If key does not exist, this function returns ErrKeyNotFound
	// Note: Use GetAs generic helper function to avoid passing in a value pointer
	Get(key string, value any, options ...options.GetOption) error
	// Keys returns a list of all associated key
	Keys() []string
	// Key retrieves the key for this virtual object invocation. This is a no-op and is
	// always safe to call.
	Key() string
}

// KeyValueWriter is the set of mutating methods which can be used in exclusive-mode Virtual Object handlers.
type KeyValueWriter interface {
	// Set sets a value against a key, using the provided codec (defaults to JSON)
	Set(key string, value any, options ...options.SetOption) error
	// Clear deletes a key
	Clear(key string)
	// ClearAll drops all stored state associated with key
	ClearAll()
}
