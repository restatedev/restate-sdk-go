package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"runtime/debug"
	"strings"

	restate "github.com/restatedev/sdk-go"
	protocol "github.com/restatedev/sdk-go/generated/dev/restate/service"
	"github.com/restatedev/sdk-go/internal"
	"github.com/restatedev/sdk-go/internal/identity"
	"github.com/restatedev/sdk-go/internal/log"
	"github.com/restatedev/sdk-go/internal/state"
	"golang.org/x/net/http2"
)

const minServiceProtocolVersion protocol.ServiceProtocolVersion = protocol.ServiceProtocolVersion_V1
const maxServiceProtocolVersion protocol.ServiceProtocolVersion = protocol.ServiceProtocolVersion_V1
const minServiceDiscoveryProtocolVersion protocol.ServiceDiscoveryProtocolVersion = protocol.ServiceDiscoveryProtocolVersion_V1
const maxServiceDiscoveryProtocolVersion protocol.ServiceDiscoveryProtocolVersion = protocol.ServiceDiscoveryProtocolVersion_V1

var xRestateServer = `restate-sdk-go/unknown`

func init() {
	bi, ok := debug.ReadBuildInfo()
	if !ok {
		return
	}
	for _, dep := range bi.Deps {
		if dep.Path == "github.com/restatedev/sdk-go" {
			xRestateServer = "restate-sdk-go/" + dep.Version
			break
		}
	}
}

// Restate represents a Restate HTTP handler to which services or virtual objects may be attached.
type Restate struct {
	logHandler     slog.Handler
	dropReplayLogs bool
	systemLog      *slog.Logger
	definitions    map[string]restate.ServiceDefinition
	keyIDs         []string
	keySet         identity.KeySetV1
	protocolMode   internal.ProtocolMode
}

// NewRestate creates a new instance of Restate server
func NewRestate() *Restate {
	handler := slog.Default().Handler()
	return &Restate{
		logHandler:     handler,
		systemLog:      slog.New(log.NewRestateContextHandler(handler)),
		dropReplayLogs: true,
		definitions:    make(map[string]restate.ServiceDefinition),
		protocolMode:   internal.ProtocolMode_BIDI_STREAM,
	}
}

// WithLogger overrides the slog handler used by the SDK (which defaults to the slog Default())
// You may specify with dropReplayLogs whether to drop logs that originated from handler code
// while the invocation was replaying. If they are not dropped, you may still determine the replay
// status in a slog.Handler using [github.com/restatedev/sdk-go/rcontext.LogContextFrom]
func (r *Restate) WithLogger(h slog.Handler, dropReplayLogs bool) *Restate {
	r.dropReplayLogs = dropReplayLogs
	r.systemLog = slog.New(log.NewRestateContextHandler(h))
	r.logHandler = h
	return r
}

// WithIdentityV1 attaches v1 request identity public keys to this server. All incoming requests will be validated
// against one of these keys.
func (r *Restate) WithIdentityV1(keys ...string) *Restate {
	r.keyIDs = append(r.keyIDs, keys...)
	return r
}

// Bidirectional is used to change the protocol mode advertised to Restate on discovery
// In bidirectional mode, Restate will keep the request body open even after we have started to respond,
// allowing for more work to be done without suspending.
// This is supported over HTTP2 and, in some cases (where there is no buffering proxy), with HTTP1.1.
// When serving over a non-bidirectional channel (eg, Lambda), use .WithBidirectional(false) otherwise your handlers may get stuck.
func (r *Restate) Bidirectional(bidi bool) *Restate {
	if bidi {
		r.protocolMode = internal.ProtocolMode_BIDI_STREAM
	} else {
		r.protocolMode = internal.ProtocolMode_REQUEST_RESPONSE
	}
	return r
}

// Bind attaches a Service Definition (a Service or Virtual Object) to this server
func (r *Restate) Bind(definition restate.ServiceDefinition) *Restate {
	if _, ok := r.definitions[definition.Name()]; ok {
		// panic because this is a programming error
		// to register multiple definitions with the same name
		panic("service definition with the same name exists")
	}

	r.definitions[definition.Name()] = definition

	return r
}

func (r *Restate) discover() (resource *internal.Endpoint, err error) {
	resource = &internal.Endpoint{
		ProtocolMode:       r.protocolMode,
		MinProtocolVersion: int32(minServiceProtocolVersion),
		MaxProtocolVersion: int32(maxServiceDiscoveryProtocolVersion),
		Services:           make([]internal.Service, 0, len(r.definitions)),
	}

	for name, definition := range r.definitions {
		service := internal.Service{
			Name:     name,
			Ty:       definition.Type(),
			Handlers: make([]internal.Handler, 0, len(definition.Handlers())),
		}

		for name, handler := range definition.Handlers() {
			service.Handlers = append(service.Handlers, internal.Handler{
				Name:   name,
				Input:  handler.InputPayload(),
				Output: handler.OutputPayload(),
				Ty:     handler.HandlerType(),
			})
		}
		resource.Services = append(resource.Services, service)
	}

	return
}

func (r *Restate) discoverHandler(writer http.ResponseWriter, req *http.Request) {
	r.systemLog.DebugContext(req.Context(), "Processing discovery request")

	acceptVersionsString := req.Header.Get("accept")
	if acceptVersionsString == "" {
		writer.WriteHeader(http.StatusUnsupportedMediaType)
		writer.Write([]byte("missing accept header"))

		return
	}

	serviceDiscoveryProtocolVersion := selectSupportedServiceDiscoveryProtocolVersion(acceptVersionsString)

	if serviceDiscoveryProtocolVersion == protocol.ServiceDiscoveryProtocolVersion_SERVICE_DISCOVERY_PROTOCOL_VERSION_UNSPECIFIED {
		writer.WriteHeader(http.StatusUnsupportedMediaType)
		writer.Write([]byte(fmt.Sprintf("Unsupported service discovery protocol version '%s'", acceptVersionsString)))
		return
	}

	response, err := r.discover()
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))

		return
	}

	bytes, err := json.Marshal(response)
	if err != nil {
		writer.WriteHeader(http.StatusInternalServerError)
		writer.Write([]byte(err.Error()))

		return
	}

	writer.Header().Add("x-restate-server", xRestateServer)
	writer.Header().Add("Content-Type", serviceDiscoveryProtocolVersionToHeaderValue(serviceDiscoveryProtocolVersion))
	if _, err := writer.Write(bytes); err != nil {
		r.systemLog.LogAttrs(req.Context(), slog.LevelError, "Failed to write discovery information", log.Error(err))
	}
}

func selectSupportedServiceDiscoveryProtocolVersion(accept string) protocol.ServiceDiscoveryProtocolVersion {
	maxVersion := protocol.ServiceDiscoveryProtocolVersion_SERVICE_DISCOVERY_PROTOCOL_VERSION_UNSPECIFIED

	for _, versionString := range strings.Split(accept, ",") {
		version := parseServiceDiscoveryProtocolVersion(versionString)
		if isServiceDiscoveryProtocolVersionSupported(version) && version > maxVersion {
			maxVersion = version
		}
	}

	return maxVersion
}

func parseServiceDiscoveryProtocolVersion(versionString string) protocol.ServiceDiscoveryProtocolVersion {
	if strings.TrimSpace(versionString) == "application/vnd.restate.endpointmanifest.v1+json" {
		return protocol.ServiceDiscoveryProtocolVersion_V1
	}

	return protocol.ServiceDiscoveryProtocolVersion_SERVICE_DISCOVERY_PROTOCOL_VERSION_UNSPECIFIED
}

func isServiceDiscoveryProtocolVersionSupported(version protocol.ServiceDiscoveryProtocolVersion) bool {
	return version >= minServiceDiscoveryProtocolVersion && version <= maxServiceDiscoveryProtocolVersion
}

func serviceDiscoveryProtocolVersionToHeaderValue(serviceDiscoveryProtocolVersion protocol.ServiceDiscoveryProtocolVersion) string {
	switch serviceDiscoveryProtocolVersion {
	case protocol.ServiceDiscoveryProtocolVersion_V1:
		return "application/vnd.restate.endpointmanifest.v1+json"
	}
	panic(fmt.Sprintf("unexpected service discovery protocol version %d", serviceDiscoveryProtocolVersion))
}

func parseServiceProtocolVersion(versionString string) protocol.ServiceProtocolVersion {
	if strings.TrimSpace(versionString) == "application/vnd.restate.invocation.v1" {
		return protocol.ServiceProtocolVersion_V1
	}

	return protocol.ServiceProtocolVersion_SERVICE_PROTOCOL_VERSION_UNSPECIFIED
}

func isServiceProtocolVersionSupported(version protocol.ServiceProtocolVersion) bool {
	return version >= minServiceProtocolVersion && version <= maxServiceProtocolVersion
}

func serviceProtocolVersionToHeaderValue(serviceProtocolVersion protocol.ServiceProtocolVersion) string {
	switch serviceProtocolVersion {
	case protocol.ServiceProtocolVersion_V1:
		return "application/vnd.restate.invocation.v1"
	}
	panic(fmt.Sprintf("unexpected service protocol version %d", serviceProtocolVersion))
}

type serviceMethod struct {
	service string
	method  string
}

// takes care of function call
func (r *Restate) callHandler(serviceProtocolVersion protocol.ServiceProtocolVersion, service, method string, writer http.ResponseWriter, request *http.Request) {
	logger := r.systemLog.With("method", slog.StringValue(fmt.Sprintf("%s/%s", service, method)))

	writer.Header().Add("x-restate-server", xRestateServer)
	writer.Header().Add("content-type", serviceProtocolVersionToHeaderValue(serviceProtocolVersion))

	definition, ok := r.definitions[service]
	if !ok {
		logger.WarnContext(request.Context(), "Service not found")
		writer.WriteHeader(http.StatusNotFound)
		return
	}
	handler, ok := definition.Handlers()[method]
	if !ok {
		logger.WarnContext(request.Context(), "Method not found on service")
		writer.WriteHeader(http.StatusNotFound)
	}

	writer.WriteHeader(200)

	conn := newConnection(writer, request)

	defer conn.Close()

	machine := state.NewMachine(handler, conn, request.Header)

	if err := machine.Start(request.Context(), r.dropReplayLogs, r.logHandler); err != nil {
		r.systemLog.LogAttrs(request.Context(), slog.LevelError, "Failed to handle invocation", log.Error(err))
	}
}

func (r *Restate) handler(writer http.ResponseWriter, request *http.Request) {
	if r.keySet != nil {
		if err := identity.ValidateRequestIdentity(r.keySet, request.RequestURI, request.Header); err != nil {
			r.systemLog.LogAttrs(request.Context(), slog.LevelError, "Rejecting request as its JWT did not validate", log.Error(err))

			writer.WriteHeader(http.StatusUnauthorized)
			writer.Write([]byte("Unauthorized"))

			return
		}
	}

	if request.RequestURI == "/discover" {
		r.discoverHandler(writer, request)
		return
	}

	if r.protocolMode == internal.ProtocolMode_BIDI_STREAM && !request.ProtoAtLeast(2, 0) {
		// bidi http1.1 requires enabling full duplex
		rc := http.NewResponseController(writer)
		if err := rc.EnableFullDuplex(); err != nil {
			r.systemLog.LogAttrs(request.Context(), slog.LevelError, "Could not enable full duplex mode on the underlying HTTP1 transport, server must be created with .Bidirectional(false)", log.Error(err))

			writer.WriteHeader(http.StatusInternalServerError)
			writer.Write([]byte("BIDI_STREAM not supported"))

			return
		}
	}

	serviceProtocolVersionString := request.Header.Get("content-type")
	if serviceProtocolVersionString == "" {
		r.systemLog.ErrorContext(request.Context(), "Missing content-type header")

		writer.WriteHeader(http.StatusUnsupportedMediaType)
		writer.Write([]byte("missing content-type header"))

		return
	}

	serviceProtocolVersion := parseServiceProtocolVersion(serviceProtocolVersionString)

	if !isServiceProtocolVersionSupported(serviceProtocolVersion) {
		r.systemLog.LogAttrs(request.Context(), slog.LevelError, "Unsupported service protocol version", slog.String("version", serviceProtocolVersionString))

		writer.WriteHeader(http.StatusUnsupportedMediaType)
		writer.Write([]byte(fmt.Sprintf("Unsupported service protocol version '%s'", serviceProtocolVersionString)))

		return
	}

	// we expecting the uri to be something like `/invoke/{service}/{method}`
	// so
	if !strings.HasPrefix(request.RequestURI, "/invoke/") {
		r.systemLog.LogAttrs(request.Context(), slog.LevelError, "Invalid request path", slog.String("path", request.RequestURI))
		writer.WriteHeader(http.StatusNotFound)
		return
	}

	parts := strings.Split(strings.TrimPrefix(request.RequestURI, "/invoke/"), "/")
	if len(parts) != 2 {
		r.systemLog.LogAttrs(request.Context(), slog.LevelError, "Invalid request path", slog.String("path", request.RequestURI))
		writer.WriteHeader(http.StatusNotFound)

		return
	}

	r.callHandler(serviceProtocolVersion, parts[0], parts[1], writer, request)
}

// Handler obtains a [http.HandlerFunc] representing the bound services which can be passed to other types of server.
// Ensure that .Bidirectional(false) is set when serving over a channel that doesn't support full-duplex request and response.
func (r *Restate) Handler() (http.HandlerFunc, error) {
	if r.keyIDs == nil {
		r.systemLog.Warn("Accepting requests without validating request signatures; handler access must be restricted")
	} else {
		ks, err := identity.ParseKeySetV1(r.keyIDs)
		if err != nil {
			return nil, fmt.Errorf("invalid request identity keys: %w", err)
		}
		r.keySet = ks
		r.systemLog.Info("Validating requests using signing keys", "keys", r.keyIDs)
	}

	return http.HandlerFunc(r.handler), nil
}

// Start starts a HTTP2 server serving the bound services
func (r *Restate) Start(ctx context.Context, address string) error {
	handler, err := r.Handler()
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen on address %s: %w", address, err)
	}

	var h2server http2.Server

	opts := &http2.ServeConnOpts{
		Context: ctx,
		Handler: handler,
	}

	for {
		con, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		go h2server.ServeConn(con, opts)
	}
}
