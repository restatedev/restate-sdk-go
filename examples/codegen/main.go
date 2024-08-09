package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"

	restate "github.com/restatedev/sdk-go"
	helloworld "github.com/restatedev/sdk-go/examples/codegen/proto"
	"github.com/restatedev/sdk-go/server"
)

type greeter struct {
	helloworld.UnimplementedGreeterServer
}

func (greeter) SayHello(ctx restate.Context, req *helloworld.HelloRequest) (*helloworld.HelloResponse, error) {
	counter := helloworld.NewCounterClient(ctx, req.Name)
	count, err := counter.Add().
		Request(&helloworld.AddRequest{Delta: 1})
	if err != nil {
		return nil, err
	}
	return &helloworld.HelloResponse{
		Message: fmt.Sprintf("Hello, %s! Call number: %d", req.Name, count.Value),
	}, nil
}

type counter struct {
	helloworld.UnimplementedCounterServer
}

func (c counter) Add(ctx restate.ObjectContext, req *helloworld.AddRequest) (*helloworld.GetResponse, error) {
	count, err := restate.GetAs[int64](ctx, "counter")
	if err != nil && !errors.Is(err, restate.ErrKeyNotFound) {
		return nil, err
	}

	count += 1
	if err := ctx.Set("counter", count); err != nil {
		return nil, err
	}

	return &helloworld.GetResponse{Value: count}, nil
}

func (c counter) Get(ctx restate.ObjectSharedContext, _ *helloworld.GetRequest) (*helloworld.GetResponse, error) {
	count, err := restate.GetAs[int64](ctx, "counter")
	if err != nil && !errors.Is(err, restate.ErrKeyNotFound) {
		return nil, err
	}

	return &helloworld.GetResponse{Value: count}, nil
}

func main() {
	server := server.NewRestate().
		Bind(helloworld.NewGreeterServer(greeter{})).
		Bind(helloworld.NewCounterServer(counter{}))

	if err := server.Start(context.Background(), ":9080"); err != nil {
		slog.Error("application exited unexpectedly", "err", err.Error())
		os.Exit(1)
	}
}
