package main

import (
	"context"
	"log/slog"
	"os"

	restate "github.com/restatedev/sdk-go"
	"github.com/restatedev/sdk-go/server"
)

func main() {
	server := server.NewRestate().
		// Handlers can be inferred from object methods
		Bind(restate.Object(&userSession{})).
		Bind(restate.Object(&ticketService{})).
		Bind(restate.Service(&checkout{}))

	if err := server.Start(context.Background(), ":9080"); err != nil {
		slog.Error("application exited unexpectedly", "err", err.Error())
		os.Exit(1)
	}
}
