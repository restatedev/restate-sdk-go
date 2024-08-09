package main

import (
	"errors"

	restate "github.com/restatedev/sdk-go"
)

type TicketStatus int

const (
	TicketAvailable TicketStatus = 0
	TicketReserved  TicketStatus = 1
	TicketSold      TicketStatus = 2
)

const TicketServiceName = "TicketService"

type ticketService struct{}

func (t *ticketService) ServiceName() string { return TicketServiceName }

func (t *ticketService) Reserve(ctx restate.ObjectContext, _ restate.Void) (bool, error) {
	status, err := restate.GetAs[TicketStatus](ctx, "status")
	if err != nil && !errors.Is(err, restate.ErrKeyNotFound) {
		return false, err
	}

	if status == TicketAvailable {
		return true, ctx.Set("status", TicketReserved)
	}

	return false, nil
}

func (t *ticketService) Unreserve(ctx restate.ObjectContext, _ restate.Void) (void restate.Void, err error) {
	ticketId := ctx.Key()
	ctx.Log().Info("un-reserving ticket", "ticket", ticketId)
	status, err := restate.GetAs[TicketStatus](ctx, "status")
	if err != nil && !errors.Is(err, restate.ErrKeyNotFound) {
		return void, err
	}

	if status != TicketSold {
		ctx.Clear("status")
		return void, nil
	}

	return void, nil
}

func (t *ticketService) MarkAsSold(ctx restate.ObjectContext, _ restate.Void) (void restate.Void, err error) {
	ticketId := ctx.Key()
	ctx.Log().Info("mark ticket as sold", "ticket", ticketId)

	status, err := restate.GetAs[TicketStatus](ctx, "status")
	if err != nil && !errors.Is(err, restate.ErrKeyNotFound) {
		return void, err
	}

	if status == TicketReserved {
		return void, ctx.Set("status", TicketSold)
	}

	return void, nil
}

func (t *ticketService) Status(ctx restate.ObjectSharedContext, _ restate.Void) (TicketStatus, error) {
	ticketId := ctx.Key()
	ctx.Log().Info("mark ticket as sold", "ticket", ticketId)

	status, err := restate.GetAs[TicketStatus](ctx, "status")
	if err != nil && !errors.Is(err, restate.ErrKeyNotFound) {
		return status, err
	}

	return status, nil
}
