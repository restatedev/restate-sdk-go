package main

import (
	"fmt"
	"math/rand"

	"github.com/google/uuid"
	restate "github.com/restatedev/sdk-go"
)

type PaymentRequest struct {
	UserID  string   `json:"userId"`
	Tickets []string `json:"tickets"`
}

type PaymentResponse struct {
	ID    string `json:"id"`
	Price int    `json:"price"`
}

type checkout struct{}

func (c *checkout) Name() string {
	return CheckoutServiceName
}

const CheckoutServiceName = "Checkout"

func (c *checkout) Payment(ctx restate.Context, request PaymentRequest) (response PaymentResponse, err error) {
	uuid, err := restate.RunAs(ctx, func(ctx restate.RunContext) (string, error) {
		uuid := uuid.New()
		return uuid.String(), nil
	})

	response.ID = uuid

	if err != nil {
		return response, err
	}

	// We are a uniform shop where everything costs 30 USD
	// that is cheaper than the official example :P
	price := len(request.Tickets) * 30

	response.Price = price
	_, err = restate.RunAs(ctx, func(ctx restate.RunContext) (bool, error) {
		log := ctx.Log().With("uuid", uuid, "price", price)
		if rand.Float64() < 0.5 {
			log.Info("payment succeeded")
			return true, nil
		} else {
			log.Error("payment failed")
			return false, fmt.Errorf("failed to pay")
		}
	})

	if err != nil {
		return response, err
	}

	// todo: send email

	return response, nil
}
