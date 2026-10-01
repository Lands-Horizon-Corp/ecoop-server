package main

import (
	"context"
	"errors"
	"fmt"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/joho/godotenv"
)

func main() {
	_ = godotenv.Load()

	l := logger.NewLogContextService("sample", "", "")
	if err := l.Start(context.Background()); err != nil {
		panic(err)
	}

	c, log, span := l.Trace("demo.checkout")
	defer span.End()
	log.Info("checkout started")
	_ = apiHandler(c)

	span.End()
	l.Stop(context.Background())
}

func apiHandler(ctx logger.LogContextService) error {
	c, log, span := ctx.Trace("demo.apiHandler")
	defer span.End()

	log.Info("apiHandler called")
	return authCheck(c)
}

func authCheck(ctx logger.LogContextService) error {
	c, log, span := ctx.Trace("demo.authCheck")
	defer span.End()

	log.Info("authCheck called")
	return orderValidate(c)
}

func orderValidate(ctx logger.LogContextService) error {
	c, log, span := ctx.Trace("demo.orderValidate")
	defer span.End()

	log.Info("orderValidate called")
	return inventoryReserve(c)
}

func inventoryReserve(ctx logger.LogContextService) error {
	c, _, span := ctx.Trace("demo.inventoryReserve")
	defer span.End()
	return paymentCharge(c)
}

func paymentCharge(ctx logger.LogContextService) error {
	c, _, span := ctx.Trace("demo.paymentCharge")
	defer span.End()
	return gatewayCall(c)
}

func gatewayCall(ctx logger.LogContextService) error {
	c, _, span := ctx.Trace("demo.gatewayCall")
	defer span.End()
	return ledgerWrite(c)
}

func ledgerWrite(ctx logger.LogContextService) error {
	c, _, span := ctx.Trace("demo.ledgerWrite")
	defer span.End()
	return dbExec(c)
}

func dbExec(ctx logger.LogContextService) error {
	_, log, span := ctx.Trace("demo.dbExec")
	defer span.End()

	err := errors.New("duplicate key value violates unique constraint")
	err = fmt.Errorf("db exec: %w", err)

	log.Error(err, "database transaction failed")
	return err
}
