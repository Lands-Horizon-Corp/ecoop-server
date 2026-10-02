package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/Lands-Horizon-Corp/ecoop-server/pkg/services/logger"
	"github.com/joho/godotenv"
)

func main() {
	os.Exit(run())
}

func run() (exitCode int) {
	_ = godotenv.Load()

	l := logger.NewLogContextService("sample", "", "")
	if err := l.Start(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "start logger:", err)
		return 1
	}
	defer func() {
		if err := l.Stop(context.Background()); err != nil {
			fmt.Fprintln(os.Stderr, "stop logger:", err)
			exitCode = 1
		}
	}()

	c, p := l.Trace("demo.checkout")
	defer p.Span().End()
	p.Info("checkout started")
	if err := apiHandler(c); err != nil {
		return 1
	}

	return 0
}

func apiHandler(ctx logger.LogContextService) error {
	c, p := ctx.Trace("demo.apiHandler")
	defer p.Span().End()

	p.Info("apiHandler called")
	return authCheck(c)
}

func authCheck(ctx logger.LogContextService) error {
	c, p := ctx.Trace("demo.authCheck")
	defer p.Span().End()

	p.Info("authCheck called")
	return orderValidate(c)
}

func orderValidate(ctx logger.LogContextService) error {
	c, p := ctx.Trace("demo.orderValidate")
	defer p.Span().End()

	p.Info("orderValidate called")
	return inventoryReserve(c)
}

func inventoryReserve(ctx logger.LogContextService) error {
	c, p := ctx.Trace("demo.inventoryReserve")
	defer p.Span().End()
	p.Info("inventoryReserve called")
	return paymentCharge(c)
}

func paymentCharge(ctx logger.LogContextService) error {
	c, p := ctx.Trace("demo.paymentCharge")
	defer p.Span().End()
	p.Info("paymentCharge called")
	return gatewayCall(c)
}

func gatewayCall(ctx logger.LogContextService) error {
	c, p := ctx.Trace("demo.gatewayCall")
	defer p.Span().End()
	p.Info("gatewayCall called")
	return ledgerWrite(c)
}

func ledgerWrite(ctx logger.LogContextService) error {
	c, p := ctx.Trace("demo.ledgerWrite")
	defer p.Span().End()
	p.Info("ledgerWrite called")
	return dbExec(c)
}

func dbExec(ctx logger.LogContextService) error {
	_, p := ctx.Trace("demo.dbExec")
	defer p.Span().End()

	err := errors.New("duplicate key value violates unique constraint")
	err = fmt.Errorf("db exec: %w", err)

	p.Fatal(err, "database transaction failed")
	return err
}
