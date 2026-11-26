package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	httpAdapter "github.com/Team-ReBOOT-Academy/Stockio/internal/adapters/http"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/adapters/kafka"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/config"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/usecases"
)

const version = "1.0.0"

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	cfg, err := config.Load()
	if err != nil {
		logger.Error("failed to load config", "error", err)
		os.Exit(1)
	}

	kafkaClient, err := kafka.NewClient(cfg, logger)
	if err != nil {
		logger.Error("failed to create kafka client", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	strategyService := usecases.NewStrategyService(cfg, kafkaClient, kafkaClient, logger)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errChan := make(chan error, 1)
	go func() {
		errChan <- strategyService.Run(ctx)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/health", httpAdapter.HealthHandler(cfg.ServiceName, version))

	server := &http.Server{
		Addr:    cfg.HTTPPort,
		Handler: mux,
	}

	go func() {
		logger.Info("starting http server", "port", cfg.HTTPPort)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("http server error", "error", err)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case err := <-errChan:
		logger.Error("strategy service failed", "error", err)
		cancel()
	case sig := <-sigChan:
		logger.Info("received signal, shutting down", "signal", sig)
		cancel()
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http server shutdown error", "error", err)
	}

	logger.Info("shutdown complete")
}
