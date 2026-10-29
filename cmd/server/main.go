package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/infrastructure/config"
	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/infrastructure/kafka"

	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/interfaces/httpapi"
)

func main() {
	cfg := config.Load()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	mux := http.NewServeMux()
	kafkaHealth := httpapi.NewKafkaHealth()
	httpapi.RegisterRoutes(mux, cfg, kafkaHealth)

	server := &http.Server{
		Addr:              ":" + cfg.HTTPPort,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)

	//log.Printf("Starting %s v%s on port %s", cfg.AppName, cfg.AppVersion, cfg.HTTPPort)

	go func() {
		slog.Info("http server listening", "addr", server.Addr)

		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	go runKafka(ctx, cfg, kafkaHealth)

	select {
	case <-ctx.Done():
		slog.Info("shutdown signal received")
	case err := <-serverErr:
		slog.Error("http server error", "err", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown error", "err", err)
	}
	slog.Info("server exited")
}

func runKafka(ctx context.Context, cfg config.Config, health *httpapi.KafkaHealth) {
	for ctx.Err() == nil {
		client, err := kafka.NewClient(cfg)
		if err == nil {
			health.Set("UNKNOWN", "registration pending")
			err = client.Start(ctx)
			if err == nil {
				health.Set("UP", "connected and registered")
			}
			client.Close()
		}
		if err != nil && ctx.Err() == nil {
			health.Set("DOWN", "connection unavailable")
			slog.Error("Kafka unavailable; retrying", "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}
