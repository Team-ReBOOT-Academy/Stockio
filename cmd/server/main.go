package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/adapters/config"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/adapters/http/health"
	kafkaclient "github.com/Team-ReBOOT-Academy/Stockio/internal/adapters/kafka"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/adapters/logging"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/service"
)

func main() {
	logger := logging.New()
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("config load failed", "error", err)
		os.Exit(1)
	}

	logger.Info("starting Stockio strategy service",
		"service", cfg.ServiceName, "version", cfg.Version,
		"accountId", cfg.AccountID, "groupId", cfg.GroupID,
		"strategyId", cfg.StrategyID, "candleTopic", cfg.CandleTopic,
		"kafkaGroupId", cfg.KafkaGroupID)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	runCtx, cancelRun := context.WithCancel(ctx)
	defer cancelRun()

	healthSrv, tracker := health.New(cfg.HTTPAddr, cfg.ServiceName, cfg.Version, logger)
	tracker.Set("kafka", health.StateUnknown, "not connected yet")

	go func() {
		if err := healthSrv.Start(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
			cancelRun()
		}
	}()

	client, err := kafkaclient.New(kafkaclient.Options{
		Brokers:                 cfg.KafkaBootstrap,
		Username:                cfg.KafkaUsername,
		Password:                cfg.KafkaPassword,
		GroupID:                 cfg.KafkaGroupID,
		Topics:                  []string{cfg.CandleTopic, cfg.TopicRegistrationResult, cfg.TopicRecommendationResult},
		RegistrationResultTopic: cfg.TopicRegistrationResult,
	})
	if err != nil {
		logger.Error("kafka client init failed", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	svc := service.New(cfg.Settings, client, tracker, logger)

	if err := svc.Run(runCtx); err != nil {
		logger.Error("strategy run failed", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := healthSrv.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("http shutdown failed", "error", err)
	}

	logger.Info("service stopped")
}
