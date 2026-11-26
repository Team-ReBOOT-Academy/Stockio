package usecases

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/config"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/domain"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/ports"
)

type StrategyService struct {
	cfg                      *config.Config
	publisher                ports.KafkaPublisher
	consumer                 ports.KafkaSubscriber
	logger                   *slog.Logger
	startedAt                string
	instanceID               string
	processedCandles         atomic.Int64
	publishedRecommendations atomic.Int64
	lastProcessedSequence    atomic.Int64
}

func NewStrategyService(cfg *config.Config, pub ports.KafkaPublisher, sub ports.KafkaSubscriber, logger *slog.Logger) *StrategyService {
	return &StrategyService{
		cfg:        cfg,
		publisher:  pub,
		consumer:   sub,
		logger:     logger,
		startedAt:  time.Now().UTC().Format(time.RFC3339),
		instanceID: fmt.Sprintf("instance-%d", time.Now().UnixNano()),
	}
}

func (s *StrategyService) Run(ctx context.Context) error {
	msgChan, errChan := s.consumer.StartConsuming(ctx)
	assignmentCtx, cancelAssignment := context.WithTimeout(ctx, 30*time.Second)
	err := s.consumer.WaitForTopicAssignment(assignmentCtx, s.cfg.RegistrationResultTopic)
	cancelAssignment()
	if err != nil {
		return fmt.Errorf("wait for registration result topic assignment: %w", err)
	}

	reqID := domain.GenerateUUID()

	regPayload := domain.RegistrationRequestPayload{
		SecretKey:    s.cfg.AccountSecretKey,
		AccountID:    s.cfg.AccountID,
		AccountName:  s.cfg.AccountName,
		GroupID:      s.cfg.GroupID,
		StrategyID:   s.cfg.StrategyID,
		StrategyName: s.cfg.StrategyName,
		ServiceName:  s.cfg.ServiceName,
		Description:  "Momentum strategy service baseline",
	}

	payloadBytes, err := json.Marshal(regPayload)
	if err != nil {
		return fmt.Errorf("failed to marshal registration payload: %w", err)
	}

	req := domain.Envelope{
		EventID:       reqID,
		EventType:     "account.registration.request",
		SchemaVersion: "1.0",
		OccurredAt:    time.Now().UTC().Format(time.RFC3339),
		CorrelationID: reqID,
		Producer:      s.cfg.ServiceName,
		Payload:       payloadBytes,
	}

	s.logger.Info("sending registration request", "event_id", reqID)
	if err := s.publisher.Publish(ctx, "account.registration.request", s.cfg.ServiceName, req); err != nil {
		return fmt.Errorf("publish registration request: %w", err)
	}

	s.logger.Info("waiting for registration result", "timeout_seconds", 30, "topic", s.cfg.RegistrationResultTopic)
	registrationTimeout := time.NewTimer(30 * time.Second)
	defer registrationTimeout.Stop()
	registered := false

	for !registered {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-registrationTimeout.C:
			s.logger.Error("registration timed out", "event_id", reqID, "topic", s.cfg.RegistrationResultTopic)
			return fmt.Errorf("registration timeout")
		case err, ok := <-errChan:
			if !ok {
				return fmt.Errorf("kafka consumer stopped before registration")
			}
			s.logger.Error("kafka error", "error", err)
		case env, ok := <-msgChan:
			if !ok {
				return fmt.Errorf("kafka consumer stopped before registration")
			}
			if env.EventType == "account.registration.result" {
				if env.CausationID == nil || *env.CausationID != reqID {
					s.logger.Warn("ignoring registration result that does not match request", "event_id", env.EventID, "expected_causation_id", reqID)
					continue
				}
				var payload domain.RegistrationResultPayload
				if err := json.Unmarshal(env.Payload, &payload); err != nil {
					s.logger.Error("failed to unmarshal registration result", "error", err)
					continue
				}
				if payload.Status == "REGISTERED" {
					s.logger.Info("successfully registered")
					registered = true
				} else {
					return fmt.Errorf("registration rejected: %s", payload.Reason)
				}
			}
		}
	}
	go s.heartbeatLoop(ctx)
	for {
		select {
		case <-ctx.Done():
			return nil
		case err := <-errChan:
			s.logger.Error("kafka error", "error", err)
		case env := <-msgChan:
			if env.EventType == "market.candle.closed" {
				s.processCandle(ctx, env)
			}
		}
	}
}

func (s *StrategyService) processCandle(ctx context.Context, env *domain.Envelope) {
	var candle domain.CandlePayload
	if err := json.Unmarshal(env.Payload, &candle); err != nil {
		s.logger.Error("failed to unmarshal candle", "error", err)
		return
	}

	recID := domain.GenerateUUID()
	now := time.Now().UTC().Format(time.RFC3339)
	causationID := env.EventID
	recPayload := domain.RecommendationPayload{
		RecommendationID:  recID,
		SourceEventID:     env.EventID,
		RunID:             candle.RunID,
		SecretKey:         s.cfg.AccountSecretKey,
		StrategyID:        s.cfg.StrategyID,
		AccountID:         s.cfg.AccountID,
		GroupID:           s.cfg.GroupID,
		Instrument:        candle.Instrument.Ticker,
		SequenceNumber:    candle.SequenceNumber,
		Signal:            "HOLD",
		Confidence:        0.0,
		SuggestedQuantity: 0,
		DataQuality:       "OK",
		Reason:            "Учебная заглушка HOLD",
		ProcessingTimeMs:  0,
		Metadata:          map[string]any{},
	}

	payloadBytes, err := json.Marshal(recPayload)
	if err != nil {
		s.logger.Error("failed to marshal recommendation payload", "error", err)
		return
	}

	rec := domain.Envelope{
		EventID:       recID,
		EventType:     "strategy.recommendation.created",
		SchemaVersion: "1.0",
		OccurredAt:    now,
		CorrelationID: env.CorrelationID,
		CausationID:   &causationID,
		Producer:      s.cfg.ServiceName,
		Payload:       payloadBytes,
	}

	if err := s.publisher.Publish(ctx, s.cfg.RecommendationTopic, recID, rec); err != nil {
		s.logger.Error("failed to publish recommendation", "error", err)
	} else {
		s.processedCandles.Add(1)
		s.publishedRecommendations.Add(1)
		s.lastProcessedSequence.Store(candle.SequenceNumber)
		s.logger.Info("published HOLD recommendation", "ticker", candle.Instrument.Ticker, "seq", candle.SequenceNumber)
	}
}

func (s *StrategyService) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().UTC().Format(time.RFC3339)
			lastSeq := s.lastProcessedSequence.Load()

			// Создаем payload heartbeat
			hbPayload := domain.HeartbeatPayload{
				SecretKey:                   s.cfg.AccountSecretKey,
				AccountID:                   s.cfg.AccountID,
				GroupID:                     s.cfg.GroupID,
				StrategyID:                  s.cfg.StrategyID,
				ServiceName:                 s.cfg.ServiceName,
				InstanceID:                  s.instanceID,
				Status:                      "UP",
				StartedAt:                   s.startedAt,
				SentAt:                      now,
				ProcessedCandles:            s.processedCandles.Load(),
				PublishedRecommendations:    s.publishedRecommendations.Load(),
				LastProcessedSequenceNumber: &lastSeq,
				Metadata:                    map[string]any{"version": "1.0.0"},
			}
			payloadBytes, err := json.Marshal(hbPayload)
			if err != nil {
				s.logger.Error("failed to marshal heartbeat payload", "error", err)
				continue
			}

			hb := domain.Envelope{
				EventID:       domain.GenerateUUID(),
				EventType:     "account.service.heartbeat",
				SchemaVersion: "1.0",
				OccurredAt:    now,
				Producer:      s.cfg.ServiceName,
				Payload:       payloadBytes,
			}

			if err := s.publisher.Publish(ctx, s.cfg.HeartbeatTopic, s.cfg.ServiceName, hb); err != nil {
				s.logger.Error("failed to publish heartbeat", "error", err)
			}
		}
	}
}
