package strategy

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/config"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/health"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/messaging"
)

type Service struct {
	cfg     config.Config
	logger  *slog.Logger
	client  *kgo.Client
	tracker *health.Tracker

	assignedCh          chan struct{}
	registeredCh        chan struct{}
	publishingReady     atomic.Bool
	registrationErrCh   chan error
	registrationOnce    sync.Once
	registrationEventID string
	registrationTime    string
	pendingMu           sync.Mutex
	pendingCandles      []pendingCandle

	instanceID string
	startedAt  time.Time

	processedCandles         atomic.Int64
	publishedRecommendations atomic.Int64
	lastSequenceNumber       atomic.Int64
}

type pendingCandle struct {
	record  *kgo.Record
	env     messaging.RawEnvelope
	payload messaging.CandlePayload
}

var errDeferCommit = errors.New("record deferred until service registration")

func New(cfg config.Config, client *kgo.Client, tracker *health.Tracker, logger *slog.Logger, assignedCh chan struct{}) *Service {
	s := &Service{
		cfg:                 cfg,
		logger:              logger,
		client:              client,
		tracker:             tracker,
		assignedCh:          assignedCh,
		registeredCh:        make(chan struct{}),
		registrationErrCh:   make(chan error, 1),
		registrationEventID: uuid.NewString(),
		registrationTime:    messaging.NowUTC(),
		instanceID:          uuid.NewString(),
		startedAt:           time.Now().UTC(),
	}
	s.lastSequenceNumber.Store(-1)
	return s
}

func (s *Service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	g, gctx := errgroup.WithContext(ctx)

	g.Go(func() error { return s.consumeLoop(gctx) })

	if err := s.waitForAssignment(gctx); err != nil {
		cancel()
		_ = g.Wait()
		return err
	}

	if err := s.registerUntilConfirmed(gctx); err != nil {
		cancel()
		_ = g.Wait()
		return fmt.Errorf("registration: %w", err)
	}
	s.logger.Info("registration confirmed")
	g.Go(func() error { return s.heartbeatLoop(gctx) })
	g.Go(func() error { return s.pingLoop(gctx) })
	if err := s.publishPendingCandles(gctx); err != nil {
		cancel()
		_ = g.Wait()
		return fmt.Errorf("publish candles buffered before registration: %w", err)
	}

	err := g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (s *Service) waitForAssignment(ctx context.Context) error {
	for {
		select {
		case <-s.assignedCh:
			s.logger.Info("consumer group assigned")
			return nil
		case <-time.After(10 * time.Second):
			s.logger.Warn("waiting for consumer group assignment")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) registerUntilConfirmed(ctx context.Context) error {
	retry := time.NewTicker(30 * time.Second)
	defer retry.Stop()
	for {
		if err := s.sendRegistration(ctx); err != nil {
			s.logger.Error("registration publish failed; retrying", "eventId", s.registrationEventID, "error", err)
			select {
			case <-time.After(5 * time.Second):
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		s.logger.Info("registration request published", "eventId", s.registrationEventID)
		select {
		case <-s.registeredCh:
			return nil
		case err := <-s.registrationErrCh:
			return err
		case <-retry.C:
			s.logger.Warn("registration result not received; retrying request", "eventId", s.registrationEventID)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *Service) consumeLoop(ctx context.Context) error {
	for {
		fetches := s.client.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil
		}

		var hadError bool
		fetches.EachError(func(topic string, partition int32, err error) {
			if errors.Is(err, context.Canceled) {
				return
			}
			hadError = true
			s.tracker.Set("kafka", health.StateDown, err.Error())
			s.logger.Error("fetch error", "topic", topic, "partition", partition, "error", err)
		})
		if !hadError {
			s.tracker.Set("kafka", health.StateUp, "")
		}

		fetches.EachRecord(func(rec *kgo.Record) {
			if err := s.handleRecord(ctx, rec); err != nil {
				if errors.Is(err, errDeferCommit) {
					return
				}
				s.logger.Error("handle record failed",
					"topic", rec.Topic, "partition", rec.Partition,
					"offset", rec.Offset, "error", err)
				return
			}
			s.client.MarkCommitRecords(rec)
		})
	}
}

func (s *Service) handleRecord(ctx context.Context, rec *kgo.Record) error {
	var env messaging.RawEnvelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		return fmt.Errorf("unmarshal envelope: %w", err)
	}
	switch env.EventType {
	case "account.registration.result":
		var p messaging.RegistrationResultPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal registration result: %w", err)
		}
		s.handleRegistrationResult(env, p)
	case "market.candle.closed":
		var p messaging.CandlePayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal candle: %w", err)
		}
		return s.handleCandle(ctx, rec, env, p)
	case "strategy.recommendation.result":
		var p messaging.RecommendationResultPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return fmt.Errorf("unmarshal recommendation result: %w", err)
		}
		if p.AccountID == s.cfg.AccountID && p.StrategyID == s.cfg.StrategyID {
			s.logger.Info("recommendation result",
				"recommendationId", p.RecommendationID,
				"status", p.RecommendationStatus,
				"signal", p.AccountSignal,
				"instrument", p.Instrument,
				"sequenceNumber", p.SequenceNumber)
		}
	default:
		s.logger.Debug("ignoring event", "eventType", env.EventType)
	}
	return nil
}

func (s *Service) sendRegistration(ctx context.Context) error {
	env := messaging.Envelope[messaging.RegistrationPayload]{
		EventID:       s.registrationEventID,
		EventType:     "account.registration.request",
		SchemaVersion: messaging.SchemaVersion,
		OccurredAt:    s.registrationTime,
		CorrelationID: s.registrationEventID,
		CausationID:   nil,
		Producer:      s.cfg.ServiceName,
		Payload: messaging.RegistrationPayload{
			SecretKey:    s.cfg.AccountSecretKey,
			AccountID:    s.cfg.AccountID,
			AccountName:  s.cfg.AccountName,
			GroupID:      s.cfg.GroupID,
			StrategyID:   s.cfg.StrategyID,
			StrategyName: s.cfg.StrategyName,
			ServiceName:  s.cfg.ServiceName,
			Description:  s.cfg.Description,
		},
	}
	value, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.produce(ctx, &kgo.Record{
		Topic: s.cfg.TopicRegistrationRequest,
		Key:   []byte(s.cfg.ServiceName),
		Value: value,
	}, 10*time.Second)
}

func (s *Service) handleRegistrationResult(env messaging.RawEnvelope, p messaging.RegistrationResultPayload) {
	if p.AccountID != s.cfg.AccountID || p.StrategyID != s.cfg.StrategyID || p.ServiceName != s.cfg.ServiceName {
		return
	}
	causedByRequest := env.CausationID != nil && *env.CausationID == s.registrationEventID
	correlatedWithRequest := env.CorrelationID == s.registrationEventID
	if !causedByRequest && !correlatedWithRequest {
		s.logger.Info("registration result from another request",
			"causationId", env.CausationID, "correlationId", env.CorrelationID, "status", p.Status)
		return
	}
	if p.Status == "REGISTERED" {
		s.logger.Info("registration accepted",
			"accountId", p.AccountID, "groupId", p.GroupID,
			"strategyId", p.StrategyID, "serviceName", p.ServiceName)
		s.registrationOnce.Do(func() { close(s.registeredCh) })
		return
	}
	s.logger.Error("registration rejected", "status", p.Status, "reason", p.Reason)
	s.registrationOnce.Do(func() {
		s.registrationErrCh <- fmt.Errorf("registration rejected: %s (%s)", p.Status, p.Reason)
	})
}

func (s *Service) handleCandle(ctx context.Context, rec *kgo.Record, env messaging.RawEnvelope, p messaging.CandlePayload) error {
	s.pendingMu.Lock()
	if !s.publishingReady.Load() {
		s.pendingCandles = append(s.pendingCandles, pendingCandle{record: rec, env: env, payload: p})
		s.pendingMu.Unlock()
		return errDeferCommit
	}
	s.pendingMu.Unlock()
	return s.publishCandle(ctx, env, p, nil)
}

func (s *Service) publishPendingCandles(ctx context.Context) error {
	for {
		s.pendingMu.Lock()
		if len(s.pendingCandles) == 0 {
			s.publishingReady.Store(true)
			s.pendingMu.Unlock()
			return nil
		}
		pending := s.pendingCandles
		s.pendingCandles = nil
		s.pendingMu.Unlock()
		for _, candle := range pending {
			if err := s.publishCandle(ctx, candle.env, candle.payload, candle.record); err != nil {
				return err
			}
		}
	}
}

func (s *Service) publishCandle(ctx context.Context, env messaging.RawEnvelope, p messaging.CandlePayload, record *kgo.Record) error {
	s.logger.Info("candle received",
		"eventId", env.EventID, "runId", p.RunID,
		"ticker", p.Instrument.Ticker,
		"sequenceNumber", p.SequenceNumber,
		"candleTime", p.CandleTime, "close", p.Close,
		"volume", p.Volume, "sessionPhase", p.SessionPhase,
		"lotSize", p.Instrument.LotSize)
	if err := s.publishHold(ctx, env, p); err != nil {
		return fmt.Errorf("publish HOLD: %w", err)
	}
	s.processedCandles.Add(1)
	s.lastSequenceNumber.Store(p.SequenceNumber)
	s.publishedRecommendations.Add(1)
	if record != nil {
		s.client.MarkCommitRecords(record)
	}
	return nil
}

func (s *Service) publishHold(ctx context.Context, candleEnv messaging.RawEnvelope, candle messaging.CandlePayload) error {
	causation := candleEnv.EventID
	payload := messaging.RecommendationPayload{
		RecommendationID:  "hold-" + candleEnv.EventID,
		SourceEventID:     candleEnv.EventID,
		RunID:             candle.RunID,
		SecretKey:         s.cfg.AccountSecretKey,
		StrategyID:        s.cfg.StrategyID,
		AccountID:         s.cfg.AccountID,
		GroupID:           s.cfg.GroupID,
		Instrument:        candle.Instrument.Ticker,
		SequenceNumber:    candle.SequenceNumber,
		Signal:            "HOLD",
		Confidence:        0,
		SuggestedQuantity: 0,
		DataQuality:       "OK",
		Reason:            "Алгоритм ещё не готов",
		ProcessingTimeMs:  0,
		Metadata:          map[string]any{},
	}
	env := messaging.Envelope[messaging.RecommendationPayload]{
		EventID:       uuid.NewString(),
		EventType:     "strategy.recommendation.created",
		SchemaVersion: messaging.SchemaVersion,
		OccurredAt:    messaging.NowUTC(),
		CorrelationID: candleEnv.CorrelationID,
		CausationID:   &causation,
		Producer:      s.cfg.ServiceName,
		Payload:       payload,
	}
	value, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.produce(ctx, &kgo.Record{
		Topic: s.cfg.TopicRecommendationCreate,
		Key:   []byte(candleEnv.EventID),
		Value: value,
	}, 5*time.Second)
}

func (s *Service) heartbeatLoop(ctx context.Context) error {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.sendHeartbeat(ctx); err != nil {
				s.logger.Error("send heartbeat failed", "error", err)
			}
		}
	}
}

func (s *Service) sendHeartbeat(ctx context.Context) error {
	lastSeq := s.lastSequenceNumber.Load()
	var lastSeqPtr *int64
	if lastSeq >= 0 {
		v := lastSeq
		lastSeqPtr = &v
	}
	eventID := uuid.NewString()
	env := messaging.Envelope[messaging.HeartbeatPayload]{
		EventID:       eventID,
		EventType:     "account.service.heartbeat",
		SchemaVersion: messaging.SchemaVersion,
		OccurredAt:    messaging.NowUTC(),
		CorrelationID: eventID,
		CausationID:   nil,
		Producer:      s.cfg.ServiceName,
		Payload: messaging.HeartbeatPayload{
			SecretKey:                   s.cfg.AccountSecretKey,
			AccountID:                   s.cfg.AccountID,
			GroupID:                     s.cfg.GroupID,
			StrategyID:                  s.cfg.StrategyID,
			ServiceName:                 s.cfg.ServiceName,
			InstanceID:                  s.instanceID,
			Status:                      "UP",
			StartedAt:                   s.startedAt.Format(time.RFC3339),
			SentAt:                      messaging.NowUTC(),
			ProcessedCandles:            s.processedCandles.Load(),
			PublishedRecommendations:    s.publishedRecommendations.Load(),
			LastProcessedSequenceNumber: lastSeqPtr,
			Metadata:                    map[string]any{"appVersion": s.cfg.Version},
		},
	}
	value, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return s.produce(ctx, &kgo.Record{
		Topic: s.cfg.TopicHeartbeat,
		Key:   []byte(s.cfg.ServiceName),
		Value: value,
	}, 5*time.Second)
}

func (s *Service) pingLoop(ctx context.Context) error {
	s.ping(ctx)
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.ping(ctx)
		}
	}
}

func (s *Service) ping(ctx context.Context) {
	pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := s.client.Ping(pctx); err != nil {
		s.tracker.Set("kafka", health.StateDown, err.Error())
		return
	}
	s.tracker.Set("kafka", health.StateUp, "")
}

func (s *Service) produce(ctx context.Context, rec *kgo.Record, timeout time.Duration) error {
	pctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.client.ProduceSync(pctx, rec).FirstErr()
}
