package service

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/contracts"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/ports"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/domain/strategy"
)

type pendingCandle struct {
	record  ports.Record
	env     contracts.RawEnvelope
	payload contracts.CandlePayload
}

var errDeferCommit = errors.New("record deferred until service registration")

func (s *Service) handleCandle(ctx context.Context, rec ports.Record, env contracts.RawEnvelope, p contracts.CandlePayload) error {
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
			record := candle.record
			if err := s.publishCandle(ctx, candle.env, candle.payload, &record); err != nil {
				return err
			}
		}
	}
}

func (s *Service) publishCandle(ctx context.Context, env contracts.RawEnvelope, p contracts.CandlePayload, record *ports.Record) error {
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
		record.MarkProcessed()
	}
	return nil
}

func (s *Service) publishHold(ctx context.Context, candleEnv contracts.RawEnvelope, candle contracts.CandlePayload) error {
	causation := candleEnv.EventID
	payload := contracts.RecommendationPayload{
		RecommendationID:  "hold-" + candleEnv.EventID,
		SourceEventID:     candleEnv.EventID,
		RunID:             candle.RunID,
		SecretKey:         s.cfg.AccountSecretKey,
		StrategyID:        s.cfg.StrategyID,
		AccountID:         s.cfg.AccountID,
		GroupID:           s.cfg.GroupID,
		Instrument:        candle.Instrument.Ticker,
		SequenceNumber:    candle.SequenceNumber,
		Signal:            string(strategy.Hold),
		Confidence:        0,
		SuggestedQuantity: 0,
		DataQuality:       "OK",
		Reason:            "Алгоритм ещё не готов",
		ProcessingTimeMs:  0,
		Metadata:          map[string]any{},
	}
	env := contracts.Envelope[contracts.RecommendationPayload]{
		EventID:       uuid.NewString(),
		EventType:     "strategy.recommendation.created",
		SchemaVersion: contracts.SchemaVersion,
		OccurredAt:    contracts.NowUTC(),
		CorrelationID: candleEnv.CorrelationID,
		CausationID:   &causation,
		Producer:      s.cfg.ServiceName,
		Payload:       payload,
	}
	value, err := contracts.MarshalEnvelope(env)
	if err != nil {
		return err
	}
	return s.broker.Publish(ctx, s.cfg.TopicRecommendationCreate, []byte(candleEnv.EventID), value, 5*time.Second)
}
