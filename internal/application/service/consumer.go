package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/contracts"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/ports"
)

func (s *Service) consumeLoop(ctx context.Context) error {
	for {
		fetches := s.broker.Poll(ctx)
		if ctx.Err() != nil {
			return nil
		}

		var hadError bool
		for _, fetchErr := range fetches.Errors {
			if errors.Is(fetchErr.Err, context.Canceled) {
				continue
			}
			hadError = true
			s.health.SetKafka(false, fetchErr.Err.Error())
			s.logger.Error("fetch error", "topic", fetchErr.Topic, "partition", fetchErr.Partition, "error", fetchErr.Err)
		}
		if !hadError {
			s.health.SetKafka(true, "")
		}

		for _, rec := range fetches.Records {
			if err := s.handleRecord(ctx, rec); err != nil {
				if errors.Is(err, errDeferCommit) {
					continue
				}
				s.logger.Error("handle record failed",
					"topic", rec.Topic, "partition", rec.Partition,
					"offset", rec.Offset, "error", err)
				continue
			}
			rec.MarkProcessed()
		}
	}
}

func (s *Service) handleRecord(ctx context.Context, rec ports.Record) error {
	env, err := contracts.DecodeEnvelope(rec.Value)
	if err != nil {
		return fmt.Errorf("unmarshal envelope: %w", err)
	}
	switch env.EventType {
	case "account.registration.result":
		p, err := contracts.DecodePayload[contracts.RegistrationResultPayload](env)
		if err != nil {
			return fmt.Errorf("unmarshal registration result: %w", err)
		}
		s.handleRegistrationResult(env, p)
	case "market.candle.closed":
		p, err := contracts.DecodePayload[contracts.CandlePayload](env)
		if err != nil {
			return fmt.Errorf("unmarshal candle: %w", err)
		}
		return s.handleCandle(ctx, rec, env, p)
	case "strategy.recommendation.result":
		p, err := contracts.DecodePayload[contracts.RecommendationResultPayload](env)
		if err != nil {
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
