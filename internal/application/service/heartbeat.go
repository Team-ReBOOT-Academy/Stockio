package service

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/contracts"
)

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
	env := contracts.Envelope[contracts.HeartbeatPayload]{
		EventID:       eventID,
		EventType:     "account.service.heartbeat",
		SchemaVersion: contracts.SchemaVersion,
		OccurredAt:    contracts.NowUTC(),
		CorrelationID: eventID,
		CausationID:   nil,
		Producer:      s.cfg.ServiceName,
		Payload: contracts.HeartbeatPayload{
			SecretKey:                   s.cfg.AccountSecretKey,
			AccountID:                   s.cfg.AccountID,
			GroupID:                     s.cfg.GroupID,
			StrategyID:                  s.cfg.StrategyID,
			ServiceName:                 s.cfg.ServiceName,
			InstanceID:                  s.instanceID,
			Status:                      "UP",
			StartedAt:                   s.startedAt.Format(time.RFC3339),
			SentAt:                      contracts.NowUTC(),
			ProcessedCandles:            s.processedCandles.Load(),
			PublishedRecommendations:    s.publishedRecommendations.Load(),
			LastProcessedSequenceNumber: lastSeqPtr,
			Metadata:                    map[string]any{"appVersion": s.cfg.Version},
		},
	}
	value, err := contracts.MarshalEnvelope(env)
	if err != nil {
		return err
	}
	return s.broker.Publish(ctx, s.cfg.TopicHeartbeat, []byte(s.cfg.ServiceName), value, 5*time.Second)
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
	if err := s.broker.Ping(pctx); err != nil {
		s.health.SetKafka(false, err.Error())
		return
	}
	s.health.SetKafka(true, "")
}
