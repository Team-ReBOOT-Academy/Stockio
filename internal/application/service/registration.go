package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/contracts"
)

func (s *Service) sendRegistration(ctx context.Context) error {
	env := contracts.Envelope[contracts.RegistrationPayload]{
		EventID:       s.registrationEventID,
		EventType:     "account.registration.request",
		SchemaVersion: contracts.SchemaVersion,
		OccurredAt:    s.registrationTime,
		CorrelationID: s.registrationEventID,
		CausationID:   nil,
		Producer:      s.cfg.ServiceName,
		Payload: contracts.RegistrationPayload{
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
	value, err := contracts.MarshalEnvelope(env)
	if err != nil {
		return err
	}
	return s.broker.Publish(ctx, s.cfg.TopicRegistrationRequest, []byte(s.cfg.ServiceName), value, 10*time.Second)
}

func (s *Service) handleRegistrationResult(env contracts.RawEnvelope, p contracts.RegistrationResultPayload) {
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
