package service

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sync/errgroup"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/application"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/contracts"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/ports"
)

type Service struct {
	cfg    application.Settings
	logger ports.Logger
	broker ports.Broker
	health ports.Health

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

func New(cfg application.Settings, broker ports.Broker, tracker ports.Health, logger ports.Logger) *Service {
	s := &Service{
		cfg:                 cfg,
		logger:              logger,
		broker:              broker,
		health:              tracker,
		registeredCh:        make(chan struct{}),
		registrationErrCh:   make(chan error, 1),
		registrationEventID: uuid.NewString(),
		registrationTime:    contracts.NowUTC(),
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
		case <-s.broker.Assigned():
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
