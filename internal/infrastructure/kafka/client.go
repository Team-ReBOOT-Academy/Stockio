package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/contracts"
	"github.com/Team-ReBOOT-Academy/Stockio.git/internal/infrastructure/config"
	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Client struct {
	cfg                     config.Config
	producer                *kgo.Client
	answers                 *kgo.Client
	candles                 *kgo.Client
	cancel                  context.CancelFunc
	done                    chan struct{}
	started                 atomic.Bool
	acceptCandles           atomic.Bool
	registered              atomic.Bool
	processed               atomic.Int64
	published               atomic.Int64
	registrationMu          sync.Mutex
	registrationByCausation map[string]registrationOutcome
	pendingRegistrationID   string
	loops                   sync.WaitGroup
}

func NewClient(cfg config.Config) (*Client, error) {
	if cfg.KafkaBootstrapServers == "" || cfg.KafkaUsername == "" || cfg.KafkaPassword == "" || cfg.KafkaGroupID == "" || cfg.CandleTopic == "" {
		return nil, fmt.Errorf("Kafka requires KAFKA_BOOTSTRAP_SERVERS, KAFKA_USERNAME, KAFKA_PASSWORD, KAFKA_GROUP_ID and CANDLE_TOPIC")
	}
	base := []kgo.Opt{
		kgo.SeedBrokers(cfg.KafkaBootstrapServers),
		kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{User: cfg.KafkaUsername, Pass: cfg.KafkaPassword}.AsSha256Mechanism()),
		kgo.RetryBackoffFn(func(int) time.Duration { return time.Second }),
		kgo.RequestRetries(10),
		kgo.RecordDeliveryTimeout(30 * time.Second),
	}
	producer, err := kgo.NewClient(base...)
	if err != nil {
		return nil, fmt.Errorf("create Kafka producer: %w", err)
	}
	answers, err := kgo.NewClient(append(append([]kgo.Opt{}, base...),
		kgo.ConsumerGroup(cfg.KafkaGroupID+".answers"),
		kgo.ConsumeTopics(cfg.RegistrationResultTopic, cfg.RecommendationResultTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)...)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("create Kafka response consumer: %w", err)
	}
	candles, err := kgo.NewClient(append(append([]kgo.Opt{}, base...),
		kgo.ConsumerGroup(cfg.KafkaGroupID), kgo.ConsumeTopics(cfg.CandleTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), kgo.DisableAutoCommit())...)
	if err != nil {
		answers.Close()
		producer.Close()
		return nil, fmt.Errorf("create Kafka candle consumer: %w", err)
	}
	return &Client{cfg: cfg, producer: producer, answers: answers, candles: candles, done: make(chan struct{}), registrationByCausation: make(map[string]registrationOutcome)}, nil
}

func (c *Client) Start(parent context.Context) error {
	if !c.started.CompareAndSwap(false, true) {
		return fmt.Errorf("Kafka client already started")
	}
	ctx, cancel := context.WithCancel(parent)
	c.cancel = cancel
	c.loops.Add(2)
	go c.answerLoop(ctx)
	go c.candleLoop(ctx)
	go func() { c.loops.Wait(); close(c.done) }()
	if err := c.answers.Ping(ctx); err != nil {
		cancel()
		c.Close()
		return fmt.Errorf("Kafka response consumer: %w", err)
	}
	if err := c.candles.Ping(ctx); err != nil {
		cancel()
		c.Close()
		return fmt.Errorf("Kafka candle consumer: %w", err)
	}
	// answerLoop and candleLoop poll before we publish, so their subscriptions are active.
	if err := c.register(ctx); err != nil {
		cancel()
		c.Close()
		return err
	}
	c.acceptCandles.Store(true)
	go c.heartbeatLoop(ctx)
	return nil
}

func (c *Client) Close() {
	if c.cancel != nil {
		c.cancel()
	}
	if c.started.Load() {
		<-c.done
	}
	c.answers.Close()
	c.candles.Close()
	c.producer.Close()
}

func (c *Client) register(ctx context.Context) error {
	requestID := uuid.NewString()
	c.registrationMu.Lock()
	c.pendingRegistrationID = requestID
	c.registrationMu.Unlock()
	request := contracts.Envelope{EventID: requestID, EventType: "account.registration.request",
		SchemaVersion: "1.0", OccurredAt: now(), CorrelationID: requestID,
		CausationID: nil, Producer: c.cfg.ServiceName, Payload: contracts.RegistrationRequest{
			SecretKey: c.cfg.SecretKey, AccountID: c.cfg.AccountID, AccountName: c.cfg.AccountName,
			GroupID: c.cfg.GroupID, StrategyID: c.cfg.StrategyID, StrategyName: c.cfg.StrategyName,
			ServiceName: c.cfg.ServiceName, Description: "Stockio momentum strategy",
		}}
	if err := c.produceJSON(ctx, c.cfg.RegistrationTopic, c.cfg.ServiceName, request); err != nil {
		return fmt.Errorf("publish registration request: %w", err)
	}
	slog.Info("registration request published", "event_id", requestID, "account_id", c.cfg.AccountID, "group_id", c.cfg.GroupID)
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			return fmt.Errorf("registration timed out after 30s")
		case <-time.After(100 * time.Millisecond):
		}
		c.registrationMu.Lock()
		result, found := c.registrationByCausation[requestID]
		if found {
			delete(c.registrationByCausation, requestID)
		}
		c.registrationMu.Unlock()
		if found {
			if result.Status != "REGISTERED" {
				return fmt.Errorf("registration rejected: %s", result.Reason)
			}
			if result.AccountID != c.cfg.AccountID || result.GroupID != c.cfg.GroupID || result.StrategyID != c.cfg.StrategyID || result.ServiceName != c.cfg.ServiceName {
				return fmt.Errorf("registration response identity does not match configuration")
			}
			c.registered.Store(true)
			slog.Info("service registered", "account_id", result.AccountID, "group_id", result.GroupID, "strategy_id", result.StrategyID)
			return nil
		}
	}
}

type registrationOutcome struct{ Causation, Status, Reason, AccountID, GroupID, StrategyID, ServiceName string }

var _ = registrationOutcome{}

func (c *Client) answerLoop(ctx context.Context) {
	defer c.loops.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := c.answers.PollFetches(ctx)
		if ctx.Err() != nil {
			return
		}
		for _, err := range fetches.Errors() {
			slog.Warn("Kafka response fetch failed; retrying", "topic", err.Topic, "partition", err.Partition, "error", err.Err)
		}
		for _, rec := range fetches.Records() {
			c.handleAnswer(rec)
			// Persist progress through the shared response topics so every reconnect
			// does not replay historical registration responses from the beginning.
			if err := c.answers.CommitRecords(ctx, rec); err != nil && ctx.Err() == nil {
				slog.Warn("Kafka response offset commit failed", "topic", rec.Topic, "partition", rec.Partition, "error", err)
			}
		}
	}
}

func (c *Client) handleAnswer(rec *kgo.Record) {
	var env contracts.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		slog.Warn("invalid Kafka response envelope", "topic", rec.Topic, "error", err)
		return
	}
	if env.EventType == "account.registration.result" {
		var result contracts.RegistrationResult
		if err := decodePayload(env.Payload, &result); err != nil {
			slog.Warn("invalid registration result", "error", err)
			return
		}
		causeID := causation(env)
		if causeID == "" {
			return
		}
		c.registrationMu.Lock()
		isCurrentRequest := causeID == c.pendingRegistrationID
		c.registrationMu.Unlock()
		if !isCurrentRequest {
			return
		}
		if result.AccountID != c.cfg.AccountID {
			slog.Warn("registration response for current request has unexpected accountId", "event_id", env.EventID, "causation_id", causeID)
			return
		}
		if result.SecretKey != c.cfg.SecretKey {
			slog.Warn("registration response for current request has unexpected key", "event_id", env.EventID, "causation_id", causeID)
			return
		}
		slog.Info("registration result received", "event_id", env.EventID, "causation_id", causeID, "account_id", result.AccountID, "status", result.Status)
		c.registrationMu.Lock()
		c.registrationByCausation[causeID] = registrationOutcome{Causation: causeID, Status: result.Status, Reason: result.Reason, AccountID: result.AccountID, GroupID: result.GroupID, StrategyID: result.StrategyID, ServiceName: result.ServiceName}
		c.registrationMu.Unlock()
		return
	}
	if env.EventType == "strategy.recommendation.result" {
		var result map[string]interface{}
		if err := decodePayload(env.Payload, &result); err != nil {
			slog.Warn("invalid recommendation result", "error", err)
			return
		}
		if result["accountId"] != c.cfg.AccountID || result["strategyId"] != c.cfg.StrategyID {
			return
		}
		slog.Info("recommendation result received", "event_id", env.EventID, "recommendation_id", result["recommendationId"], "status", result["recommendationStatus"])
	}
}

func (c *Client) candleLoop(ctx context.Context) {
	defer c.loops.Done()
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := c.candles.PollFetches(ctx)
		for _, err := range fetches.Errors() {
			slog.Warn("Kafka candle fetch failed; retrying", "topic", err.Topic, "partition", err.Partition, "error", err.Err)
		}
		for _, rec := range fetches.Records() {
			c.handleCandle(ctx, rec)
		}
	}
}

func (c *Client) handleCandle(ctx context.Context, rec *kgo.Record) {
	if !c.acceptCandles.Load() {
		return
	}
	var env contracts.Envelope
	if err := json.Unmarshal(rec.Value, &env); err != nil {
		slog.Warn("invalid candle envelope", "topic", rec.Topic, "error", err)
		c.commit(rec)
		return
	}
	if env.EventType != "market.candle.closed" {
		c.commit(rec)
		return
	}
	var candle contracts.Candle
	if err := decodePayload(env.Payload, &candle); err != nil {
		slog.Warn("invalid candle payload", "event_id", env.EventID, "error", err)
		c.commit(rec)
		return
	}
	if candle.Instrument.Ticker == "" || candle.RunID == "" || candle.SequenceNumber < 0 {
		slog.Warn("candle missing required fields", "event_id", env.EventID)
		c.commit(rec)
		return
	}
	started := time.Now()
	recommendationID := uuid.NewString()
	cause := env.EventID
	dataQuality, reason := "OK", "Momentum integration stub: safe HOLD"
	if candle.Instrument.LotSize < 1 || invalidPrice(candle.Close) {
		dataQuality, reason = "DEGRADED", "Invalid candle data; safe HOLD"
	}
	recommendation := contracts.Recommendation{RecommendationID: recommendationID, SourceEventID: env.EventID, RunID: candle.RunID, SecretKey: c.cfg.SecretKey, StrategyID: c.cfg.StrategyID, AccountID: c.cfg.AccountID, GroupID: c.cfg.GroupID, Instrument: candle.Instrument.Ticker, SequenceNumber: candle.SequenceNumber, Signal: "HOLD", Confidence: 0, SuggestedQuantity: 0, DataQuality: dataQuality, Reason: reason, ProcessingTimeMs: time.Since(started).Milliseconds(), Metadata: map[string]interface{}{}}
	resultEnv := contracts.Envelope{EventID: recommendationID, EventType: "strategy.recommendation.created", SchemaVersion: "1.0", OccurredAt: now(), CorrelationID: env.CorrelationID, CausationID: &cause, Producer: c.cfg.ServiceName, Payload: recommendation}
	// Retain event and recommendation IDs across retries of this offset.
	if err := c.produceJSON(ctx, c.cfg.RecommendationTopic, candle.Instrument.Ticker, resultEnv); err != nil {
		slog.Warn("recommendation publish failed; candle will be retried", "event_id", env.EventID, "error", err)
		return
	}
	c.processed.Add(1)
	c.published.Add(1)
	if err := c.candles.CommitRecords(ctx, rec); err != nil {
		slog.Warn("candle offset commit failed", "event_id", env.EventID, "error", err)
		return
	}
	slog.Info("candle processed", "event_id", env.EventID, "ticker", candle.Instrument.Ticker, "run_id", candle.RunID, "sequence", candle.SequenceNumber, "signal", "HOLD")
}

func (c *Client) heartbeatLoop(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	startedAt := now()
	instanceID := uuid.NewString()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !c.registered.Load() {
				continue
			}
			payload := contracts.Heartbeat{SecretKey: c.cfg.SecretKey, AccountID: c.cfg.AccountID, GroupID: c.cfg.GroupID, StrategyID: c.cfg.StrategyID, ServiceName: c.cfg.ServiceName, InstanceID: instanceID, Status: "UP", StartedAt: startedAt, SentAt: now(), ProcessedCandles: c.processed.Load(), PublishedRecommendations: c.published.Load(), Metadata: map[string]interface{}{"appVersion": c.cfg.AppVersion}}
			env := contracts.Envelope{EventID: uuid.NewString(), EventType: "account.service.heartbeat", SchemaVersion: "1.0", OccurredAt: now(), CorrelationID: uuid.NewString(), Producer: c.cfg.ServiceName, Payload: payload}
			if err := c.produceJSON(ctx, c.cfg.HeartbeatTopic, c.cfg.ServiceName, env); err != nil {
				slog.Warn("heartbeat publish failed", "error", err)
			}
		}
	}
}

func (c *Client) commit(rec *kgo.Record) {
	if err := c.candles.CommitRecords(context.Background(), rec); err != nil {
		slog.Warn("candle offset commit failed", "error", err)
	}
}
func (c *Client) produceJSON(ctx context.Context, topic, key string, value interface{}) error {
	body, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return c.producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: []byte(key), Value: body}).FirstErr()
}
func decodePayload(value interface{}, dest interface{}) error {
	raw, err := json.Marshal(value)
	if err != nil {
		return err
	}
	return json.Unmarshal(raw, dest)
}
func causation(env contracts.Envelope) string {
	if env.CausationID != nil {
		return *env.CausationID
	}
	return ""
}
func now() string { return time.Now().UTC().Format(time.RFC3339Nano) }

func invalidPrice(value string) bool {
	price, err := strconv.ParseFloat(value, 64)
	return err != nil || price <= 0
}
