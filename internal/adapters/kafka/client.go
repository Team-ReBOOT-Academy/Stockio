package kafka

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/config"
	"github.com/Team-ReBOOT-Academy/Stockio/internal/domain"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Client struct {
	producer *kgo.Client
	consumer *kgo.Client
	logger   *slog.Logger
	records  map[string]*kgo.Record
	assigned chan map[string][]int32
}

func NewClient(cfg *config.Config, logger *slog.Logger) (*Client, error) {
	saslOpt := kgo.SASL(scram.Auth{
		User: cfg.KafkaUsername,
		Pass: cfg.KafkaPassword,
	}.AsSha256Mechanism())
	tlsOpt := kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12})

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers),
		saslOpt,
		tlsOpt,
		kgo.DialTimeout(10*time.Second),
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka producer: %w", err)
	}

	assigned := make(chan map[string][]int32, 1)
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBrokers),
		saslOpt,
		tlsOpt,
		kgo.DialTimeout(10*time.Second),
		kgo.ConsumerGroup(cfg.KafkaGroupID),
		kgo.ConsumeTopics(cfg.CandleTopic, cfg.RegistrationResultTopic, cfg.RecommendationResultTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, partitions map[string][]int32) {
			select {
			case assigned <- partitions:
			default:
			}
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka consumer: %w", err)
	}

	return &Client{
		producer: producer,
		consumer: consumer,
		logger:   logger,
		records:  make(map[string]*kgo.Record),
		assigned: assigned,
	}, nil
}

func (c *Client) WaitForTopicAssignment(ctx context.Context, topic string) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case partitions := <-c.assigned:
		if len(partitions[topic]) == 0 {
			return fmt.Errorf("Kafka did not assign any partitions for topic %s", topic)
		}
		return nil
	}
}

func (c *Client) Publish(ctx context.Context, topic string, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	record := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
	}

	produceCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	return c.producer.ProduceSync(produceCtx, record).FirstErr()
}

func (c *Client) StartConsuming(ctx context.Context) (<-chan *domain.Envelope, <-chan error) {
	out := make(chan *domain.Envelope, 100)
	errs := make(chan error, 10)

	go func() {
		defer close(out)
		defer close(errs)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				fetches := c.consumer.PollFetches(ctx)
				if fetches.IsClientClosed() || ctx.Err() != nil {
					return
				}
				fetches.EachError(func(t string, p int32, err error) {
					select {
					case errs <- fmt.Errorf("kafka fetch error topic %s partition %d: %w", t, p, err):
					case <-ctx.Done():
					}
				})
				fetches.EachRecord(func(r *kgo.Record) {
					var env domain.Envelope
					if err := json.Unmarshal(r.Value, &env); err != nil {
						select {
						case errs <- fmt.Errorf("unmarshal error on topic %s: %w", r.Topic, err):
						case <-ctx.Done():
						}
						return
					}
					select {
					case out <- &env:
					case <-ctx.Done():
					}
				})
			}
		}
	}()

	return out, errs
}

func (c *Client) Close() {
	c.producer.Close()
	c.consumer.Close()
}
