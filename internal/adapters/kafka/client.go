package kafka

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/application/ports"
)

type Options struct {
	Brokers                 []string
	Username                string
	Password                string
	GroupID                 string
	Topics                  []string
	RegistrationResultTopic string
}

type Client struct {
	client   *kgo.Client
	assigned chan struct{}
}

func New(opts Options) (*Client, error) {
	assigned := make(chan struct{}, 1)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{User: opts.Username, Pass: opts.Password}.AsSha256Mechanism()),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.ConsumeTopics(opts.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assignment map[string][]int32) {
			if len(assignment[opts.RegistrationResultTopic]) == 0 {
				return
			}
			select {
			case assigned <- struct{}{}:
			default:
			}
		}),
	)
	if err != nil {
		return nil, err
	}
	return &Client{client: client, assigned: assigned}, nil
}

func (c *Client) Assigned() <-chan struct{} { return c.assigned }

func (c *Client) Poll(ctx context.Context) ports.FetchBatch {
	fetches := c.client.PollFetches(ctx)
	batch := ports.FetchBatch{}
	if ctx.Err() != nil {
		return batch
	}
	fetches.EachError(func(topic string, partition int32, err error) {
		batch.Errors = append(batch.Errors, ports.FetchError{Topic: topic, Partition: partition, Err: err})
	})
	fetches.EachRecord(func(rec *kgo.Record) {
		batch.Records = append(batch.Records, ports.NewRecord(
			rec.Topic, rec.Partition, rec.Offset, append([]byte(nil), rec.Value...),
			func() { c.client.MarkCommitRecords(rec) },
		))
	})
	return batch
}

func (c *Client) Publish(ctx context.Context, topic string, key, value []byte, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return c.client.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).FirstErr()
}

func (c *Client) Ping(ctx context.Context) error { return c.client.Ping(ctx) }

func (c *Client) Close() { c.client.Close() }
