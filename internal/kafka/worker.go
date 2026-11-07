package kafka

import (
	"context"
	"crypto/tls"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Options struct {
	Brokers    []string
	Username   string
	Password   string
	GroupID    string
	Topics     []string
	OnAssigned func(map[string][]int32)
}

func New(opts Options) (*kgo.Client, error) {
	onAssigned := opts.OnAssigned
	if onAssigned == nil {
		onAssigned = func(map[string][]int32) {}
	}
	return kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}),
		kgo.SASL(scram.Auth{
			User: opts.Username,
			Pass: opts.Password,
		}.AsSha256Mechanism()),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.ConsumeTopics(opts.Topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			onAssigned(assigned)
		}),
	)
}
