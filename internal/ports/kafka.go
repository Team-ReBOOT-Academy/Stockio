package ports

import (
	"context"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/domain"
)

type KafkaPublisher interface {
	Publish(ctx context.Context, topic string, key string, payload any) error
}

type KafkaSubscriber interface {
	StartConsuming(ctx context.Context) (<-chan *domain.Envelope, <-chan error)
	WaitForTopicAssignment(ctx context.Context, topic string) error
}
