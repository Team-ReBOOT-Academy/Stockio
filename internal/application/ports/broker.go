package ports

import (
	"context"
	"time"
)

type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	Value     []byte
	mark      func()
}

func NewRecord(topic string, partition int32, offset int64, value []byte, mark func()) Record {
	return Record{Topic: topic, Partition: partition, Offset: offset, Value: value, mark: mark}
}

func (r Record) MarkProcessed() {
	if r.mark != nil {
		r.mark()
	}
}

type FetchError struct {
	Topic     string
	Partition int32
	Err       error
}

type FetchBatch struct {
	Records []Record
	Errors  []FetchError
}

type Broker interface {
	Assigned() <-chan struct{}
	Poll(context.Context) FetchBatch
	Publish(context.Context, string, []byte, []byte, time.Duration) error
	Ping(context.Context) error
	Close()
}
