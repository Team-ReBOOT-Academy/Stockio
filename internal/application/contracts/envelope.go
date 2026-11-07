package contracts

import (
	"encoding/json"
	"time"
)

const SchemaVersion = "1.0"

type Envelope[T any] struct {
	EventID       string  `json:"eventId"`
	EventType     string  `json:"eventType"`
	SchemaVersion string  `json:"schemaVersion"`
	OccurredAt    string  `json:"occurredAt"`
	CorrelationID string  `json:"correlationId"`
	CausationID   *string `json:"causationId"`
	Producer      string  `json:"producer"`
	Payload       T       `json:"payload"`
}

type RawEnvelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	SchemaVersion string          `json:"schemaVersion"`
	OccurredAt    string          `json:"occurredAt"`
	CorrelationID string          `json:"correlationId"`
	CausationID   *string         `json:"causationId"`
	Producer      string          `json:"producer"`
	Payload       json.RawMessage `json:"payload"`
}

func NowUTC() string { return time.Now().UTC().Format(time.RFC3339) }

func DecodeEnvelope(data []byte) (RawEnvelope, error) {
	var envelope RawEnvelope
	err := json.Unmarshal(data, &envelope)
	return envelope, err
}

func DecodePayload[T any](envelope RawEnvelope) (T, error) {
	var payload T
	err := json.Unmarshal(envelope.Payload, &payload)
	return payload, err
}

func MarshalEnvelope[T any](envelope Envelope[T]) ([]byte, error) {
	return json.Marshal(envelope)
}
