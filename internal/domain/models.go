package domain

import (
	"encoding/json"

	"github.com/google/uuid"
)

func GenerateUUID() string {
	return uuid.New().String()
}

type Envelope struct {
	EventID       string          `json:"eventId"`
	EventType     string          `json:"eventType"`
	SchemaVersion string          `json:"schemaVersion"`
	OccurredAt    string          `json:"occurredAt"`
	CorrelationID string          `json:"correlationId"`
	ChainID       string          `json:"chainId,omitempty"`
	CausationID   *string         `json:"causationId"`
	Producer      string          `json:"producer"`
	Payload       json.RawMessage `json:"payload"`
}

type RegistrationRequestPayload struct {
	SecretKey    string `json:"secretKey"`
	AccountID    string `json:"accountId"`
	AccountName  string `json:"accountName"`
	GroupID      string `json:"groupId"`
	StrategyID   string `json:"strategyId"`
	StrategyName string `json:"strategyName"`
	ServiceName  string `json:"serviceName"`
	Description  string `json:"description"`
}

type RegistrationResultPayload struct {
	SecretKey   string `json:"secretKey"`
	AccountID   string `json:"accountId"`
	GroupID     string `json:"groupId"`
	StrategyID  string `json:"strategyId"`
	ServiceName string `json:"serviceName"`
	Status      string `json:"status"`
	Reason      string `json:"reason"`
}

type CandlePayload struct {
	RunID          string     `json:"runId"`
	GroupID        string     `json:"groupId"`
	Instrument     Instrument `json:"instrument"`
	Timeframe      string     `json:"timeframe"`
	SequenceNumber int64      `json:"sequenceNumber"`
	CandleTime     string     `json:"candleTime"`
	TradingDate    string     `json:"tradingDate"`
	SessionPhase   string     `json:"sessionPhase"`
	Open           string     `json:"open"`
	High           string     `json:"high"`
	Low            string     `json:"low"`
	Close          string     `json:"close"`
	Volume         int64      `json:"volume"`
	IsWarmup       bool       `json:"isWarmup"`
}

type Instrument struct {
	Ticker   string `json:"ticker"`
	Figi     string `json:"figi"`
	Currency string `json:"currency"`
	LotSize  int    `json:"lotSize"`
}

type RecommendationPayload struct {
	RecommendationID  string         `json:"recommendationId"`
	SourceEventID     string         `json:"sourceEventId"`
	RunID             string         `json:"runId"`
	SecretKey         string         `json:"secretKey"`
	StrategyID        string         `json:"strategyId"`
	AccountID         string         `json:"accountId"`
	GroupID           string         `json:"groupId"`
	Instrument        string         `json:"instrument"`
	SequenceNumber    int64          `json:"sequenceNumber"`
	Signal            string         `json:"signal"`
	Confidence        float64        `json:"confidence"`
	SuggestedQuantity int            `json:"suggestedQuantity"`
	DataQuality       string         `json:"dataQuality"`
	Reason            string         `json:"reason"`
	ProcessingTimeMs  int64          `json:"processingTimeMs"`
	Metadata          map[string]any `json:"metadata"`
}

type HeartbeatPayload struct {
	SecretKey                   string         `json:"secretKey"`
	AccountID                   string         `json:"accountId"`
	GroupID                     string         `json:"groupId"`
	StrategyID                  string         `json:"strategyId"`
	ServiceName                 string         `json:"serviceName"`
	InstanceID                  string         `json:"instanceId"`
	Status                      string         `json:"status"`
	StartedAt                   string         `json:"startedAt"`
	SentAt                      string         `json:"sentAt"`
	ProcessedCandles            int64          `json:"processedCandles"`
	PublishedRecommendations    int64          `json:"publishedRecommendations"`
	LastProcessedSequenceNumber *int64         `json:"lastProcessedSequenceNumber"`
	Metadata                    map[string]any `json:"metadata"`
}
