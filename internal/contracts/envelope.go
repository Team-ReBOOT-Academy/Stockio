package contracts

type Envelope struct {
	EventID       string      `json:"eventID"`
	EventType     string      `json:"eventType"`
	SchemaVersion string      `json:"schemaVersion"`
	OccurredAt    string      `json:"occurredAt"`
	CorrelationID string      `json:"correlationId"`
	CausationID   *string     `json:"causationId"`
	Producer      string      `json:"producer"`
	Payload       interface{} `json:"payload"`
}
