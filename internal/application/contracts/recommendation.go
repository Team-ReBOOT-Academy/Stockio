package contracts

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

type RecommendationResultPayload struct {
	RecommendationID     string `json:"recommendationId"`
	SourceEventID        string `json:"sourceEventId"`
	AccountID            string `json:"accountId"`
	StrategyID           string `json:"strategyId"`
	Instrument           string `json:"instrument"`
	SequenceNumber       int64  `json:"sequenceNumber"`
	RecommendationStatus string `json:"recommendationStatus"`
	StatusReason         string `json:"statusReason"`
	AccountSignal        string `json:"accountSignal"`
}
