package contracts

type Recommendation struct {
	RecommendationID  string  `json:"recommendationId"`
	SourceEventID     string  `json:"sourceEventId"`
	RunID             string  `json:"runId"`
	SecretKey         string  `json:"secretKey"`
	StrategyID        string  `json:"strategyId"`
	AccountID         string  `json:"accountId"`
	GroupID           string  `json:"groupId"`
	Instrument        string  `json:"instrument"`
	SequenceNumber    int64   `json:"sequenceNumber"`
	Signal            string  `json:"signal"`
	Confidence        float64 `json:"confidence"`
	SuggestedQuantity int     `json:"suggestedQuantity"`

	DataQuality      string                 `json:"dataQuality"`
	Reason           string                 `json:"reason"`
	ProcessingTimeMs int64                  `json:"processingTimeMs"`
	Metadata         map[string]interface{} `json:"metadata"`
}
