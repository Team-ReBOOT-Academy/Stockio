package contracts

type Heartbeat struct {
	SecretKey   string `json:"secretKey"`
	AccountID   string `json:"accountId"`
	GroupID     string `json:"groupId"`
	StrategyID  string `json:"strategyId"`
	ServiceName string `json:"serviceName"`
	InstanceID  string `json:"instanceId"`

	Status    string `json:"status"`
	StartedAt string `json:"startedAt"`
	SentAt    string `json:"sentAt"`

	ProcessedCandles            int64  `json:"processedCandles"`
	PublishedRecommendations    int64  `json:"publishedRecommendations"`
	LastProcessedSequenceNumber *int64 `json:"lastProcessedSequenceNumber"`

	Metadata map[string]interface{} `json:"metadata,omitempty"`
}
