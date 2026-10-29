package contracts

type RegistrationRequest struct {
	SecretKey    string `json:"secretKey"`
	AccountID    string `json:"accountId"`
	AccountName  string `json:"accountName"`
	GroupID      string `json:"groupId"`
	StrategyID   string `json:"strategyId"`
	StrategyName string `json:"strategyName"`
	ServiceName  string `json:"serviceName"`
	Description  string `json:"description"`
}

type RegistrationResult struct {
	SecretKey   string `json:"secretKey"`
	AccountID   string `json:"accountId"`
	GroupID     string `json:"groupId"`
	StrategyID  string `json:"strategyId"`
	ServiceName string `json:"serviceName"`
	Correlation string `json:"correlation"`
	Status      string `json:"status"`
	Reason      string `json:"reason"`
}
