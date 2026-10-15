package contracts

type RegistrationRequest struct {
	SecretKey    string `json:"secretKey"`
	AccountId    string `json:"accountId"`
	AccountName  string `json:"accountName"`
	GroupId      string `json:"groupId"`
	StrategyID   string `json:"strategyId"`
	StrategyName string `json:"strategyName"`
	ServiceName  string `json:"serviceName"`
	Description  string `json:"description"`
}

type RegistrationResult struct {
	ServiceName string `json:"serviceName"`
	Correlation string `json:"correlation"`
	Status      string `json:"status"`
	Reason      string `json:"reason"`
}
