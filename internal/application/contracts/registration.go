package contracts

type RegistrationPayload struct {
	SecretKey    string `json:"secretKey"`
	AccountID    string `json:"accountId"`
	AccountName  string `json:"accountName"`
	GroupID      string `json:"groupId"`
	StrategyID   string `json:"strategyId"`
	StrategyName string `json:"strategyName"`
	ServiceName  string `json:"serviceName"`
	Description  string `json:"description,omitempty"`
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
