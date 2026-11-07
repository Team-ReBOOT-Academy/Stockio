package application

type Settings struct {
	TopicRegistrationRequest  string
	TopicRegistrationResult   string
	TopicHeartbeat            string
	TopicRecommendationCreate string
	TopicRecommendationResult string
	CandleTopic               string

	AccountSecretKey string
	AccountID        string
	AccountName      string
	GroupID          string
	StrategyID       string
	StrategyName     string
	ServiceName      string
	Description      string
	Version          string
}
