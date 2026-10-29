package config

import "os"

type Config struct {
	AppName    string
	AppVersion string
	HTTPPort   string

	KafkaBootstrapServers string
	KafkaSecurityProtocol string
	KafkaSASLMechanism    string
	KafkaUsername         string
	KafkaPassword         string
	KafkaGroupID          string

	RegistrationTopic         string
	RegistrationResultTopic   string
	HeartbeatTopic            string
	RecommendationResultTopic string
	CandleTopic               string
	RecommendationTopic       string

	SecretKey    string
	AccountID    string
	AccountName  string
	StudentID    string
	GroupID      string
	StrategyID   string
	StrategyName string
	ServiceName  string
}

func Load() Config {
	return Config{
		AppName:    getEnv("APP_NAME", "Stockio"),
		AppVersion: getEnv("APP_VERSION", "1.0.0"),
		HTTPPort:   getEnv("HTTP_PORT", "8080"),

		KafkaBootstrapServers: getEnv("KAFKA_BOOTSTRAP_SERVERS", "omsu.7bits.it:9094"),
		KafkaSecurityProtocol: getEnv("KAFKA_SECURITY_PROTOCOL", "SASL_SSL"),
		KafkaSASLMechanism:    getEnv("KAFKA_SASL_MECHANISM", "SCRAM-SHA-256"),
		KafkaUsername:         getEnv("KAFKA_USERNAME", ""),
		KafkaPassword:         getEnv("KAFKA_PASSWORD", ""),
		KafkaGroupID:          getEnv("KAFKA_GROUP_ID", ""),

		RegistrationTopic:         getEnv("KAFKA_REGISTRATION_TOPIC", "account.registration.request"),
		RegistrationResultTopic:   getEnv("KAFKA_REGISTRATION_RESULT_TOPIC", "account.registration.result"),
		HeartbeatTopic:            getEnv("KAFKA_HEARTBEAT_TOPIC", "account.service.heartbeat"),
		CandleTopic:               getEnv("CANDLE_TOPIC", getEnv("KAFKA_CANDLE_TOPIC", "")),
		RecommendationTopic:       getEnv("KAFKA_RECOMMENDATION_TOPIC", "strategy.recommendation.created"),
		RecommendationResultTopic: getEnv("KAFKA_RECOMMENDATION_RESULT_TOPIC", "strategy.recommendation.result"),

		SecretKey:    getEnv("SECRET_KEY", ""),
		AccountID:    getEnv("ACCOUNT_ID", ""),
		AccountName:  getEnv("ACCOUNT_NAME", ""),
		GroupID:      getEnv("GROUP_ID", ""),
		StrategyID:   getEnv("STRATEGY_ID", ""),
		StrategyName: getEnv("STRATEGY_NAME", ""),
		ServiceName:  getEnv("SERVICE_NAME", "stockio"),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
