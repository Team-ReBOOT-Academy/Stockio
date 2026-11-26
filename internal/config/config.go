package config

import (
	"fmt"
	"os"
)

type Config struct {
	KafkaBrokers              string
	KafkaUsername             string
	KafkaPassword             string
	KafkaGroupID              string
	CandleTopic               string
	RegistrationResultTopic   string
	HeartbeatTopic            string
	RecommendationTopic       string
	RecommendationResultTopic string

	AccountSecretKey string
	AccountID        string
	AccountName      string
	GroupID          string
	StrategyID       string
	StrategyName     string
	ServiceName      string

	HTTPPort string
}

func Load() (*Config, error) {
	cfg := &Config{
		KafkaBrokers:              getEnv("KAFKA_BROKERS", "omsu.7bits.it:9094"),
		KafkaUsername:             getEnv("KAFKA_USERNAME", ""),
		KafkaPassword:             getEnv("KAFKA_PASSWORD", ""),
		KafkaGroupID:              getEnv("KAFKA_GROUP_ID", ""),
		CandleTopic:               getEnv("CANDLE_TOPIC", ""),
		RegistrationResultTopic:   getEnv("REGISTRATION_RESULT_TOPIC", "account.registration.result"),
		HeartbeatTopic:            getEnv("HEARTBEAT_TOPIC", "account.service.heartbeat"),
		RecommendationTopic:       getEnv("RECOMMENDATION_TOPIC", "strategy.recommendation.created"),
		RecommendationResultTopic: getEnv("RECOMMENDATION_RESULT_TOPIC", "strategy.recommendation.result"),

		AccountSecretKey: getEnv("ACCOUNT_SECRET_KEY", ""),
		AccountID:        getEnv("ACCOUNT_ID", ""),
		AccountName:      getEnv("ACCOUNT_NAME", ""),
		GroupID:          getEnv("GROUP_ID", ""),
		StrategyID:       getEnv("STRATEGY_ID", ""),
		StrategyName:     getEnv("STRATEGY_NAME", ""),
		ServiceName:      getEnv("SERVICE_NAME", ""),

		HTTPPort: getEnv("HTTP_PORT", ":8080"),
	}

	required := map[string]string{
		"KAFKA_USERNAME":     cfg.KafkaUsername,
		"KAFKA_PASSWORD":     cfg.KafkaPassword,
		"KAFKA_GROUP_ID":     cfg.KafkaGroupID,
		"CANDLE_TOPIC":       cfg.CandleTopic,
		"ACCOUNT_SECRET_KEY": cfg.AccountSecretKey,
		"ACCOUNT_ID":         cfg.AccountID,
		"ACCOUNT_NAME":       cfg.AccountName,
		"GROUP_ID":           cfg.GroupID,
		"STRATEGY_ID":        cfg.StrategyID,
		"STRATEGY_NAME":      cfg.StrategyName,
		"SERVICE_NAME":       cfg.ServiceName,
	}
	for name, value := range required {
		if value == "" {
			return nil, fmt.Errorf("missing required environment variable %s", name)
		}
	}

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value, exists := os.LookupEnv(key); exists {
		return value
	}
	return defaultValue
}
