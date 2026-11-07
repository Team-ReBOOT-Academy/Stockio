package config

import (
	"fmt"
	"os"
	"strings"

	"github.com/Team-ReBOOT-Academy/Stockio/internal/application"
)

type Config struct {
	application.Settings
	KafkaBootstrap []string
	KafkaUsername  string
	KafkaPassword  string
	KafkaGroupID   string
	HTTPAddr       string
}

func Load() (Config, error) {
	cfg := Config{
		KafkaBootstrap: splitCSV(getenv("KAFKA_BOOTSTRAP_SERVERS", "omsu.7bits.it:9094")),
		KafkaUsername:  strings.TrimSpace(os.Getenv("KAFKA_USERNAME")),
		KafkaPassword:  strings.TrimSpace(os.Getenv("KAFKA_PASSWORD")),
		KafkaGroupID:   strings.TrimSpace(os.Getenv("KAFKA_GROUP_ID")),
		HTTPAddr:       getenv("HTTP_ADDR", ":"+getenv("HTTP_PORT", "8080")),
	}
	cfg.Settings = application.Settings{
		TopicRegistrationRequest:  getenv("KAFKA_REGISTRATION_TOPIC", "account.registration.request"),
		TopicRegistrationResult:   getenv("KAFKA_REGISTRATION_RESULT_TOPIC", "account.registration.result"),
		TopicHeartbeat:            getenv("KAFKA_HEARTBEAT_TOPIC", "account.service.heartbeat"),
		TopicRecommendationCreate: getenv("KAFKA_RECOMMENDATION_TOPIC", "strategy.recommendation.created"),
		TopicRecommendationResult: getenv("KAFKA_RECOMMENDATION_RESULT_TOPIC", "strategy.recommendation.result"),
		CandleTopic:               strings.TrimSpace(os.Getenv("CANDLE_TOPIC")),

		AccountSecretKey: strings.TrimSpace(os.Getenv("SECRET_KEY")),
		AccountID:        strings.TrimSpace(os.Getenv("ACCOUNT_ID")),
		AccountName:      strings.TrimSpace(os.Getenv("ACCOUNT_NAME")),
		GroupID:          strings.TrimSpace(os.Getenv("GROUP_ID")),
		StrategyID:       strings.TrimSpace(os.Getenv("STRATEGY_ID")),
		StrategyName:     strings.TrimSpace(os.Getenv("STRATEGY_NAME")),
		ServiceName:      strings.TrimSpace(os.Getenv("SERVICE_NAME")),
		Description:      getenv("DESCRIPTION", "Stockio strategy service"),
		Version:          getenv("APP_VERSION", "1.0.0"),
	}
	if len(cfg.KafkaBootstrap) == 0 {
		return Config{}, fmt.Errorf("KAFKA_BOOTSTRAP_SERVERS must contain at least one broker")
	}
	missing := make([]string, 0)
	for key, value := range map[string]string{
		"KAFKA_USERNAME": cfg.KafkaUsername,
		"KAFKA_PASSWORD": cfg.KafkaPassword,
		"KAFKA_GROUP_ID": cfg.KafkaGroupID,
		"CANDLE_TOPIC":   cfg.CandleTopic,
		"SECRET_KEY":     cfg.AccountSecretKey,
		"ACCOUNT_ID":     cfg.AccountID,
		"ACCOUNT_NAME":   cfg.AccountName,
		"GROUP_ID":       cfg.GroupID,
		"STRATEGY_ID":    cfg.StrategyID,
		"STRATEGY_NAME":  cfg.StrategyName,
		"SERVICE_NAME":   cfg.ServiceName,
	} {
		if value == "" {
			missing = append(missing, key)
		}
	}
	if len(missing) > 0 {
		return Config{}, fmt.Errorf("missing required environment variables: %s", strings.Join(missing, ", "))
	}
	return cfg, nil
}

func getenv(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	brokers := make([]string, 0, len(parts))
	for _, part := range parts {
		if broker := strings.TrimSpace(part); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
