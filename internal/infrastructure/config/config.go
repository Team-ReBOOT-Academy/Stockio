package config

import "os"

type Config struct {
	AppName    string
	AppVersion string
	HTTPPort   string
}

func Load() Config {
	return Config{
		AppName:    getEnv("APP_NAME", "Stockio"),
		AppVersion: getEnv("APP_VERSION", "1.0.0"),
		HTTPPort:   getEnv("HTTP_PORT", "8080"),
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
