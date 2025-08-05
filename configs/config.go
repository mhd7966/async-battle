package configs

import (
	"fmt"
	"log"
	"os"

	"github.com/ilyakaznacheev/cleanenv"
)

// Config holds all configuration for the application
type Config struct {
	App struct {
		Debug      bool   `env:"DEBUG" envDefault:"true"`
		Env        string `env:"ENV" envDefault:"development"`
		InstanceID string `env:"INSTANCE_ID" envDefault:"async-battle-app"`
		Port       int    `env:"PORT" envDefault:"8080"`
	} `env-prefix:"APP_"`

	Redis struct {
		Host     string `env:"HOST" envDefault:"localhost"`
		Port     int    `env:"PORT" envDefault:"6379"`
		Password string `env:"PASSWORD" envDefault:""`
		DB       int    `env:"DB" envDefault:"0"`
		Stream   string `env:"STREAM" envDefault:"test-stream"`
	} `env-prefix:"REDIS_"`

	Kafka struct {
		Brokers              []string `env:"BROKERS" envDefault:"kafka:9092"`
		Topic                string   `env:"TOPIC" envDefault:"test-topic"`
		GroupID              string   `env:"GROUP_ID" envDefault:"test-group"`
		ClientID             string   `env:"CLIENT_ID" envDefault:"async-battle-client"`
		MaxMessagesPerSecond int      `env:"MAX_MESSAGES_PER_SECOND" envDefault:"20"`
	} `env-prefix:"KAFKA_"`

	RabbitMQ struct {
		Host     string `env:"HOST" envDefault:"localhost"`
		Port     int    `env:"PORT" envDefault:"5672"`
		User     string `env:"USER" envDefault:"guest"`
		Password string `env:"PASSWORD" envDefault:"guest"`
		VHost    string `env:"VHOST" envDefault:"/"`
		Queue    string `env:"QUEUE" envDefault:"test-queue"`
		Exchange string `env:"EXCHANGE" envDefault:"test-exchange"`
	} `env-prefix:"RABBITMQ_"`

	NATS struct {
		URL      string `env:"URL" envDefault:"nats://localhost:4222"`
		Stream   string `env:"STREAM" envDefault:"test-stream"`
		Subject  string `env:"SUBJECT" envDefault:"test.subject"`
		Consumer string `env:"CONSUMER" envDefault:"test-consumer"`
	} `env-prefix:"NATS_"`

	Redpanda struct {
		Brokers  []string `env:"BROKERS" envDefault:"localhost:9092"`
		Topic    string   `env:"TOPIC" envDefault:"test-topic"`
		GroupID  string   `env:"GROUP_ID" envDefault:"test-group"`
		ClientID string   `env:"CLIENT_ID" envDefault:"async-battle-client"`
	} `env-prefix:"REDPANDA_"`

	Log struct {
		SentryDSN string `env:"SENTRY_DSN" envDefault:""`
		Level     string `env:"LEVEL" envDefault:"info"`
	} `env-prefix:"LOG_"`
}

// Configs loads configuration from environment variables and .env file
func Configs(envFile string) (*Config, error) {
	config := &Config{}

	// Load .env file if it exists
	if envFile != "" {
		if _, err := os.Stat(envFile); err == nil {
			if err := cleanenv.ReadConfig(envFile, config); err != nil {
				return nil, fmt.Errorf("error reading config file %s: %w", envFile, err)
			}
		}
	}

	// Read environment variables (this will override any values from .env file)
	if err := cleanenv.ReadEnv(config); err != nil {
		return nil, fmt.Errorf("error reading environment variables: %w", err)
	}

	log.Printf("Loaded configuration: %+v", config)

	return config, nil
}
