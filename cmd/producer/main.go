package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/streadway/amqp"
	"go.uber.org/zap"

	"github.com/mhd7966/async-battle/configs"
)

var config *configs.Config

func main() {
	// Load configuration from .env or environment
	cfg, err := configs.Configs(".env")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	config = cfg

	zap.S().Error(config)
	// Example usage: produce 1000 messages with a sample payload
	payload := map[string]interface{}{"data": "test-message"}
	results := ProduceHighLoad(2000, payload)
	for broker, result := range results {
		fmt.Printf("Broker: %s, Result: %s\n", broker, result)
	}
}

// BrokerConfig holds enable/disable flags for each broker
var (
	enableRedis    = false // Set to true to enable Redis producer
	enableKafka    = false  // Set to true to enable Kafka producer
	enableRabbitMQ = true // Set to true to enable RabbitMQ producer
	enableNATS     = false // Set to true to enable NATS producer
	enableRedpanda = false // Set to true to enable Redpanda producer

// To enable or disable a broker, comment or uncomment the corresponding line below.
)

// Message is the structure sent to brokers
type Message struct {
	ID      string                 `json:"id"`
	Payload map[string]interface{} `json:"payload"`
}

// High load producer function
// Call this function to produce high load messages to enabled brokers
func ProduceHighLoad(count int, payload map[string]interface{}) map[string]string {
	results := make(map[string]string)
	if count <= 0 {
		count = 1000 // default
	}
	payload = map[string]interface{}{
		"data": strings.Repeat("test-message-", 1), // حدود 700-800 بایت
	}

	if enableRedis {
		if err := produceHighLoadToRedis(count, payload); err != nil {
			results["redis"] = err.Error()
		} else {
			results["redis"] = "success"
		}
	}
	if enableKafka {
		if err := produceHighLoadToKafka(count, payload); err != nil {
			results["kafka"] = err.Error()
		} else {
			results["kafka"] = "success"
		}
	}
	if enableRabbitMQ {
		if err := produceHighLoadToRabbitMQ(count, payload); err != nil {
			results["rabbitmq"] = err.Error()
		} else {
			results["rabbitmq"] = "success"
		}
	}
	if enableNATS {
		if err := produceHighLoadToNATS(count, payload); err != nil {
			results["nats"] = err.Error()
		} else {
			results["nats"] = "success"
		}
	}
	if enableRedpanda {
		if err := produceHighLoadToRedpanda(count, payload); err != nil {
			results["redpanda"] = err.Error()
		} else {
			results["redpanda"] = "success"
		}
	}
	return results
}

// Dummy implementations for high load producers
func produceHighLoadToRedis(count int, payload map[string]interface{}) error {
	for i := 0; i < count; i++ {
		msg := Message{ID: fmt.Sprintf("%d", i), Payload: payload}
		if err := sendToRedis(msg); err != nil {
			return err
		}
	}
	return nil
}
func produceHighLoadToKafka(count int, payload map[string]interface{}) error {
	const workerCount = 10
	type job struct {
		Index int
	}
	jobs := make(chan job, count)
	errCh := make(chan error, count)
	var wg sync.WaitGroup

	// Start 10 worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				msg := Message{ID: fmt.Sprintf("%d", j.Index), Payload: payload}
				if err := sendToKafka(msg); err != nil {
					errCh <- err
				}
			}
		}()
	}

	// Send jobs
	for i := 0; i < count; i++ {
		jobs <- job{Index: i}
	}
	close(jobs)

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func produceHighLoadToRabbitMQ(count int, payload map[string]interface{}) error {
	const workerCount = 10
	type job struct {
		Index int
	}
	jobs := make(chan job, count)
	errCh := make(chan error, count)
	var wg sync.WaitGroup

	// Start 10 worker goroutines
	for w := 0; w < workerCount; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				msg := Message{ID: fmt.Sprintf("%d", j.Index), Payload: payload}
				if err := sendToRabbitMQ(msg); err != nil {
					errCh <- err
				}
			}
		}()
	}

	// Send jobs
	for i := 0; i < count; i++ {
		jobs <- job{Index: i}
	}
	close(jobs)

	wg.Wait()
	close(errCh)

	for err := range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

//	func produceHighLoadToRabbitMQ(count int, payload map[string]interface{}) error {
//		for i := 0; i < count; i++ {
//			msg := Message{ID: fmt.Sprintf("%d", i), Payload: payload}
//			if err := sendToRabbitMQ(msg); err != nil {
//				return err
//			}
//		}
//		return nil
//	}
func produceHighLoadToNATS(count int, payload map[string]interface{}) error {
	for i := 0; i < count; i++ {
		msg := Message{ID: fmt.Sprintf("%d", i), Payload: payload}
		if err := sendToNATS(msg); err != nil {
			return err
		}
	}
	return nil
}
func produceHighLoadToRedpanda(count int, payload map[string]interface{}) error {
	for i := 0; i < count; i++ {
		msg := Message{ID: fmt.Sprintf("%d", i), Payload: payload}
		if err := sendToRedpanda(msg); err != nil {
			return err
		}
	}
	return nil
}

// RegisterRedis initializes and returns a Redis client
func RegisterRedis(ctx context.Context) (*redis.Client, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", config.Redis.Host, config.Redis.Port),
		Password: config.Redis.Password,
		DB:       config.Redis.DB,
	})

	// Test the connection
	_, err := client.Ping(ctx).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %w", err)
	}

	return client, nil
}

// Message struct assumed to be defined elsewhere
// type Message struct {
//     ID      string
//     Payload map[string]interface{}
// }

// sendToRedis sends a message to a Redis stream
func sendToRedis(msg Message) error {
	ctx := context.Background()
	client, err := RegisterRedis(ctx)
	if err != nil {
		return fmt.Errorf("sendToRedis: failed to connect to Redis: %w", err)
	}
	defer client.Close()

	stream := config.Redis.Stream
	values := make(map[string]interface{})
	values["id"] = msg.ID
	for k, v := range msg.Payload {
		values[k] = v
	}

	_, err = client.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: values,
	}).Result()
	if err != nil {
		return fmt.Errorf("sendToRedis: failed to XAdd: %w", err)
	}
	return nil
}

// sendToKafka sends a message to a Kafka topic
func sendToKafka(msg Message) error {
	kafkaCfg := config.Kafka
	brokers := kafkaCfg.Brokers
	topic := kafkaCfg.Topic

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_1_0_0
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.ClientID = kafkaCfg.ClientID

	producer, err := sarama.NewSyncProducer(brokers, saramaCfg)
	if err != nil {
		return fmt.Errorf("sendToKafka: failed to create producer: %w", err)
	}
	defer producer.Close()

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("sendToKafka: failed to marshal payload: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(msg.ID),
		Value: sarama.ByteEncoder(payloadBytes),
	}

	_, _, err = producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("sendToKafka: failed to send message: %w", err)
	}
	return nil
}

// sendToRabbitMQ sends a message to a RabbitMQ queue
func sendToRabbitMQ(msg Message) error {
	rmqCfg := config.RabbitMQ
	addr := fmt.Sprintf("amqp://%s:%s@%s:%d%s", rmqCfg.User, rmqCfg.Password, rmqCfg.Host, rmqCfg.Port, rmqCfg.VHost)

	conn, err := amqp.Dial(addr)
	if err != nil {
		return fmt.Errorf("sendToRabbitMQ: failed to connect: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("sendToRabbitMQ: failed to open channel: %w", err)
	}
	defer ch.Close()

	// Declare exchange and queue
	if err := ch.ExchangeDeclare(
		rmqCfg.Exchange, "direct", true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("sendToRabbitMQ: failed to declare exchange: %w", err)
	}
	if _, err := ch.QueueDeclare(
		rmqCfg.Queue, true, false, false, false, nil,
	); err != nil {
		return fmt.Errorf("sendToRabbitMQ: failed to declare queue: %w", err)
	}
	if err := ch.QueueBind(
		rmqCfg.Queue, rmqCfg.Queue, rmqCfg.Exchange, false, nil,
	); err != nil {
		return fmt.Errorf("sendToRabbitMQ: failed to bind queue: %w", err)
	}

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("sendToRabbitMQ: failed to marshal payload: %w", err)
	}

	err = ch.Publish(
		rmqCfg.Exchange,
		rmqCfg.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			MessageId:   msg.ID,
			Body:        payloadBytes,
			Timestamp:   time.Now(),
		},
	)
	if err != nil {
		return fmt.Errorf("sendToRabbitMQ: failed to publish: %w", err)
	}
	return nil
}

// sendToNATS sends a message to a NATS subject
func sendToNATS(msg Message) error {
	natsCfg := config.NATS
	nc, err := nats.Connect(natsCfg.URL)
	if err != nil {
		return fmt.Errorf("sendToNATS: failed to connect: %w", err)
	}
	defer nc.Close()

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("sendToNATS: failed to marshal payload: %w", err)
	}

	subject := natsCfg.Subject
	if subject == "" {
		subject = "test.subject"
	}

	if err := nc.Publish(subject, payloadBytes); err != nil {
		return fmt.Errorf("sendToNATS: failed to publish: %w", err)
	}
	return nil
}

// sendToRedpanda sends a message to a Redpanda topic (Kafka compatible)
func sendToRedpanda(msg Message) error {
	redpandaCfg := config.Redpanda
	brokers := redpandaCfg.Brokers
	topic := redpandaCfg.Topic

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_1_0_0
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.ClientID = redpandaCfg.ClientID

	producer, err := sarama.NewSyncProducer(brokers, saramaCfg)
	if err != nil {
		return fmt.Errorf("sendToRedpanda: failed to create producer: %w", err)
	}
	defer producer.Close()

	payloadBytes, err := json.Marshal(msg.Payload)
	if err != nil {
		return fmt.Errorf("sendToRedpanda: failed to marshal payload: %w", err)
	}

	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(msg.ID),
		Value: sarama.ByteEncoder(payloadBytes),
	}

	_, _, err = producer.SendMessage(message)
	if err != nil {
		return fmt.Errorf("sendToRedpanda: failed to send message: %w", err)
	}
	return nil
}
