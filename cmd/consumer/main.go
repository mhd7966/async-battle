package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/go-redis/redis/v8"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/streadway/amqp"

	"github.com/mhd7966/async-battle/configs"
)

var config *configs.Config

func main() {
	cfg, err := configs.Configs("")
	if err != nil {
		log.Fatalf("Error while loading configurations : %v", err)
	}
	config = cfg
	log.Println(cfg)
	log.Println(config)

	// Create Kafka topic before starting consumers
	if err := createKafkaTopic(); err != nil {
		log.Printf("Failed to create Kafka topic: %v", err)
	}

	// Start consumers for each broker
	// go RedisConsumer()
	// go KafkaConsumer()
	go RabbitMQConsumer()
	// go NATSConsumer()
	// go RedpandaConsumer()

	// Keep the main function running
	select {}
}

// createKafkaTopic creates the Kafka topic if it doesn't exist
func createKafkaTopic() error {
	kafkaCfg := config.Kafka
	topic := kafkaCfg.Topic
	brokers := kafkaCfg.Brokers

	log.Printf("Creating Kafka topic: %s", topic)

	// Create admin client
	adminClient, err := sarama.NewClusterAdmin(brokers, nil)
	if err != nil {
		return fmt.Errorf("failed to create Kafka admin client: %w", err)
	}
	defer adminClient.Close()

	// Check if topic already exists
	topics, err := adminClient.ListTopics()
	if err != nil {
		return fmt.Errorf("failed to list Kafka topics: %w", err)
	}

	if _, exists := topics[topic]; exists {
		log.Printf("Kafka topic %s already exists", topic)
		return nil
	}

	// Create topic with default configuration
	topicDetail := &sarama.TopicDetail{
		NumPartitions:     3,
		ReplicationFactor: 1,
		ConfigEntries:     make(map[string]*string),
	}

	err = adminClient.CreateTopic(topic, topicDetail, false)
	if err != nil {
		return fmt.Errorf("failed to create Kafka topic %s: %w", topic, err)
	}

	log.Printf("Successfully created Kafka topic: %s", topic)
	return nil
}

// --- Metrics and Prometheus Integration ---

var (
	consumerCount = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_consumer_count",
			Help: "Number of consumers for each broker",
		},
		[]string{"broker"},
	)
	messageConsumeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "broker_message_consume_duration_seconds",
			Help:    "Time taken to consume each message per broker",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"broker"},
	)
	totalMessagesConsumed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_total_messages_consumed",
			Help: "Total number of messages consumed per broker",
		},
		[]string{"broker"},
	)
	// Enhanced message rate metrics
	messagesPerSecond = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_messages_per_second",
			Help: "Current messages consumed per second per broker",
		},
		[]string{"broker"},
	)
	messagesPerMinute = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_messages_per_minute",
			Help: "Current messages consumed per minute per broker",
		},
		[]string{"broker"},
	)
	messagesPerHour = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_messages_per_hour",
			Help: "Current messages consumed per hour per broker",
		},
		[]string{"broker"},
	)
	// Message rate over different time windows
	messageRate1m = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_message_rate_1m",
			Help: "Message consumption rate over 1-minute window per broker",
		},
		[]string{"broker"},
	)
	messageRate5m = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_message_rate_5m",
			Help: "Message consumption rate over 5-minute window per broker",
		},
		[]string{"broker"},
	)
	messageRate15m = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_message_rate_15m",
			Help: "Message consumption rate over 15-minute window per broker",
		},
		[]string{"broker"},
	)
	// Queue depth and consumer lag metrics
	queueDepth = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_queue_depth",
			Help: "Current queue depth (number of messages in queue) per broker",
		},
		[]string{"broker", "queue"},
	)
	consumerLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "broker_consumer_lag",
			Help: "Consumer lag (messages behind) per broker",
		},
		[]string{"broker", "queue"},
	)
	// Error metrics
	messageConsumeErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "broker_message_consume_errors",
			Help: "Total number of message consumption errors per broker",
		},
		[]string{"broker", "error_type"},
	)
)

func init() {
	prometheus.MustRegister(consumerCount)
	prometheus.MustRegister(messageConsumeDuration)
	prometheus.MustRegister(totalMessagesConsumed)
	prometheus.MustRegister(messagesPerSecond)
	prometheus.MustRegister(messagesPerMinute)
	prometheus.MustRegister(messagesPerHour)
	prometheus.MustRegister(messageRate1m)
	prometheus.MustRegister(messageRate5m)
	prometheus.MustRegister(messageRate15m)
	prometheus.MustRegister(queueDepth)
	prometheus.MustRegister(consumerLag)
	prometheus.MustRegister(messageConsumeErrors)

	// Start Prometheus HTTP server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Println("Prometheus metrics available at :2112/metrics")
		if err := http.ListenAndServe(":2112", nil); err != nil {
			log.Fatalf("Failed to start Prometheus HTTP server: %v", err)
		}
	}()
}

// Helper to track consumer count per broker
func IncConsumerCount(broker string) {
	consumerCount.WithLabelValues(broker).Inc()
}
func DecConsumerCount(broker string) {
	consumerCount.WithLabelValues(broker).Dec()
}

// Helper to observe message consumption duration and speed
func ObserveMessageConsumed(broker string, duration time.Duration) {
	messageConsumeDuration.WithLabelValues(broker).Observe(duration.Seconds())
	totalMessagesConsumed.WithLabelValues(broker).Inc()
}

// Helper to set message rate metrics
func SetMessageRate(broker string, ratePerSecond, ratePerMinute, ratePerHour float64) {
	messagesPerSecond.WithLabelValues(broker).Set(ratePerSecond)
	messagesPerMinute.WithLabelValues(broker).Set(ratePerMinute)
	messagesPerHour.WithLabelValues(broker).Set(ratePerHour)
}

// Helper to set message rate over time windows
func SetMessageRateWindows(broker string, rate1m, rate5m, rate15m float64) {
	messageRate1m.WithLabelValues(broker).Set(rate1m)
	messageRate5m.WithLabelValues(broker).Set(rate5m)
	messageRate15m.WithLabelValues(broker).Set(rate15m)
}

// Helper to set queue metrics
func SetQueueMetrics(broker, queue string, depth, lag float64) {
	queueDepth.WithLabelValues(broker, queue).Set(depth)
	consumerLag.WithLabelValues(broker, queue).Set(lag)
}

// Helper to increment error counter
func IncrementError(broker, errorType string) {
	messageConsumeErrors.WithLabelValues(broker, errorType).Inc()
}

// --- Instrumented Consumers ---

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

// RedisConsumer reads messages from a Redis stream and processes them.
func RedisConsumer() {
	broker := "redis"
	IncConsumerCount(broker)
	defer DecConsumerCount(broker)

	ctx := context.Background()

	client, err := RegisterRedis(ctx)
	if err != nil {
		log.Printf("Failed to connect to Redis: %v", err)
		return
	}
	defer client.Close()

	stream := config.Redis.Stream
	group := "async-battle-group"
	consumer := config.App.InstanceID

	// Create consumer group if it doesn't exist
	_, err = client.XGroupCreateMkStream(ctx, stream, group, "$").Result()
	if err != nil {
		// Ignore BUSYGROUP error (group already exists)
		if !strings.Contains(err.Error(), "BUSYGROUP") {
			log.Printf("Failed to create Redis consumer group: %v", err)
			return
		}
	}

	var lastTime = time.Now()

	for {
		res, err := client.XReadGroup(ctx, &redis.XReadGroupArgs{
			Group:    group,
			Consumer: consumer,
			Streams:  []string{stream, ">"},
			Count:    10,
			Block:    5 * time.Second,
		}).Result()

		if err != nil && err != redis.Nil {
			log.Printf("Error reading from Redis stream: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}

		var msgCount int64
		for _, streamRes := range res {
			for _, msg := range streamRes.Messages {
				start := time.Now()
				log.Printf("RedisConsumer received message: %v", msg.Values)
				_, err := client.XAck(ctx, stream, group, msg.ID).Result()
				if err != nil {
					log.Printf("Failed to ACK Redis message %s: %v", msg.ID, err)
				}
				ObserveMessageConsumed(broker, time.Since(start))
				msgCount++
			}
		}
		// Update rate metrics
		if msgCount > 0 {
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			if elapsed > 0 {
				ratePerSecond := float64(msgCount) / elapsed
				ratePerMinute := ratePerSecond * 60
				ratePerHour := ratePerSecond * 3600
				SetMessageRate(broker, ratePerSecond, ratePerMinute, ratePerHour)
			}
			lastTime = now
		}
	}
}
func KafkaConsumer() {
	broker := "kafka"
	IncConsumerCount(broker)
	defer DecConsumerCount(broker)

	kafkaCfg := config.Kafka

	consumerGroup := kafkaCfg.GroupID
	brokers := kafkaCfg.Brokers
	topic := kafkaCfg.Topic
	instanceID := config.App.InstanceID

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_1_0_0
	saramaCfg.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRange
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaCfg.ClientID = instanceID

	// Configure fetch settings for better performance
	saramaCfg.Consumer.Fetch.Default = 10240 // 10 KB per request
	saramaCfg.Consumer.Fetch.Min = 1024
	saramaCfg.Consumer.Fetch.Max = 10240
	saramaCfg.Consumer.MaxWaitTime = 100 * time.Millisecond

	ctx := context.Background()
	handler := &kafkaConsumerGroupHandler{
		broker:               broker,
		maxMessagesPerSecond: kafkaCfg.MaxMessagesPerSecond,
	}

	client, err := sarama.NewConsumerGroup(brokers, consumerGroup, saramaCfg)
	if err != nil {
		log.Printf("KafkaConsumer: failed to create consumer group: %v", err)
		IncrementError(broker, "consumer_creation_failed")
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("KafkaConsumer: error closing consumer group: %v", err)
		}
	}()

	log.Printf("KafkaConsumer: started consuming from topic %s", topic)

	// Standard consumption loop without ticker
	for {
		err := client.Consume(ctx, []string{topic}, handler)
		if err != nil {
			log.Printf("KafkaConsumer: error from consumer: %v", err)
			IncrementError(broker, "consume_error")
			time.Sleep(2 * time.Second) // Wait before retrying
		}

		// Check if context is done
		if ctx.Err() != nil {
			log.Printf("KafkaConsumer: context error: %v", ctx.Err())
			return
		}
	}
}

type kafkaConsumerGroupHandler struct {
	broker               string
	maxMessagesPerSecond int
}

func (h *kafkaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *kafkaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	var msgCount int64
	var lastTime = time.Now()
	var rateLimitCounter int
	var rateLimitStartTime = time.Now()

	for msg := range claim.Messages() {
		start := time.Now()

		// Rate limiting: check if we've exceeded the limit
		rateLimitCounter++
		elapsedSinceStart := time.Since(rateLimitStartTime)

		if rateLimitCounter >= h.maxMessagesPerSecond {
			if elapsedSinceStart < time.Second {
				// Wait for the remaining time to complete 1 second
				sleepTime := 10*time.Second - elapsedSinceStart
				log.Printf("Rate limit reached, sleeping for %v", sleepTime)
				time.Sleep(sleepTime)
			}
			// Reset counter and start time
			rateLimitCounter = 0
			rateLimitStartTime = time.Now()
		}

		// Process the message
		log.Printf("KafkaConsumer received message: topic=%s partition=%d offset=%d value=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Value))

		// Mark message as processed
		session.MarkMessage(msg, "")

		// Record metrics
		ObserveMessageConsumed(h.broker, time.Since(start))
		msgCount++

		// Update rate metrics every 10 messages
		if msgCount%10 == 0 {
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			if elapsed > 0 {
				ratePerSecond := float64(msgCount) / elapsed
				ratePerMinute := ratePerSecond * 60
				ratePerHour := ratePerSecond * 3600
				SetMessageRate(h.broker, ratePerSecond, ratePerMinute, ratePerHour)
			}
			lastTime = now
			msgCount = 0
		}
	}
	return nil
}

// RabbitMQConsumer reads messages from a RabbitMQ queue and processes them.
func RabbitMQConsumer() {
	broker := "rabbitmq"
	IncConsumerCount(broker)
	defer DecConsumerCount(broker)

	rabbitCfg := config.RabbitMQ

	// Connection management with reconnection
	var conn *amqp.Connection
	var ch *amqp.Channel
	var err error

	// Function to establish connection and channel
	connect := func() error {
		amqpURL := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
			rabbitCfg.User,
			rabbitCfg.Password,
			rabbitCfg.Host,
			rabbitCfg.Port,
			rabbitCfg.VHost,
		)
		log.Printf("RabbitMQConsumer: connecting to %s", amqpURL)

		conn, err = amqp.Dial(amqpURL)
		if err != nil {
			return fmt.Errorf("failed to connect to RabbitMQ: %w", err)
		}

		ch, err = conn.Channel()
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to open a channel: %w", err)
		}

		// Set up connection close handler
		conn.NotifyClose(make(chan *amqp.Error))

		// Declare queue
		_, err = ch.QueueDeclare(
			rabbitCfg.Queue,
			true,  // durable
			false, // auto-deleted
			false, // exclusive
			false, // no-wait
			nil,   // arguments
		)
		if err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("failed to declare queue: %w", err)
		}

		log.Printf("RabbitMQConsumer: successfully connected and declared queue %s", rabbitCfg.Queue)
		return nil
	}

	// Initial connection
	if err := connect(); err != nil {
		log.Printf("RabbitMQConsumer: %v", err)
		IncrementError(broker, "connection_failed")
		return
	}
	defer func() {
		if ch != nil {
			ch.Close()
		}
		if conn != nil {
			conn.Close()
		}
	}()

	// Channel for graceful shutdown
	done := make(chan bool)
	defer close(done)

	// Start a goroutine to periodically update queue metrics
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				// Check if connection and channel are still valid
				if conn == nil || conn.IsClosed() || ch == nil {
					log.Printf("RabbitMQConsumer: connection or channel is closed, skipping queue inspection")
					IncrementError(broker, "connection_lost")
					continue
				}

				// Create a new channel for queue inspection to avoid conflicts
				inspectCh, err := conn.Channel()
				if err != nil {
					log.Printf("RabbitMQConsumer: failed to create inspection channel: %v", err)
					IncrementError(broker, "inspection_channel_failed")
					continue
				}

				// Get queue info for depth monitoring
				q, err := inspectCh.QueueInspect(rabbitCfg.Queue)
				if err != nil {
					log.Printf("RabbitMQConsumer: failed to inspect queue: %v", err)
					IncrementError(broker, "queue_inspect_failed")
					inspectCh.Close()
					continue
				}
				SetQueueMetrics(broker, rabbitCfg.Queue, float64(q.Messages), 0)
				inspectCh.Close()
			case <-done:
				return
			}
		}
	}()

	// Main consumer loop with reconnection logic
	for {
		// Check if connection is still valid
		if conn == nil || conn.IsClosed() || ch == nil {
			log.Printf("RabbitMQConsumer: connection lost, attempting to reconnect...")
			if err := connect(); err != nil {
				log.Printf("RabbitMQConsumer: reconnection failed: %v", err)
				IncrementError(broker, "reconnection_failed")
				time.Sleep(5 * time.Second)
				continue
			}
		}

		// Set QoS for better message distribution
		err = ch.Qos(
			1,     // prefetch count
			0,     // prefetch size
			false, // global
		)
		if err != nil {
			log.Printf("RabbitMQConsumer: failed to set QoS: %v", err)
			IncrementError(broker, "qos_setup_failed")
			time.Sleep(2 * time.Second)
			continue
		}

		// Register consumer
		msgs, err := ch.Consume(
			rabbitCfg.Queue,
			"",    // consumer
			true,  // auto-ack (we'll ack manually)
			false, // exclusive
			false, // no-local
			false, // no-wait
			nil,   // args
		)
		if err != nil {
			log.Printf("RabbitMQConsumer: failed to register consumer: %v", err)
			IncrementError(broker, "consumer_registration_failed")
			time.Sleep(2 * time.Second)
			continue
		}

		log.Printf("RabbitMQConsumer: started consuming from queue %s", rabbitCfg.Queue)

		var msgCount int64
		var lastTime = time.Now()

		// Process messages
		for msg := range msgs {
			start := time.Now()
			log.Printf("RabbitMQConsumer received message: %s", string(msg.Body))

			// // Acknowledge the message
			// if err := msg.Ack(false); err != nil {
			// 	log.Printf("RabbitMQConsumer: failed to ack message: %v", err)
			// 	IncrementError(broker, "ack_failed")
			// 	// If ack fails, the channel might be closed, so break out of the loop
			// 	break
			// }

			ObserveMessageConsumed(broker, time.Since(start))
			msgCount++

			if msgCount%10 == 0 {
				now := time.Now()
				elapsed := now.Sub(lastTime).Seconds()
				if elapsed > 0 {
					ratePerSecond := float64(msgCount) / elapsed
					ratePerMinute := ratePerSecond * 60
					ratePerHour := ratePerSecond * 3600
					SetMessageRate(broker, ratePerSecond, ratePerMinute, ratePerHour)
				}
				lastTime = now
				msgCount = 0
			}
		}

		// If we reach here, the message channel was closed
		log.Printf("RabbitMQConsumer: message channel closed, will attempt to reconnect")
		time.Sleep(2 * time.Second)
	}
}

// NATSConsumer reads messages from a NATS JetStream subject and processes them.
func NATSConsumer() {
	broker := "nats"
	IncConsumerCount(broker)
	defer DecConsumerCount(broker)

	natsCfg := config.NATS

	nc, err := nats.Connect(natsCfg.URL)
	if err != nil {
		log.Printf("NATSConsumer: failed to connect to NATS: %v", err)
		return
	}
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		log.Printf("NATSConsumer: failed to get JetStream context: %v", err)
		return
	}

	sub, err := js.PullSubscribe(
		natsCfg.Subject,
		natsCfg.Consumer,
	)
	if err != nil {
		log.Printf("NATSConsumer: failed to subscribe to subject: %v", err)
		return
	}

	log.Printf("NATSConsumer: started consuming from subject %s", natsCfg.Subject)

	var msgCount int64
	var lastTime = time.Now()

	for {
		msgs, err := sub.Fetch(10, nats.MaxWait(5*time.Second))
		if err != nil && err != nats.ErrTimeout {
			log.Printf("NATSConsumer: error fetching messages: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		for _, msg := range msgs {
			start := time.Now()
			log.Printf("NATSConsumer received message: %s", string(msg.Data))
			if err := msg.Ack(); err != nil {
				log.Printf("NATSConsumer: failed to ack message: %v", err)
			}
			ObserveMessageConsumed(broker, time.Since(start))
			msgCount++
		}
		if msgCount > 0 {
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			if elapsed > 0 {
				ratePerSecond := float64(msgCount) / elapsed
				ratePerMinute := ratePerSecond * 60
				ratePerHour := ratePerSecond * 3600
				SetMessageRate(broker, ratePerSecond, ratePerMinute, ratePerHour)
			}
			lastTime = now
			msgCount = 0
		}
	}
}

// RedpandaConsumer reads messages from a Redpanda topic using the Sarama Kafka client and processes them.
func RedpandaConsumer() {
	broker := "redpanda"
	IncConsumerCount(broker)
	defer DecConsumerCount(broker)

	kafkaCfg := config.Redpanda

	consumerGroup := kafkaCfg.GroupID
	brokers := kafkaCfg.Brokers
	topic := kafkaCfg.Topic
	instanceID := config.App.InstanceID

	saramaCfg := sarama.NewConfig()
	saramaCfg.Version = sarama.V2_1_0_0
	saramaCfg.Consumer.Group.Rebalance.Strategy = sarama.NewBalanceStrategyRange()
	saramaCfg.Consumer.Offsets.Initial = sarama.OffsetNewest
	saramaCfg.ClientID = instanceID

	ctx := context.Background()

	handler := &redpandaConsumerGroupHandler{}

	client, err := sarama.NewConsumerGroup(brokers, consumerGroup, saramaCfg)
	if err != nil {
		log.Printf("RedpandaConsumer: failed to create consumer group: %v", err)
		return
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("RedpandaConsumer: error closing consumer group: %v", err)
		}
	}()

	log.Printf("RedpandaConsumer: started consuming from topic %s", topic)

	for {
		if err := client.Consume(ctx, []string{topic}, handler); err != nil {
			log.Printf("RedpandaConsumer: error from consumer: %v", err)
			time.Sleep(2 * time.Second)
		}
		if ctx.Err() != nil {
			log.Printf("RedpandaConsumer: context error: %v", ctx.Err())
			return
		}
	}
}

type redpandaConsumerGroupHandler struct{}

func (h *redpandaConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *redpandaConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *redpandaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	broker := "redpanda"
	var msgCount int64
	var lastTime = time.Now()
	for msg := range claim.Messages() {
		start := time.Now()
		log.Printf("RedpandaConsumer received message: topic=%s partition=%d offset=%d value=%s", msg.Topic, msg.Partition, msg.Offset, string(msg.Value))
		session.MarkMessage(msg, "")
		ObserveMessageConsumed(broker, time.Since(start))
		msgCount++
		if msgCount%10 == 0 {
			now := time.Now()
			elapsed := now.Sub(lastTime).Seconds()
			if elapsed > 0 {
				ratePerSecond := float64(msgCount) / elapsed
				ratePerMinute := ratePerSecond * 60
				ratePerHour := ratePerSecond * 3600
				SetMessageRate(broker, ratePerSecond, ratePerMinute, ratePerHour)
			}
			lastTime = now
			msgCount = 0
		}
	}
	return nil
}
