# RabbitMQ vs Kafka Consumer Performance Comparison

This README demonstrates the differences in message consumption patterns using RabbitMQ and Kafka under various configurations. For each scenario, a performance chart or picture is provided to visualize the results.

## RabbitMQ Consumption Scenarios

We compare two main RabbitMQ consumer modes:
- **Auto Acknowledge (auto ack)**
- **Manual Acknowledge (manual ack)**

For each mode, we vary the `prefetch` count (the maximum number of unacknowledged messages a consumer can receive):

### 1. Prefetch = 10

- **Auto Ack**
  - ![RabbitMQ Auto Ack Prefetch 10](images/rabbitmq_autoack_prefetch10.png)
- **Manual Ack**
  - ![RabbitMQ Manual Ack Prefetch 10](images/rabbitmq_manualack_prefetch10.png)

### 2. Prefetch = 30

- **Auto Ack**
  - ![RabbitMQ Auto Ack Prefetch 30](images/rabbitmq_autoack_prefetch30.png)
- **Manual Ack**
  - ![RabbitMQ Manual Ack Prefetch 30](images/rabbitmq_manualack_prefetch30.png)

---

## Kafka Consumption Scenarios

Kafka is tested with:
- **Default Consumer Settings**
  - ![Kafka Default Consumer](images/kafka_default_consumer.png)
- **Controlled Concurrency**
  - ![Kafka Controlled Concurrency](images/kafka_controlled_concurrency.png)
  - ![Kafka Controlled Concurrency](images/kafka_controlled_concurrency2.png)

---

## Summary Table

| Scenario                                   | RabbitMQ Auto Ack | RabbitMQ Manual Ack | Kafka Default | Kafka Controlled Concurrency |
|---------------------------------------------|:-----------------:|:------------------:|:-------------:|:---------------------------:|
| Prefetch = 1                               | ![](images/rabbitmq_autoack_prefetch1.png) | ![](images/rabbitmq_manualack_prefetch1.png) | ![](images/kafka_default_consumer.png) | ![](images/kafka_controlled_concurrency.png) |
| Prefetch = 10                              | ![](images/rabbitmq_autoack_prefetch10.png) | ![](images/rabbitmq_manualack_prefetch10.png) |   |   |
| Prefetch = 30                              | ![](images/rabbitmq_autoack_prefetch30.png) | ![](images/rabbitmq_manualack_prefetch30.png) 
---
