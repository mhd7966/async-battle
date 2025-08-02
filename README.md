# Async Messaging Comparison

This project provides a comparative overview of several popular async messaging systems, focusing on their consumption modes (pull/push) and concurrency control mechanisms. The systems covered are:

- RabbitMQ
- Apache Kafka
- JetStream (NATS)
- Redis Streams
- Redpanda

## Table of Contents

- [Overview](#overview)
- [Comparison Table](#comparison-table)
- [Detailed Analysis](#detailed-analysis)
  - [RabbitMQ](#rabbitmq)
  - [Apache Kafka](#apache-kafka)
  - [JetStream (NATS)](#jetstream-nats)
  - [Redis Streams](#redis-streams)
  - [Redpanda](#redpanda)
- [Usage](#usage)
- [License](#license)

## Overview

Asynchronous messaging systems are crucial for decoupling components, improving scalability, and enabling reliable communication in distributed systems. Each system has distinct consumption models and concurrency controls that suit different use cases.

## Comparison Table

| System         | Consumption Mode        | Concurrency Control                   | Key Notes                             |
|----------------|------------------------|---------------------------------------|---------------------------------------|
| RabbitMQ       | Push (default), Pull   | Consumer prefetch, manual ack, QoS    | Flexible, strong routing              |
| Apache Kafka   | Pull                   | Consumer groups, partition assignment | High throughput, order per partition  |
| JetStream NATS | Pull, Push             | Max Ack Pending, consumer config      | Lightweight, event streaming          |
| Redis Streams  | Pull (XREAD), Push via Pub/Sub | Consumer groups, pending entries | Simple, scalable, at-least-once       |
| Redpanda       | Pull                   | Same as Kafka (Kafka API compatible)  | Kafka-compatible, low latency         |

## Detailed Analysis

### RabbitMQ

- **Consumption Mode:**  
  - **Push:** By default, RabbitMQ pushes messages to consumers.
  - **Pull:** AMQP supports basic.get for pull, but it's less common.
- **Concurrency Control:**  
  - Consumers can set prefetch limits (QoS) to control message flow.
  - Manual acknowledgments ensure at-least-once delivery.
- **Notes:**  
  - Suitable for complex routing and flexible topologies.

### Apache Kafka

- **Consumption Mode:**  
  - **Pull:** Consumers poll for messages at their own pace.
- **Concurrency Control:**  
  - Consumer groups enable multiple consumers to process partitions in parallel.
  - Each partition is consumed by only one consumer in a group at a time.
- **Notes:**  
  - High throughput, horizontal scalability, strong ordering per partition.

### JetStream (NATS)

- **Consumption Mode:**  
  - **Push:** JetStream can push messages to subscribers.
  - **Pull:** Also supports pull-based consumption for batch or flow control.
- **Concurrency Control:**  
  - Max Ack Pending setting controls in-flight messages.
  - Consumers can be configured for concurrency and delivery policies.
- **Notes:**  
  - Lightweight, cloud-native, supports both streaming and queue semantics.

### Redis Streams

- **Consumption Mode:**  
  - **Pull:** Clients use XREAD/XREADGROUP to fetch messages.
  - **Push:** Can use Pub/Sub for push, but with fewer guarantees.
- **Concurrency Control:**  
  - Consumer groups with pending entries management.
  - Each message in a group is delivered to one consumer.
- **Notes:**  
  - Simple and scalable, at-least-once delivery, best for lightweight use cases.

### Redpanda

- **Consumption Mode:**  
  - **Pull:** Fully Kafka API compatible; consumers poll for messages.
- **Concurrency Control:**  
  - Same as Kafka, using consumer groups and partition assignment.
- **Notes:**  
  - Kafka drop-in replacement, performance and operational improvements.

## Usage

This repository aims to provide code samples and benchmarks (if applicable) for each system, focusing on:

- Setting up consumers with different modes (pull, push)
- Configuring concurrency and scaling consumers
- Measuring throughput and latency (optional in future)

Please refer to the `/examples` directory (to be added) for sample implementations.

## License

This project is licensed under the MIT License.