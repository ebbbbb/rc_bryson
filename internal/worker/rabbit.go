package worker

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"reliable-notifier/internal/dispatch"
)

type RabbitConsumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	messages   <-chan amqp.Delivery
}

func NewRabbitConsumer(rawURL string) (*RabbitConsumer, error) {
	connection, err := amqp.Dial(rawURL)
	if err != nil {
		return nil, fmt.Errorf("connect RabbitMQ Worker: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ Worker channel: %w", err)
	}
	if err := channel.Qos(1, 0, false); err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("set Worker prefetch: %w", err)
	}
	messages, err := channel.Consume(
		dispatch.QueueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		_ = channel.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("consume dispatch queue: %w", err)
	}
	return &RabbitConsumer{connection: connection, channel: channel, messages: messages}, nil
}

func (consumer *RabbitConsumer) Run(ctx context.Context, worker *Worker) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case message, ok := <-consumer.messages:
			if !ok {
				return fmt.Errorf("RabbitMQ Worker delivery channel closed")
			}
			var signal dispatch.Signal
			if err := json.Unmarshal(message.Body, &signal); err != nil {
				if nackErr := message.Nack(false, false); nackErr != nil {
					return fmt.Errorf("dead-letter malformed signal: %w", nackErr)
				}
				continue
			}
			ack, err := worker.Process(ctx, signal)
			if err != nil || !ack {
				if nackErr := message.Nack(false, true); nackErr != nil {
					return fmt.Errorf("requeue failed delivery signal: %w", nackErr)
				}
				continue
			}
			if err := message.Ack(false); err != nil {
				return fmt.Errorf("ack committed delivery signal: %w", err)
			}
		}
	}
}

func (consumer *RabbitConsumer) Close() error {
	var closeError error
	if consumer.channel != nil {
		closeError = consumer.channel.Close()
	}
	if consumer.connection != nil {
		if err := consumer.connection.Close(); err != nil && closeError == nil {
			closeError = err
		}
	}
	return closeError
}
