package dispatch

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func QueueDepth(ctx context.Context, rawURL string) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	connection, err := amqp.DialConfig(rawURL, amqp.Config{
		Dial: amqp.DefaultDial(2 * time.Second),
	})
	if err != nil {
		return 0, fmt.Errorf("connect RabbitMQ metrics probe: %w", err)
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return 0, fmt.Errorf("open RabbitMQ metrics channel: %w", err)
	}
	defer channel.Close()
	queue, err := channel.QueueInspect(QueueName)
	if err != nil {
		return 0, fmt.Errorf("inspect RabbitMQ dispatch queue: %w", err)
	}
	return queue.Messages, nil
}
