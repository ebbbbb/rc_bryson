package dispatch

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	ExchangeName       = "reliable-notifier.dispatch"
	QueueName          = "reliable-notifier.dispatch"
	RoutingKey         = "delivery"
	DeadLetterExchange = "reliable-notifier.dispatch.dlx"
	DeadLetterQueue    = "reliable-notifier.dispatch.dlq"
	DeadLetterKey      = "dead"
)

type Signal struct {
	DeliveryID string `json:"delivery_id"`
	Generation int64  `json:"generation"`
	TraceID    string `json:"trace_id"`
}

type RabbitBroker struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	confirms   <-chan amqp.Confirmation
	returns    <-chan amqp.Return
}

func NewRabbitBroker(rawURL string) (*RabbitBroker, error) {
	connection, err := amqp.DialConfig(rawURL, amqp.Config{
		Dial: amqp.DefaultDial(5 * time.Second),
	})
	if err != nil {
		return nil, fmt.Errorf("connect RabbitMQ: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	broker := &RabbitBroker{
		connection: connection,
		channel:    channel,
	}
	if err := broker.declareTopology(); err != nil {
		_ = broker.Close()
		return nil, err
	}
	if err := channel.Confirm(false); err != nil {
		_ = broker.Close()
		return nil, fmt.Errorf("enable RabbitMQ publisher confirms: %w", err)
	}
	broker.confirms = channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	broker.returns = channel.NotifyReturn(make(chan amqp.Return, 1))
	return broker, nil
}

func (broker *RabbitBroker) declareTopology() error {
	if err := broker.channel.ExchangeDeclare(
		DeadLetterExchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dead-letter exchange: %w", err)
	}
	if _, err := broker.channel.QueueDeclare(
		DeadLetterQueue,
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dead-letter queue: %w", err)
	}
	if err := broker.channel.QueueBind(
		DeadLetterQueue,
		DeadLetterKey,
		DeadLetterExchange,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind dead-letter queue: %w", err)
	}
	if err := broker.channel.ExchangeDeclare(
		ExchangeName,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("declare dispatch exchange: %w", err)
	}
	if _, err := broker.channel.QueueDeclare(
		QueueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-dead-letter-exchange":    DeadLetterExchange,
			"x-dead-letter-routing-key": DeadLetterKey,
		},
	); err != nil {
		return fmt.Errorf("declare dispatch queue: %w", err)
	}
	if err := broker.channel.QueueBind(
		QueueName,
		RoutingKey,
		ExchangeName,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("bind dispatch queue: %w", err)
	}
	return nil
}

func (broker *RabbitBroker) Publish(ctx context.Context, signal Signal) error {
	body, err := json.Marshal(signal)
	if err != nil {
		return fmt.Errorf("encode dispatch signal: %w", err)
	}
	err = broker.channel.PublishWithContext(
		ctx,
		ExchangeName,
		RoutingKey,
		true,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Type:         "delivery.dispatch",
			Timestamp:    time.Now().UTC(),
			Body:         body,
		},
	)
	if err != nil {
		return fmt.Errorf("publish dispatch signal: %w", err)
	}
	select {
	case returned := <-broker.returns:
		return fmt.Errorf("dispatch signal was unroutable: %s", returned.ReplyText)
	case confirmation, ok := <-broker.confirms:
		if !ok {
			return errors.New("RabbitMQ confirm channel closed")
		}
		if !confirmation.Ack {
			return errors.New("RabbitMQ negatively acknowledged dispatch signal")
		}
		select {
		case returned := <-broker.returns:
			return fmt.Errorf("dispatch signal was unroutable: %s", returned.ReplyText)
		default:
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (broker *RabbitBroker) Close() error {
	var closeError error
	if broker.channel != nil {
		closeError = broker.channel.Close()
	}
	if broker.connection != nil {
		if err := broker.connection.Close(); err != nil && closeError == nil {
			closeError = err
		}
	}
	return closeError
}
