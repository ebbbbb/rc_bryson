package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"

	"reliable-notifier/internal/dispatch"
)

type RabbitConsumer struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	messages   <-chan amqp.Delivery
	deferrer   signalDeferrer
}

type signalDeferrer interface {
	Defer(context.Context, dispatch.Signal) error
	Close() error
}

func NewRabbitConsumer(rawURL string) (*RabbitConsumer, error) {
	deferrer, err := dispatch.NewRabbitBroker(rawURL)
	if err != nil {
		return nil, fmt.Errorf("create RabbitMQ deferral publisher: %w", err)
	}
	connection, err := amqp.Dial(rawURL)
	if err != nil {
		_ = deferrer.Close()
		return nil, fmt.Errorf("connect RabbitMQ Worker: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = deferrer.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("open RabbitMQ Worker channel: %w", err)
	}
	if err := channel.Qos(8, 0, false); err != nil {
		_ = deferrer.Close()
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
		_ = deferrer.Close()
		_ = channel.Close()
		_ = connection.Close()
		return nil, fmt.Errorf("consume dispatch queue: %w", err)
	}
	return &RabbitConsumer{
		connection: connection,
		channel:    channel,
		messages:   messages,
		deferrer:   deferrer,
	}, nil
}

func (consumer *RabbitConsumer) Run(ctx context.Context, worker *Worker) error {
	runContext, cancel := context.WithCancel(ctx)
	var group sync.WaitGroup
	defer func() {
		cancel()
		group.Wait()
	}()
	failures := make(chan error, 1)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-failures:
			return err
		case message, ok := <-consumer.messages:
			if !ok {
				return fmt.Errorf("RabbitMQ Worker delivery channel closed")
			}
			group.Add(1)
			go func() {
				defer group.Done()
				if err := consumer.processMessage(runContext, worker, message); err != nil {
					select {
					case failures <- err:
						cancel()
					default:
					}
				}
			}()
		}
	}
}

func (consumer *RabbitConsumer) processMessage(
	ctx context.Context,
	worker *Worker,
	message amqp.Delivery,
) error {
	var signal dispatch.Signal
	if err := json.Unmarshal(message.Body, &signal); err != nil {
		if nackErr := message.Nack(false, false); nackErr != nil {
			return fmt.Errorf("dead-letter malformed signal: %w", nackErr)
		}
		return nil
	}
	if err := signal.Validate(); err != nil {
		if nackErr := message.Nack(false, false); nackErr != nil {
			return fmt.Errorf("dead-letter invalid signal: %w", nackErr)
		}
		return nil
	}
	ack, err := worker.Process(ctx, signal)
	if err != nil {
		if nackErr := message.Nack(false, true); nackErr != nil {
			return fmt.Errorf("requeue failed delivery signal: %w", nackErr)
		}
		return fmt.Errorf("process delivery signal: %w", err)
	}
	if !ack {
		if err := consumer.deferrer.Defer(ctx, signal); err != nil {
			if nackErr := message.Nack(false, true); nackErr != nil {
				return fmt.Errorf("requeue signal after failed deferral: %w", nackErr)
			}
			return fmt.Errorf("defer capacity-limited signal: %w", err)
		}
		if err := message.Ack(false); err != nil {
			return fmt.Errorf("ack durably deferred delivery signal: %w", err)
		}
		return nil
	}
	if err := message.Ack(false); err != nil {
		return fmt.Errorf("ack committed delivery signal: %w", err)
	}
	return nil
}

func (consumer *RabbitConsumer) Close() error {
	var closeError error
	if consumer.deferrer != nil {
		closeError = consumer.deferrer.Close()
	}
	if consumer.channel != nil {
		if err := consumer.channel.Close(); err != nil && closeError == nil {
			closeError = err
		}
	}
	if consumer.connection != nil {
		if err := consumer.connection.Close(); err != nil && closeError == nil {
			closeError = err
		}
	}
	return closeError
}
