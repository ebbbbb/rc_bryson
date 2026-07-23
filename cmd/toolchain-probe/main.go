// Command toolchain-probe verifies approved infrastructure capabilities. It is
// test tooling and is not linked into the notifier service.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const probeTimeout = 15 * time.Second

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "toolchain probe:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("expected rabbit-semantics, rabbit-publish, rabbit-consume, or http-tls")
	}

	switch args[0] {
	case "rabbit-semantics":
		return probeRabbitSemantics(envOr("RABBITMQ_URL", "amqp://notifier:notifier-test-only@localhost:15672/"))
	case "rabbit-publish":
		if len(args) != 3 {
			return errors.New("rabbit-publish requires queue and body")
		}
		return publishDurable(envOr("RABBITMQ_URL", "amqp://notifier:notifier-test-only@localhost:15672/"), args[1], []byte(args[2]))
	case "rabbit-consume":
		if len(args) != 3 {
			return errors.New("rabbit-consume requires queue and expected body")
		}
		return consumeDurable(envOr("RABBITMQ_URL", "amqp://notifier:notifier-test-only@localhost:15672/"), args[1], []byte(args[2]))
	case "http-tls":
		return probeHTTPS(
			envOr("FAKE_SUPPLIER_URL", "https://fake-supplier.test:18443/healthz"),
			envOr("FAKE_SUPPLIER_VALIDATED_IP", "127.0.0.1"),
			envOr("FAKE_SUPPLIER_CA_FILE", "testdata/certs/test-ca.crt"),
		)
	default:
		return fmt.Errorf("unknown probe %q", args[0])
	}
}

func probeRabbitSemantics(url string) error {
	const queue = "notifier.toolchain.ack-probe"
	const body = "confirm-and-redeliver"

	connection, channel, err := openRabbit(url)
	if err != nil {
		return err
	}
	if err := declareAndPurge(channel, queue); err != nil {
		closeRabbit(connection, channel)
		return err
	}
	if err := publishConfirmed(channel, queue, []byte(body)); err != nil {
		closeRabbit(connection, channel)
		return err
	}

	deliveries, err := channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		closeRabbit(connection, channel)
		return fmt.Errorf("consume unacked message: %w", err)
	}
	first, err := awaitDelivery(deliveries)
	if err != nil {
		closeRabbit(connection, channel)
		return err
	}
	if string(first.Body) != body {
		closeRabbit(connection, channel)
		return fmt.Errorf("first body = %q", first.Body)
	}
	closeRabbit(connection, channel)

	connection, channel, err = openRabbit(url)
	if err != nil {
		return err
	}
	defer closeRabbit(connection, channel)
	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("redeclare queue: %w", err)
	}
	deliveries, err = channel.Consume(queue, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("consume redelivery: %w", err)
	}
	second, err := awaitDelivery(deliveries)
	if err != nil {
		return err
	}
	if !second.Redelivered {
		return errors.New("unacked message was not marked redelivered")
	}
	if string(second.Body) != body {
		return fmt.Errorf("redelivered body = %q", second.Body)
	}
	if err := second.Ack(false); err != nil {
		return fmt.Errorf("manual ack: %w", err)
	}
	if _, err := channel.QueueDelete(queue, false, false, false); err != nil {
		return fmt.Errorf("delete probe queue: %w", err)
	}
	return nil
}

func publishDurable(url, queue string, body []byte) error {
	connection, channel, err := openRabbit(url)
	if err != nil {
		return err
	}
	defer closeRabbit(connection, channel)
	if err := declareAndPurge(channel, queue); err != nil {
		return err
	}
	return publishConfirmed(channel, queue, body)
}

func consumeDurable(url, queue string, expected []byte) error {
	connection, channel, err := openRabbit(url)
	if err != nil {
		return err
	}
	defer closeRabbit(connection, channel)
	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare durable queue: %w", err)
	}
	message, ok, err := channel.Get(queue, false)
	if err != nil {
		return fmt.Errorf("get durable message: %w", err)
	}
	if !ok {
		return errors.New("durable message missing after restart")
	}
	if string(message.Body) != string(expected) {
		return fmt.Errorf("durable body = %q, want %q", message.Body, expected)
	}
	if err := message.Ack(false); err != nil {
		return fmt.Errorf("ack durable message: %w", err)
	}
	if _, err := channel.QueueDelete(queue, false, false, false); err != nil {
		return fmt.Errorf("delete durable queue: %w", err)
	}
	return nil
}

func openRabbit(url string) (*amqp.Connection, *amqp.Channel, error) {
	connection, err := amqp.DialConfig(url, amqp.Config{
		Dial: amqp.DefaultDial(probeTimeout),
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dial RabbitMQ: %w", err)
	}
	channel, err := connection.Channel()
	if err != nil {
		_ = connection.Close()
		return nil, nil, fmt.Errorf("open RabbitMQ channel: %w", err)
	}
	return connection, channel, nil
}

func closeRabbit(connection *amqp.Connection, channel *amqp.Channel) {
	if channel != nil {
		_ = channel.Close()
	}
	if connection != nil {
		_ = connection.Close()
	}
}

func declareAndPurge(channel *amqp.Channel, queue string) error {
	if _, err := channel.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return fmt.Errorf("declare durable queue: %w", err)
	}
	if _, err := channel.QueuePurge(queue, false); err != nil {
		return fmt.Errorf("purge probe queue: %w", err)
	}
	return nil
}

func publishConfirmed(channel *amqp.Channel, queue string, body []byte) error {
	if err := channel.Confirm(false); err != nil {
		return fmt.Errorf("enable publisher confirms: %w", err)
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	err := channel.PublishWithContext(ctx, "", queue, false, false, amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		ContentType:  "text/plain",
		Body:         body,
	})
	if err != nil {
		return fmt.Errorf("publish persistent message: %w", err)
	}
	select {
	case confirmation := <-confirms:
		if !confirmation.Ack {
			return errors.New("broker negatively acknowledged publish")
		}
		return nil
	case <-ctx.Done():
		return errors.New("publisher confirm timed out")
	}
}

func awaitDelivery(deliveries <-chan amqp.Delivery) (amqp.Delivery, error) {
	timer := time.NewTimer(probeTimeout)
	defer timer.Stop()
	select {
	case delivery, ok := <-deliveries:
		if !ok {
			return amqp.Delivery{}, errors.New("delivery channel closed")
		}
		return delivery, nil
	case <-timer.C:
		return amqp.Delivery{}, errors.New("message delivery timed out")
	}
}

func probeHTTPS(rawURL, validatedIP, caFile string) error {
	var dialer net.Dialer
	return probeHTTPSWithDialer(rawURL, validatedIP, caFile, dialer.DialContext)
}

func probeHTTPSWithDialer(
	rawURL string,
	validatedIP string,
	caFile string,
	dial func(context.Context, string, string) (net.Conn, error),
) error {
	const registeredHostname = "fake-supplier.test"
	target, err := url.ParseRequestURI(rawURL)
	if err != nil || target.Scheme != "https" || target.Hostname() != registeredHostname || target.User != nil {
		return errors.New("test-only URL must name fake-supplier.test explicitly")
	}
	if net.ParseIP(validatedIP) == nil {
		return errors.New("validated test IP is invalid")
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return fmt.Errorf("read test CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return errors.New("parse test CA")
	}

	dialed := false
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    roots,
			ServerName: registeredHostname,
		},
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, err
			}
			if host != registeredHostname {
				return nil, fmt.Errorf("refusing unregistered host %q", host)
			}
			dialed = true
			return dial(ctx, network, net.JoinHostPort(validatedIP, port))
		},
	}
	defer transport.CloseIdleConnections()

	client := &http.Client{
		Transport: transport,
		Timeout:   probeTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	response, err := client.Get(rawURL)
	if err != nil {
		return fmt.Errorf("GET through validated-IP dialer: %w", err)
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, response.Body)
	if !dialed {
		return errors.New("validated-IP dialer was not used")
	}
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("HTTPS status = %d", response.StatusCode)
	}
	return nil
}

func envOr(name, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}
