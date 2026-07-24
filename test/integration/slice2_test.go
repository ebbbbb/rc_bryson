//go:build integration

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"

	notifierdelivery "reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
)

const dispatchQueue = dispatch.QueueName

func TestSlice2PublishesIdentifierOnlySignal(t *testing.T) {
	connection, channel := connectDispatchTopology(t)
	defer connection.Close()
	defer channel.Close()

	if _, err := channel.QueuePurge(dispatchQueue, false); err != nil {
		t.Fatal(err)
	}

	accepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"must_not_enter_queue":"seeded-sensitive-body"}`),
	)
	if accepted.StatusCode != 202 {
		t.Fatalf("submit status = %d, body = %s", accepted.StatusCode, accepted.Body)
	}

	found := false
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		message, ok, err := channel.Get(dispatchQueue, false)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		if message.DeliveryMode != amqp.Persistent {
			t.Fatalf("delivery mode = %d, want persistent", message.DeliveryMode)
		}

		var signal map[string]any
		if err := json.Unmarshal(message.Body, &signal); err != nil {
			t.Fatalf("decode dispatch signal: %v", err)
		}
		if len(signal) != 3 {
			t.Fatalf("signal fields = %v, want identifier-only delivery_id, generation, trace_id", signal)
		}
		for _, field := range []string{"delivery_id", "generation", "trace_id"} {
			if _, ok := signal[field]; !ok {
				t.Fatalf("signal does not contain %q: %v", field, signal)
			}
		}
		if err := message.Ack(false); err != nil {
			t.Fatal(err)
		}
		if signal["delivery_id"] == accepted.Delivery.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("dispatch signal for %s was not published", accepted.Delivery.ID)
	}
}

func TestSlice2QueueOutageDoesNotStrandAcceptedDelivery(t *testing.T) {
	compose(t, "stop", "rabbitmq")
	t.Cleanup(func() {
		composeCleanup(t, "up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
	})

	accepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"accepted_while_queue":"offline"}`),
	)
	if accepted.StatusCode != 202 {
		t.Fatalf("submit during queue outage status = %d, body = %s", accepted.StatusCode, accepted.Body)
	}

	pool := integrationPool(t)
	var published bool
	if err := pool.QueryRow(t.Context(), `
		SELECT published_at IS NOT NULL
		FROM outbox_events
		WHERE delivery_id = $1 AND generation = 0
	`, accepted.Delivery.ID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("Outbox was marked published while RabbitMQ was stopped")
	}

	compose(t, "up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if err := pool.QueryRow(t.Context(), `
			SELECT published_at IS NOT NULL
			FROM outbox_events
			WHERE delivery_id = $1 AND generation = 0
		`, accepted.Delivery.ID).Scan(&published); err != nil {
			t.Fatal(err)
		}
		if published {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !published {
		t.Fatal("recovered publication was not recorded in PostgreSQL")
	}

	compose(t, "restart", "rabbitmq")
	compose(t, "up", "-d", "--wait", "--wait-timeout", "120", "rabbitmq")
	connection := connectRabbit(t)
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	waitForSignal(t, channel, accepted.Delivery.ID, 10*time.Second)
}

func TestSlice2ConfirmAmbiguityProducesDuplicateSignalNotDelivery(t *testing.T) {
	topologyConnection, topologyChannel := connectDispatchTopology(t)
	_ = topologyChannel.Close()
	_ = topologyConnection.Close()
	compose(t, "stop", "app")
	t.Cleanup(func() {
		composeCleanup(t, "up", "-d", "--wait", "--wait-timeout", "120", "app")
	})

	pool := integrationPool(t)
	event := insertUnpublishedEvent(t, pool)
	connection := connectRabbit(t)
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channel.QueuePurge(dispatch.QueueName, false); err != nil {
		t.Fatal(err)
	}
	if _, err := channel.QueuePurge(dispatch.DeadLetterQueue, false); err != nil {
		t.Fatal(err)
	}
	_ = channel.Close()
	_ = connection.Close()

	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}
	failingStore := &failFirstMarkStore{Store: store}
	firstBroker := connectDispatchBroker(t)
	firstPublisher, err := dispatch.NewPublisher(failingStore, firstBroker, "ambiguity-first", 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if published, err := firstPublisher.RunOnce(t.Context()); err == nil || published != 0 {
		t.Fatalf("ambiguous first cycle = (%d, %v), want 0 and injected mark failure", published, err)
	}
	_ = firstBroker.Close()

	secondBroker := connectDispatchBroker(t)
	secondPublisher, err := dispatch.NewPublisher(store, secondBroker, "ambiguity-second", 30*time.Second, 1)
	if err != nil {
		t.Fatal(err)
	}
	if published, err := secondPublisher.RunOnce(t.Context()); err != nil || published != 1 {
		t.Fatalf("retry cycle = (%d, %v), want 1 and nil", published, err)
	}
	_ = secondBroker.Close()

	connection = connectRabbit(t)
	defer connection.Close()
	channel, err = connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	signals := collectSignals(t, channel, event.DeliveryID, 2, 5*time.Second)
	for index, signal := range signals {
		if signal.Generation != event.Generation || signal.TraceID != event.TraceID {
			t.Fatalf("duplicate signal %d = %+v, want same generation and trace", index, signal)
		}
	}

	var deliveryRows int
	var published bool
	if err := pool.QueryRow(t.Context(), `
		SELECT
			(SELECT count(*) FROM deliveries WHERE id = $1),
			(SELECT published_at IS NOT NULL FROM outbox_events WHERE id = $2)
	`, event.DeliveryID, event.ID).Scan(&deliveryRows, &published); err != nil {
		t.Fatal(err)
	}
	if deliveryRows != 1 || !published {
		t.Fatalf("delivery rows = %d, published = %v, want one logical delivery and published Outbox", deliveryRows, published)
	}
}

func TestSlice2OutboxLeaseHasSingleOwnerAndFencesPublication(t *testing.T) {
	compose(t, "stop", "app")
	t.Cleanup(func() {
		composeCleanup(t, "up", "-d", "--wait", "--wait-timeout", "120", "app")
	})

	pool := integrationPool(t)
	inserted := insertUnpublishedEvent(t, pool)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}

	type claimResult struct {
		events []notifierdelivery.OutboxEvent
		err    error
	}
	results := make(chan claimResult, 2)
	for _, owner := range []string{"claimer-a", "claimer-b"} {
		go func() {
			events, err := store.ClaimOutbox(t.Context(), owner, 30*time.Second, 1)
			results <- claimResult{events: events, err: err}
		}()
	}

	var claimed []notifierdelivery.OutboxEvent
	for range 2 {
		result := <-results
		if result.err != nil {
			t.Fatal(result.err)
		}
		claimed = append(claimed, result.events...)
	}
	if len(claimed) != 1 || claimed[0].ID != inserted.ID {
		t.Fatalf("concurrent claims = %+v, want exactly the inserted event once", claimed)
	}

	stale := claimed[0]
	stale.LeaseToken = "00000000-0000-4000-8000-000000000099"
	if err := store.MarkOutboxPublished(t.Context(), stale); !errors.Is(err, notifierdelivery.ErrOutboxLeaseLost) {
		t.Fatalf("stale publication result = %v, want ErrOutboxLeaseLost", err)
	}
	var published bool
	if err := pool.QueryRow(t.Context(), `
		SELECT published_at IS NOT NULL FROM outbox_events WHERE id = $1
	`, inserted.ID).Scan(&published); err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("stale lease marked Outbox event published")
	}
	if err := store.ReleaseOutbox(t.Context(), claimed[0]); err != nil {
		t.Fatal(err)
	}
}

func TestSlice2DeadLetterQueueRemainsIdentifierOnly(t *testing.T) {
	topologyConnection, topologyChannel := connectDispatchTopology(t)
	_ = topologyChannel.Close()
	_ = topologyConnection.Close()
	compose(t, "stop", "app")
	t.Cleanup(func() {
		composeCleanup(t, "up", "-d", "--wait", "--wait-timeout", "120", "app")
	})

	broker := connectDispatchBroker(t)
	connection := connectRabbit(t)
	channel, err := connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := channel.QueuePurge(dispatch.QueueName, false); err != nil {
		t.Fatal(err)
	}
	if _, err := channel.QueuePurge(dispatch.DeadLetterQueue, false); err != nil {
		t.Fatal(err)
	}
	_ = channel.Close()
	_ = connection.Close()

	signal := dispatch.Signal{
		DeliveryID: "00000000-0000-4000-8000-000000000001",
		Generation: 7,
		TraceID:    "00000000-0000-4000-8000-000000000002",
	}
	if err := broker.Publish(t.Context(), signal); err != nil {
		t.Fatal(err)
	}
	_ = broker.Close()

	connection = connectRabbit(t)
	defer connection.Close()
	channel, err = connection.Channel()
	if err != nil {
		t.Fatal(err)
	}
	defer channel.Close()
	message := getMessage(t, channel, dispatch.QueueName, 5*time.Second)
	assertIdentifierOnly(t, message, signal.DeliveryID)
	if err := message.Nack(false, false); err != nil {
		t.Fatal(err)
	}

	deadLetter := getMessage(t, channel, dispatch.DeadLetterQueue, 5*time.Second)
	assertIdentifierOnly(t, deadLetter, signal.DeliveryID)
	if err := deadLetter.Ack(false); err != nil {
		t.Fatal(err)
	}
}

type failFirstMarkStore struct {
	*notifierdelivery.Store
	failed bool
}

func (store *failFirstMarkStore) MarkOutboxPublished(
	ctx context.Context,
	event notifierdelivery.OutboxEvent,
) error {
	if !store.failed {
		store.failed = true
		return errors.New("injected failure after broker confirm")
	}
	return store.Store.MarkOutboxPublished(ctx, event)
}

func insertUnpublishedEvent(t *testing.T, pool *pgxpool.Pool) notifierdelivery.OutboxEvent {
	t.Helper()
	if _, err := pool.Exec(t.Context(), `
		UPDATE outbox_events
		SET available_at = clock_timestamp() + interval '1 hour'
		WHERE published_at IS NULL
	`); err != nil {
		t.Fatal(err)
	}
	var event notifierdelivery.OutboxEvent
	err := pool.QueryRow(t.Context(), `
		WITH inserted_delivery AS (
			INSERT INTO deliveries (
				id,
				caller_id,
				idempotency_key,
				request_hash,
				destination_id,
				destination_version,
				method,
				caller_headers,
				body,
				supplier_idempotency_value,
				status,
				generation,
				accepted_at,
				retry_deadline,
				next_attempt_at
			)
			VALUES (
				gen_random_uuid(),
				'caller-a',
				$1,
				decode(repeat('00', 32), 'hex'),
				'supplier-a',
				1,
				'POST',
				'{}'::jsonb,
				''::bytea,
				gen_random_uuid()::text,
				'pending',
				0,
				clock_timestamp(),
				clock_timestamp() + interval '24 hours',
				clock_timestamp()
			)
			RETURNING id
		),
		inserted_event AS (
			INSERT INTO outbox_events (
				id,
				delivery_id,
				generation,
				trace_id,
				available_at
			)
			SELECT
				gen_random_uuid(),
				id,
				0,
				gen_random_uuid(),
				clock_timestamp()
			FROM inserted_delivery
			RETURNING id, delivery_id, generation, trace_id
		)
		SELECT id::text, delivery_id::text, generation, trace_id::text
		FROM inserted_event
	`, uniqueKey(t)).Scan(&event.ID, &event.DeliveryID, &event.Generation, &event.TraceID)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func collectSignals(
	t *testing.T,
	channel *amqp.Channel,
	deliveryID string,
	count int,
	timeout time.Duration,
) []dispatch.Signal {
	t.Helper()
	signals := make([]dispatch.Signal, 0, count)
	deadline := time.Now().Add(timeout)
	for len(signals) < count && time.Now().Before(deadline) {
		message, ok, err := channel.Get(dispatch.QueueName, false)
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		signal := decodeSignal(t, message)
		if err := message.Ack(false); err != nil {
			t.Fatal(err)
		}
		if signal.DeliveryID == deliveryID {
			signals = append(signals, signal)
		}
	}
	if len(signals) != count {
		t.Fatalf("signals for %s = %d, want %d", deliveryID, len(signals), count)
	}
	return signals
}

func waitForSignal(t *testing.T, channel *amqp.Channel, deliveryID string, timeout time.Duration) {
	t.Helper()
	_ = collectSignals(t, channel, deliveryID, 1, timeout)
}

func getMessage(t *testing.T, channel *amqp.Channel, queue string, timeout time.Duration) amqp.Delivery {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		message, ok, err := channel.Get(queue, false)
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return message
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("message not received from %s", queue)
	return amqp.Delivery{}
}

func assertIdentifierOnly(t *testing.T, message amqp.Delivery, deliveryID string) {
	t.Helper()
	if message.DeliveryMode != amqp.Persistent {
		t.Fatalf("delivery mode = %d, want persistent", message.DeliveryMode)
	}
	signal := decodeSignal(t, message)
	if signal.DeliveryID != deliveryID {
		t.Fatalf("delivery_id = %s, want %s", signal.DeliveryID, deliveryID)
	}
	var fields map[string]any
	if err := json.Unmarshal(message.Body, &fields); err != nil {
		t.Fatal(err)
	}
	if len(fields) != 3 {
		t.Fatalf("message fields = %v, want identifier-only signal", fields)
	}
}

func decodeSignal(t *testing.T, message amqp.Delivery) dispatch.Signal {
	t.Helper()
	var signal dispatch.Signal
	if err := json.Unmarshal(message.Body, &signal); err != nil {
		t.Fatalf("decode signal: %v", err)
	}
	return signal
}

func connectRabbit(t *testing.T) *amqp.Connection {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := amqp.Dial(rabbitURL(t))
		if err == nil {
			return connection
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("connect RabbitMQ: %v", lastErr)
	return nil
}

func connectDispatchTopology(t *testing.T) (*amqp.Connection, *amqp.Channel) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		connection, err := amqp.Dial(rabbitURL(t))
		if err != nil {
			lastErr = err
			time.Sleep(100 * time.Millisecond)
			continue
		}
		channel, err := connection.Channel()
		if err == nil {
			_, err = channel.QueueDeclarePassive(
				dispatch.QueueName,
				true,
				false,
				false,
				false,
				nil,
			)
		}
		if err == nil {
			return connection, channel
		}
		lastErr = err
		if channel != nil {
			_ = channel.Close()
		}
		_ = connection.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("wait for RabbitMQ dispatch topology: %v", lastErr)
	return nil, nil
}

func connectDispatchBroker(t *testing.T) *dispatch.RabbitBroker {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		broker, err := dispatch.NewRabbitBroker(rabbitURL(t))
		if err == nil {
			return broker
		}
		lastErr = err
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("connect RabbitMQ dispatch broker: %v", lastErr)
	return nil
}

func compose(t *testing.T, args ...string) {
	t.Helper()
	command := exec.Command(
		"docker",
		append([]string{
			"compose",
			"--project-name", requiredEnv(t, "COMPOSE_PROJECT_NAME"),
			"--project-directory", requiredEnv(t, "REPO_ROOT"),
		}, args...)...,
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose %v: %v\n%s", args, err, output)
	}
	if containsArgument(args, "app") && containsArgument(args, "up") {
		refreshAppURL(t)
	}
}

func composeCleanup(t *testing.T, args ...string) {
	t.Helper()
	command := exec.Command(
		"docker",
		append([]string{
			"compose",
			"--project-name", os.Getenv("COMPOSE_PROJECT_NAME"),
			"--project-directory", os.Getenv("REPO_ROOT"),
		}, args...)...,
	)
	if output, err := command.CombinedOutput(); err != nil {
		t.Errorf("cleanup docker compose %v: %v\n%s", args, err, output)
	} else if containsArgument(args, "app") && containsArgument(args, "up") {
		refreshAppURL(t)
	}
}

func refreshAppURL(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		command := exec.Command(
			"docker",
			"compose",
			"--project-name", requiredEnv(t, "COMPOSE_PROJECT_NAME"),
			"--project-directory", requiredEnv(t, "REPO_ROOT"),
			"port",
			"app",
			"8080",
		)
		output, err := command.CombinedOutput()
		if err == nil {
			_, port, splitErr := net.SplitHostPort(strings.TrimSpace(string(output)))
			if splitErr == nil && port != "" && port != "0" {
				if err := os.Setenv(
					"APP_HEALTH_URL",
					"http://127.0.0.1:"+port+"/healthz",
				); err != nil {
					t.Fatalf("update app URL: %v", err)
				}
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("resolve non-zero app port after Compose restart")
}

func containsArgument(args []string, expected string) bool {
	for _, argument := range args {
		if argument == expected {
			return true
		}
	}
	return false
}

func requiredEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required", name)
	}
	return value
}

func rabbitURL(t *testing.T) string {
	t.Helper()
	command := exec.Command(
		"docker",
		"compose",
		"--project-name", requiredEnv(t, "COMPOSE_PROJECT_NAME"),
		"--project-directory", requiredEnv(t, "REPO_ROOT"),
		"port",
		"rabbitmq",
		"5672",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("resolve RabbitMQ port: %v\n%s", err, output)
	}
	_, port, err := net.SplitHostPort(strings.TrimSpace(string(output)))
	if err != nil {
		t.Fatalf("parse RabbitMQ port %q: %v", output, err)
	}
	return "amqp://notifier:notifier-test-only@127.0.0.1:" + port + "/"
}
