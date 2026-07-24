//go:build integration

package integration

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	notifierdelivery "reliable-notifier/internal/delivery"
	"reliable-notifier/internal/dispatch"
)

func TestSlice3WorkerResultIsFullyFenced(t *testing.T) {
	tests := []struct {
		name       string
		mutate     func(*notifierdelivery.AttemptResult)
		expire     bool
		clearLease bool
	}{
		{
			name: "delivery ID",
			mutate: func(result *notifierdelivery.AttemptResult) {
				result.DeliveryID = "00000000-0000-4000-8000-000000000099"
			},
		},
		{
			name: "generation",
			mutate: func(result *notifierdelivery.AttemptResult) {
				result.Generation++
			},
		},
		{
			name: "lease owner",
			mutate: func(result *notifierdelivery.AttemptResult) {
				result.LeaseOwner = "stale-worker"
			},
		},
		{
			name: "lease token",
			mutate: func(result *notifierdelivery.AttemptResult) {
				result.LeaseToken = "00000000-0000-4000-8000-000000000099"
			},
		},
		{
			name:   "expired lease",
			mutate: func(*notifierdelivery.AttemptResult) {},
			expire: true,
		},
		{
			name:       "delivering state",
			mutate:     func(*notifierdelivery.AttemptResult) {},
			clearLease: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accepted := submitDelivery(
				t,
				testCallerKey,
				uniqueKey(t),
				testDestination,
				[]byte(`{"fencing":true}`),
			)
			if accepted.StatusCode != 202 {
				t.Fatalf("submit status = %d, body = %s", accepted.StatusCode, accepted.Body)
			}

			pool := integrationPool(t)
			store, err := notifierdelivery.NewStore(pool, 100000)
			if err != nil {
				t.Fatal(err)
			}
			task, _, err := store.ClaimDelivery(
				t.Context(),
				accepted.Delivery.ID,
				accepted.Delivery.Generation,
				"fencing-worker",
				30*time.Second,
			)
			if err != nil {
				t.Fatal(err)
			}
			if test.expire {
				if _, err := pool.Exec(t.Context(), `
					UPDATE deliveries
					SET lease_until = clock_timestamp() - interval '1 second'
					WHERE id = $1
				`, task.ID); err != nil {
					t.Fatal(err)
				}
			}
			if test.clearLease {
				if _, err := pool.Exec(t.Context(), `
					UPDATE deliveries
					SET
						status = 'pending',
						lease_owner = NULL,
						lease_token = NULL,
						lease_until = NULL
					WHERE id = $1
				`, task.ID); err != nil {
					t.Fatal(err)
				}
			}
			status := 204
			result := notifierdelivery.AttemptResult{
				DeliveryID:     task.ID,
				Generation:     task.Generation,
				LeaseOwner:     task.LeaseOwner,
				LeaseToken:     task.LeaseToken,
				ResponseStatus: &status,
				StartedAt:      time.Now().Add(-time.Millisecond),
				FinishedAt:     time.Now(),
			}
			test.mutate(&result)
			if err := store.CompleteSuccess(t.Context(), result); !errors.Is(err, notifierdelivery.ErrWorkerLeaseLost) {
				t.Fatalf("fenced result = %v, want ErrWorkerLeaseLost", err)
			}

			var deliveryStatus string
			var attempts int
			if err := pool.QueryRow(t.Context(), `
				SELECT
					(SELECT status FROM deliveries WHERE id = $1),
					(SELECT count(*) FROM delivery_attempts WHERE delivery_id = $1)
			`, task.ID).Scan(&deliveryStatus, &attempts); err != nil {
				t.Fatal(err)
			}
			if deliveryStatus == "succeeded" || attempts != 0 {
				t.Fatalf("stale result changed status=%q attempts=%d", deliveryStatus, attempts)
			}
		})
	}
}

func TestSlice3StaleAndEarlySignalsCannotAcquireLease(t *testing.T) {
	accepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"claim":"guarded"}`),
	)
	if accepted.StatusCode != 202 {
		t.Fatalf("submit status = %d, body = %s", accepted.StatusCode, accepted.Body)
	}
	pool := integrationPool(t)
	store, err := notifierdelivery.NewStore(pool, 100000)
	if err != nil {
		t.Fatal(err)
	}

	if _, _, err := store.ClaimDelivery(
		t.Context(),
		accepted.Delivery.ID,
		accepted.Delivery.Generation+1,
		"worker-newer-message",
		30*time.Second,
	); !errors.Is(err, notifierdelivery.ErrDeliveryNotClaimable) {
		t.Fatalf("wrong-generation claim = %v, want ErrDeliveryNotClaimable", err)
	}
	if _, err := pool.Exec(t.Context(), `
		UPDATE deliveries
		SET next_attempt_at = clock_timestamp() + interval '1 hour'
		WHERE id = $1
	`, accepted.Delivery.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ClaimDelivery(
		t.Context(),
		accepted.Delivery.ID,
		accepted.Delivery.Generation,
		"worker-early-message",
		30*time.Second,
	); !errors.Is(err, notifierdelivery.ErrDeliveryNotClaimable) {
		t.Fatalf("early claim = %v, want ErrDeliveryNotClaimable", err)
	}

	var status string
	var leaseToken *string
	if err := pool.QueryRow(t.Context(), `
		SELECT status, lease_token::text
		FROM deliveries
		WHERE id = $1
	`, accepted.Delivery.ID).Scan(&status, &leaseToken); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || leaseToken != nil {
		t.Fatalf("guarded delivery status=%q lease=%v, want pending without lease", status, leaseToken)
	}
}

func TestSlice3RegisteredHTTPSDeliveryCommitsSuccess(t *testing.T) {
	if err := os.Setenv("WORKER_ENABLED", "true"); err != nil {
		t.Fatal(err)
	}
	compose(t, "up", "-d", "--force-recreate", "--wait", "--wait-timeout", "120", "app")
	t.Cleanup(func() {
		if err := os.Setenv("WORKER_ENABLED", "false"); err != nil {
			t.Errorf("disable Worker for integration cleanup: %v", err)
			return
		}
		composeCleanup(t, "up", "-d", "--force-recreate", "--wait", "--wait-timeout", "120", "app")
	})

	accepted := submitDelivery(
		t,
		testCallerKey,
		uniqueKey(t),
		testDestination,
		[]byte(`{"slice":3,"event":"first-https-delivery"}`),
	)
	if accepted.StatusCode != 202 {
		t.Fatalf("submit status = %d, body = %s", accepted.StatusCode, accepted.Body)
	}

	pool := integrationPool(t)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var status string
		if err := pool.QueryRow(t.Context(), `
			SELECT status
			FROM deliveries
			WHERE id = $1
		`, accepted.Delivery.ID).Scan(&status); err != nil {
			t.Fatal(err)
		}
		if status == "succeeded" {
			var attempts int
			var leaseCleared bool
			var storedCredential int
			if err := pool.QueryRow(t.Context(), `
				SELECT
					(SELECT count(*) FROM delivery_attempts
					 WHERE delivery_id = $1
					   AND generation = 0
					   AND result_class = 'succeeded'
					   AND response_status = 204),
					(SELECT lease_owner IS NULL
						AND lease_token IS NULL
						AND lease_until IS NULL
					 FROM deliveries WHERE id = $1),
					(SELECT count(*) FROM deliveries
					 WHERE id = $1
					   AND caller_headers::text LIKE '%fake-supplier-test-token%')
			`, accepted.Delivery.ID).Scan(&attempts, &leaseCleared, &storedCredential); err != nil {
				t.Fatal(err)
			}
			if attempts != 1 || !leaseCleared || storedCredential != 0 {
				t.Fatalf(
					"success attempts=%d leaseCleared=%v storedCredential=%d",
					attempts,
					leaseCleared,
					storedCredential,
				)
			}
			broker, err := dispatch.NewRabbitBroker(rabbitURL(t))
			if err != nil {
				t.Fatal(err)
			}
			if err := broker.Publish(t.Context(), dispatch.Signal{
				DeliveryID: accepted.Delivery.ID,
				Generation: accepted.Delivery.Generation,
				TraceID:    "00000000-0000-4000-8000-000000000003",
			}); err != nil {
				_ = broker.Close()
				t.Fatal(err)
			}
			_ = broker.Close()
			time.Sleep(500 * time.Millisecond)
			if err := pool.QueryRow(t.Context(), `
				SELECT count(*)
				FROM delivery_attempts
				WHERE delivery_id = $1
			`, accepted.Delivery.ID).Scan(&attempts); err != nil {
				t.Fatal(err)
			}
			if attempts != 1 {
				t.Fatalf("duplicate terminal signal created %d attempts, want 1", attempts)
			}
			assertAppLogsRedacted(t)
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("delivery did not reach succeeded; secure Worker behavior is missing")
}

func assertAppLogsRedacted(t *testing.T) {
	t.Helper()
	command := exec.Command(
		"docker",
		"compose",
		"--project-name", requiredEnv(t, "COMPOSE_PROJECT_NAME"),
		"--project-directory", requiredEnv(t, "REPO_ROOT"),
		"logs",
		"--no-color",
		"app",
	)
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("read app logs: %v\n%s", err, output)
	}
	logs := strings.ToLower(string(output))
	for _, forbidden := range []string{
		"fake-supplier-test-token",
		"authorization:",
		`"first-https-delivery"`,
	} {
		if strings.Contains(logs, strings.ToLower(forbidden)) {
			t.Fatalf("app logs leaked forbidden value %q", forbidden)
		}
	}
}
