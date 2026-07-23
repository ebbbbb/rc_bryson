#!/bin/sh
set -eu

mode=${1:-}
case "$mode" in
	gate|integration) ;;
	*)
		echo "usage: $0 gate|integration" >&2
		exit 2
		;;
esac

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
run_dir=$(mktemp -d "$repo_root/.compose-${mode}.XXXXXX")
run_id=$(basename "$run_dir" | tr '[:upper:]' '[:lower:]' | tr -c 'a-z0-9_-' '-')
project="rn-${mode}-${run_id}-$$"

export POSTGRES_PORT=0
export RABBITMQ_PORT=0
export RABBITMQ_MANAGEMENT_PORT=0
export APP_PORT=0
if [ "$mode" = "integration" ]; then
	export WORKER_ENABLED=false
fi

compose() {
	docker compose --project-name "$project" --project-directory "$repo_root" "$@"
}

cleanup() {
	compose down --volumes --remove-orphans >/dev/null 2>&1 || true
	rm -rf "$run_dir"
}
trap cleanup EXIT INT TERM

compose up -d --build --wait --wait-timeout 120

app_endpoint=$(compose port app 8080)
app_port=${app_endpoint##*:}
curl --fail --silent --show-error "http://127.0.0.1:$app_port/healthz" >/dev/null
app_container=$(compose ps -q app)
test "$(docker inspect --format '{{.State.Health.Status}}' "$app_container")" = "healthy"
echo "PASS final app image starts and reports healthy"

if [ "$mode" = "integration" ]; then
	postgres_endpoint=$(compose port postgres 5432)
	postgres_port=${postgres_endpoint##*:}
	rabbit_endpoint=$(compose port rabbitmq 5672)
	rabbit_port=${rabbit_endpoint##*:}
	APP_HEALTH_URL="http://127.0.0.1:$app_port/healthz" \
		COMPOSE_PROJECT_NAME="$project" \
		DATABASE_URL="postgres://notifier:notifier-test-only@127.0.0.1:$postgres_port/notifier?sslmode=disable" \
		RABBITMQ_URL="amqp://notifier:notifier-test-only@127.0.0.1:$rabbit_port/" \
		REPO_ROOT="$repo_root" \
		go test -tags=integration ./test/integration
	app_endpoint=$(compose port app 8080)
	app_port=${app_endpoint##*:}
	persistence_key="restart-$(date +%s)-$$"
	persistence_payload='{"destination_id":"supplier-a","method":"POST","headers":{"Content-Type":"application/json","X-Event-Type":"restart"},"body_base64":"e30="}'
	persistence_response=$(
		curl --noproxy '*' --fail --silent --show-error \
			-H "Authorization: Bearer caller-a-test-key" \
			-H "Idempotency-Key: $persistence_key" \
			-H "Content-Type: application/json" \
			--data "$persistence_payload" \
			"http://127.0.0.1:$app_port/deliveries"
	)
	persistence_id=$(printf '%s' "$persistence_response" | jq -er '.id')
	compose restart app >/dev/null
	compose up -d --wait --wait-timeout 120 app >/dev/null
	persisted_status=
	attempt=0
	while [ "$attempt" -lt 20 ]; do
		if persisted_response=$(
			compose exec -T app wget -q -O - \
				--header "Authorization: Bearer caller-a-test-key" \
				"http://127.0.0.1:8080/deliveries/$persistence_id"
		); then
			persisted_status=$(printf '%s' "$persisted_response" | jq -er '.status')
			break
		fi
		attempt=$((attempt + 1))
		sleep 0.25
	done
	test "$persisted_status" = "pending"
	echo "PASS accepted delivery remains queryable after app restart"
	echo "PASS isolated Compose service integration"
	exit 0
fi

marker="pg-$(date +%s)-$$"
compose exec -T postgres psql -v ON_ERROR_STOP=1 -U notifier -d notifier \
	-c "CREATE TABLE IF NOT EXISTS toolchain_persistence_probe (id integer PRIMARY KEY, marker text NOT NULL)" \
	-c "INSERT INTO toolchain_persistence_probe (id, marker) VALUES (1, '$marker') ON CONFLICT (id) DO UPDATE SET marker = EXCLUDED.marker" \
	>/dev/null
compose restart postgres >/dev/null
compose up -d --wait --wait-timeout 120 postgres >/dev/null
stored_marker=$(compose exec -T postgres psql -At -v ON_ERROR_STOP=1 -U notifier -d notifier \
	-c "SELECT marker FROM toolchain_persistence_probe WHERE id = 1")
test "$stored_marker" = "$marker"
echo "PASS PostgreSQL test volume persists across restart"

rabbit_endpoint=$(compose port rabbitmq 5672)
rabbit_port=${rabbit_endpoint##*:}
export RABBITMQ_URL="amqp://notifier:notifier-test-only@127.0.0.1:$rabbit_port/"
queue="notifier.toolchain.restart-probe"
body="rabbit-$(date +%s)-$$"
go run ./cmd/toolchain-probe rabbit-publish "$queue" "$body"
compose restart rabbitmq >/dev/null
compose up -d --wait --wait-timeout 120 rabbitmq >/dev/null
rabbit_endpoint=$(compose port rabbitmq 5672)
rabbit_port=${rabbit_endpoint##*:}
export RABBITMQ_URL="amqp://notifier:notifier-test-only@127.0.0.1:$rabbit_port/"
consumed=false
attempt=0
while [ "$attempt" -lt 20 ]; do
	if go run ./cmd/toolchain-probe rabbit-consume "$queue" "$body"; then
		consumed=true
		break
	fi
	attempt=$((attempt + 1))
	sleep 0.25
done
test "$consumed" = "true"
echo "PASS RabbitMQ test volume retains a durable message across restart"

go run ./cmd/toolchain-probe rabbit-semantics
echo "PASS publisher confirm, manual ACK, and unacked redelivery"

validated_ip=$(compose exec -T fake-supplier hostname -i | awk '{print $1}')
compose build safe-dial-probe >/dev/null
compose run --rm --no-deps \
	-e "FAKE_SUPPLIER_VALIDATED_IP=$validated_ip" \
	safe-dial-probe
echo "PASS validated-IP dial with registered-hostname TLS verification"
