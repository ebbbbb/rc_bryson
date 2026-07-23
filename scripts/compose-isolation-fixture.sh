#!/bin/sh
set -eu

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"
fixture_dir=$(mktemp -d "$repo_root/.compose-isolation.XXXXXX")

cleanup() {
	rm -rf "$fixture_dir"
}
trap cleanup EXIT INT TERM

before_dev=$(docker compose ps -aq | sort)

./scripts/compose-test.sh integration >"$fixture_dir/first.log" 2>&1 &
first_pid=$!
./scripts/compose-test.sh integration >"$fixture_dir/second.log" 2>&1 &
second_pid=$!

first_status=0
second_status=0
wait "$first_pid" || first_status=$?
wait "$second_pid" || second_status=$?
if [ "$first_status" -ne 0 ] || [ "$second_status" -ne 0 ]; then
	cat "$fixture_dir/first.log" "$fixture_dir/second.log" >&2
	exit 1
fi

after_dev=$(docker compose ps -aq | sort)
test "$before_dev" = "$after_dev"

if docker ps -a --filter label=com.docker.compose.project \
	--format '{{.Label "com.docker.compose.project"}}' |
	grep -Eq '^rn-integration-'; then
	echo "compose isolation fixture: test container leaked" >&2
	exit 1
fi
if docker volume ls --filter label=com.docker.compose.project \
	--format '{{.Label "com.docker.compose.project"}}' |
	grep -Eq '^rn-integration-'; then
	echo "compose isolation fixture: test volume leaked" >&2
	exit 1
fi

echo "PASS concurrent integration runs use isolated projects and leave development state unchanged"
