#!/bin/sh
set -eu

missing=
for command_name in bash sh jq curl git go gofmt docker make awk sed tar; do
	if ! command -v "$command_name" >/dev/null 2>&1; then
		missing="$missing $command_name"
	fi
done
if [ -n "$missing" ]; then
	echo "preflight: missing required local tool(s):$missing" >&2
	exit 1
fi

go_version=$(go env GOVERSION)
case "$go_version" in
	go1.26.*) ;;
	*)
		echo "preflight: Go 1.26.x required, found $go_version" >&2
		exit 1
		;;
esac

docker compose version >/dev/null
docker info >/dev/null

echo "PASS preflight: Go $go_version, Shell, jq, curl, Git, Make, and Docker Compose"
