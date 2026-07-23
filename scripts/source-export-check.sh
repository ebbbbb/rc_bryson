#!/bin/sh
set -eu

repo_root=$(git rev-parse --show-toplevel)
scratch_parent="$repo_root/.source-export"
scratch="$scratch_parent/source"

cleanup() {
	rm -rf "$scratch_parent"
}
trap cleanup EXIT INT TERM

mkdir -p "$scratch"
(
	cd "$repo_root"
	tar \
		--exclude=.git \
		--exclude=.source-export \
		--exclude=bin \
		--exclude=coverage \
		-cf - .
) | (
	cd "$scratch"
	tar -xf -
)

git -C "$scratch" init -q
$(command -v make) -C "$scratch" format-check lint validate-skills test test-race
docker compose -f "$scratch/compose.yaml" config --quiet
echo "PASS source-only export reproduces non-integration checks and Compose parsing"
