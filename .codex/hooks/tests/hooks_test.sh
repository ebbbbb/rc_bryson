#!/bin/bash
set -euo pipefail

project_root=$(git rev-parse --show-toplevel)
format_hook="$project_root/.codex/hooks/format-go.sh"
verify_hook="$project_root/.codex/hooks/verify-gate.sh"
fixture_root="$project_root/.hook-fixture"

cleanup() {
	rm -rf "$fixture_root"
}
trap cleanup EXIT INT TERM
cleanup
mkdir -p "$fixture_root"
git -C "$fixture_root" init -q
git -C "$fixture_root" config user.email fixture@example.invalid
git -C "$fixture_root" config user.name "Hook Fixture"

patch_input() {
	local session=$1
	local path=$2
	local success=${3:-true}
	jq -n \
		--arg cwd "$fixture_root" \
		--arg session "$session" \
		--arg command "*** Update File: $path" \
		--argjson success "$success" \
		'{cwd: $cwd, session_id: $session, hook_event_name: "PostToolUse", tool_name: "apply_patch", tool_input: {command: $command}, tool_response: {success: $success}}'
}

bash_input() {
	local session=$1
	local command=$2
	local exit_code=${3:-missing}
	if [[ "$exit_code" = "missing" ]]; then
		jq -n \
			--arg cwd "$fixture_root" \
			--arg session "$session" \
			--arg command "$command" \
			'{cwd: $cwd, session_id: $session, hook_event_name: "PostToolUse", tool_name: "Bash", tool_input: {command: $command}, tool_response: {output: "looks successful"}}'
	else
		jq -n \
			--arg cwd "$fixture_root" \
			--arg session "$session" \
			--arg command "$command" \
			--argjson exit_code "$exit_code" \
			'{cwd: $cwd, session_id: $session, hook_event_name: "PostToolUse", tool_name: "Bash", tool_input: {command: $command}, tool_response: {exit_code: $exit_code}}'
	fi
}

stop_input() {
	local session=$1
	local active=${2:-false}
	jq -n \
		--arg cwd "$fixture_root" \
		--arg session "$session" \
		--argjson active "$active" \
		'{cwd: $cwd, session_id: $session, hook_event_name: "Stop", stop_hook_active: $active}'
}

mkdir -p "$fixture_root/pkg"
mkdir -p "$fixture_root/.codex/hooks"
cp "$format_hook" "$verify_hook" "$fixture_root/.codex/hooks/"
printf 'package pkg\n\nfunc Value() int { return 1 }\n' >"$fixture_root/pkg/value.go"
git -C "$fixture_root" add .
git -C "$fixture_root" commit -qm baseline

printf 'package pkg\nfunc Value( )int{return 2}\n' >"$fixture_root/pkg/value.go"
patch_input failed-edit pkg/value.go false | bash "$format_hook" >/dev/null
test -n "$(gofmt -l "$fixture_root/pkg/value.go")"
patch_input failed-edit pkg/value.go false | bash "$verify_hook" track >/dev/null
test "$(stop_input failed-edit | bash "$verify_hook" stop | jq -r '.decision? // "allow"')" = "allow"
echo "PASS failed edit events neither format nor track files"

patch_input format-ok pkg/value.go | bash "$format_hook" >/dev/null
test -z "$(gofmt -l "$fixture_root/pkg/value.go")"
echo "PASS format hook formats only a changed Go target"

printf 'package pkg\nfunc Value( )int{return 22}\n' >"$fixture_root/pkg/value.go"
configured_format=$(jq -r '.hooks.PostToolUse[0].hooks[0].command' "$project_root/.codex/hooks.json")
(
	cd /
	patch_input cwd-independent pkg/value.go | /bin/sh -c "$configured_format" >/dev/null
)
test -z "$(gofmt -l "$fixture_root/pkg/value.go")"
echo "PASS hook bootstrap resolves the repository from input, not initial cwd"

configured_track=$(jq -r '.hooks.PostToolUse[0].hooks[1].command' "$project_root/.codex/hooks.json")
configured_record=$(jq -r '.hooks.PostToolUse[1].hooks[0].command' "$project_root/.codex/hooks.json")
configured_stop=$(jq -r '.hooks.Stop[0].hooks[0].command' "$project_root/.codex/hooks.json")
printf '\n// bootstrap tracking\n' >>"$fixture_root/pkg/value.go"
(
	cd /
	patch_input bootstrap-all pkg/value.go | /bin/sh -c "$configured_track" >/dev/null
)
test "$(
	cd /
	stop_input bootstrap-all | /bin/sh -c "$configured_stop" | jq -r '.decision'
)" = "block"
(
	cd /
	bash_input bootstrap-all "make verify" 0 | /bin/sh -c "$configured_record"
)
test "$(
	cd /
	stop_input bootstrap-all | /bin/sh -c "$configured_stop" | jq -r '.decision? // "allow"'
)" = "allow"
echo "PASS every configured hook resolves repository state without initial cwd"

printf 'package pkg\nfunc Value( )int{return 3}\n' >"$fixture_root/pkg/value.go"
if patch_input format-fail pkg/value.go | GOFMT_BIN=/usr/bin/false bash "$format_hook" >"$fixture_root/format-fail.out" 2>"$fixture_root/format-fail.err"; then
	echo "format failure fixture unexpectedly passed" >&2
	exit 1
fi
grep -q "gofmt failed" "$fixture_root/format-fail.err"
echo "PASS format hook reports formatter failure"

for number in 1 2 3 4 5 6 7; do
	printf 'package pkg\n\nfunc F%s() {}\n' "$number" >"$fixture_root/pkg/file${number}.go"
done
git -C "$fixture_root" add .
git -C "$fixture_root" commit -qm "add tracking files"
for number in 1 2 3 4 5 6 7; do
	printf '\n// changed\n' >>"$fixture_root/pkg/file${number}.go"
done

for number in 1 2 3 4 5 6; do
	patch_input session-a "pkg/file${number}.go" |
		bash "$verify_hook" track >"$fixture_root/session-a-${number}.out" &
done
patch_input session-b pkg/file7.go |
	bash "$verify_hook" track >"$fixture_root/session-b.out" &
wait

warning_count=$(grep -l "run make verify now" "$fixture_root"/session-a-*.out | wc -l | tr -d ' ')
test "$warning_count" = "1"
test ! -s "$fixture_root/session-b.out"
echo "PASS tracking is lock-safe and isolated by repository plus session"

bash_input session-a "make verify" 0 | bash "$verify_hook" record-verification
test "$(stop_input session-a | bash "$verify_hook" stop | jq -r '.decision? // "allow"')" = "allow"
test "$(stop_input session-b | bash "$verify_hook" stop | jq -r '.decision? // "allow"')" = "block"
echo "PASS successful verification resets only its own session"

patch_input reset-fixture pkg/file1.go | bash "$verify_hook" track >/dev/null
bash_input reset-fixture "make lint" 0 | bash "$verify_hook" record-verification
test "$(stop_input reset-fixture | bash "$verify_hook" stop | jq -r '.decision')" = "block"
bash_input reset-fixture "make verify" 1 | bash "$verify_hook" record-verification
test "$(stop_input reset-fixture | bash "$verify_hook" stop | jq -r '.decision')" = "block"
bash_input reset-fixture "make verify" missing | bash "$verify_hook" record-verification
test "$(stop_input reset-fixture | bash "$verify_hook" stop | jq -r '.decision')" = "block"
bash_input reset-fixture "make verify" 0 | bash "$verify_hook" record-verification
test "$(stop_input reset-fixture | bash "$verify_hook" stop | jq -r '.decision? // "allow"')" = "allow"
patch_input reset-fixture pkg/file1.go | bash "$verify_hook" track >/dev/null
test "$(stop_input reset-fixture | bash "$verify_hook" stop | jq -r '.decision')" = "block"
echo "PASS only successful make verify resets state, and later edits invalidate it"

patch_input recursive-stop pkg/file2.go | bash "$verify_hook" track >/dev/null
test "$(stop_input recursive-stop true | bash "$verify_hook" stop | jq -r '.decision? // "allow"')" = "allow"
echo "PASS recursive Stop invocation is allowed"

jq -e '
	.hooks.PostToolUse | length == 2
	and ([.[] | select(.matcher == "apply_patch|Edit|Write")] | length == 1)
' "$project_root/.codex/hooks.json" >/dev/null
echo "PASS hooks.json fixture matches the project hook schema shape"

grep -q 'Run `make verify` as the single completion path' "$project_root/.agents/skills/verify/SKILL.md"
test "$(grep -c 'make verify' "$project_root/.agents/skills/verify/SKILL.md")" -ge 1
echo "PASS verify skill declares the same sole completion path as the hook"
