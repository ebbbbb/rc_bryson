#!/bin/bash
set -euo pipefail

action=${1:-}
input=$(cat)
cwd=$(jq -er '.cwd | select(type == "string" and length > 0)' <<<"$input")
session_id=$(jq -er '.session_id | select(type == "string" and length > 0)' <<<"$input")
repo_root=$(git -C "$cwd" rev-parse --show-toplevel)
git_dir=$(git -C "$repo_root" rev-parse --absolute-git-dir)
session_hash=$(printf '%s' "$session_id" | shasum -a 256 | awk '{print $1}')
state_dir="$git_dir/codex-hook-state/$session_hash"
state_file="$state_dir/production-files"
review_file="$state_dir/maintainability-review-required"
warned_file="$state_dir/threshold-warned"
lock_dir="$state_dir.lock"

tool_succeeded() {
	jq -e '
		.tool_response as $response
		| if ($response | type) != "object" then false
		  elif ($response | has("success")) then $response.success == true
		  else [
			$response.exit_code?,
			$response.exitCode?,
			$response.metadata.exit_code?,
			$response.result.exit_code?
		  ]
		  | map(select(type == "number"))
		  | (first // 1) == 0
		  end
	' >/dev/null <<<"$input"
}

acquire_lock() {
	mkdir -p "$(dirname "$state_dir")"
	local attempt=0
	while ! mkdir "$lock_dir" 2>/dev/null; do
		attempt=$((attempt + 1))
		if (( attempt >= 50 )); then
			echo "verify-gate hook: timed out waiting for repository session lock" >&2
			exit 2
		fi
		sleep 0.02
	done
	trap 'rmdir "$lock_dir" 2>/dev/null || true' EXIT
}

changed_paths() {
	jq -r '
		[
			.tool_input.file_path?,
			.tool_input.path?,
			(
				(.tool_input.command? // "")
				| split("\n")[]
				| select(test("^\\*\\*\\* (Add|Update|Move to) File: "))
				| sub("^\\*\\*\\* (Add|Update|Move to) File: "; "")
			)
		]
		| .[]
		| select(type == "string" and length > 0)
	' <<<"$input" | sort -u
}

is_production_go() {
	local path=$1
	if [[ "$path" = "$repo_root/"* ]]; then
		path=${path#"$repo_root/"}
	fi
	case "$path" in
		/*|../*|*/../*|vendor/*|*/vendor/*|test/*|*/testdata/*|*_test.go) return 1 ;;
		*.go) ;;
		*) return 1 ;;
	esac
	local file="$repo_root/$path"
	[[ -f "$file" ]] || return 1
	[[ -n "$(git -C "$repo_root" status --porcelain -- "$path")" ]] || return 1
	if head -n 20 "$file" | grep -Eq '^// Code generated .* DO NOT EDIT\.$'; then
		return 1
	fi
	printf '%s\n' "$path"
}

track() {
	tool_succeeded || return 0
	acquire_lock
	mkdir -p "$state_dir"
	local additions
	additions=$(
		while IFS= read -r path; do
			[[ -n "$path" ]] || continue
			is_production_go "$path" || true
		done < <(changed_paths)
	)
	[[ -n "$additions" ]] || return 0

	local next="$state_dir/production-files.next"
	{
		[[ -f "$state_file" ]] && cat "$state_file"
		printf '%s\n' "$additions"
	} | awk 'NF' | sort -u >"$next"
	mv "$next" "$state_file"
	local review_next="$state_dir/maintainability-review-required.next"
	{
		[[ -f "$review_file" ]] && cat "$review_file"
		printf '%s\n' "$additions"
	} | awk 'NF' | sort -u >"$review_next"
	mv "$review_next" "$review_file"

	local count
	count=$(wc -l <"$state_file" | tr -d ' ')
	if (( count >= 6 )) && [[ ! -f "$warned_file" ]]; then
		: >"$warned_file"
		jq -n --arg message \
			"$count distinct production Go files changed in this repository session; use \$review-maintainability and run make verify now." \
			'{systemMessage: $message, hookSpecificOutput: {hookEventName: "PostToolUse", additionalContext: $message}}'
	fi
}

record_verification() {
	local command
	command=$(jq -r '(.tool_input.command? // .tool_input.cmd? // "") | if type == "string" then gsub("^\\s+|\\s+$"; "") else "" end' <<<"$input")
	[[ "$command" = "make verify" ]] || return 0
	tool_succeeded || return 0

	acquire_lock
	rm -f "$state_file" "$warned_file"
	rmdir "$state_dir" 2>/dev/null || true
}

record_review() {
	local command
	command=$(jq -r '(.tool_input.command? // .tool_input.cmd? // "") | if type == "string" then gsub("^\\s+|\\s+$"; "") else "" end' <<<"$input")
	[[ "$command" = "make record-maintainability-review" ]] || return 0
	tool_succeeded || return 0

	acquire_lock
	rm -f "$review_file"
	rmdir "$state_dir" 2>/dev/null || true
}

stop_check() {
	if [[ "$(jq -r '.stop_hook_active? // false' <<<"$input")" = "true" ]]; then
		printf '{}\n'
		return 0
	fi
	local verification_required=false
	local review_required=false
	[[ -s "$state_file" ]] && verification_required=true
	[[ -s "$review_file" ]] && review_required=true
	if [[ "$verification_required" = true && "$review_required" = true ]]; then
		jq -n \
			'{decision: "block", reason: "Production Go files changed after the last successful checks. Continue the task: use $review-maintainability, settle any confirmed findings, run make record-maintainability-review, and run make verify before stopping."}'
	elif [[ "$review_required" = true ]]; then
		jq -n \
			'{decision: "block", reason: "Production Go files changed without maintainability-review evidence. Continue the task: use $review-maintainability, settle any confirmed findings, then run make record-maintainability-review before stopping."}'
	elif [[ "$verification_required" = true ]]; then
		jq -n \
			'{decision: "block", reason: "Production Go files changed after the last successful make verify. Continue the task, run make verify, and fix any failure before stopping."}'
	else
		printf '{}\n'
	fi
}

case "$action" in
	track) track ;;
	record-verification) record_verification ;;
	record-review) record_review ;;
	stop) stop_check ;;
	*)
		echo "verify-gate hook: expected track, record-verification, record-review, or stop" >&2
		exit 2
		;;
esac
