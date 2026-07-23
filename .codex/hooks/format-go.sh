#!/bin/bash
set -euo pipefail

input=$(cat)
cwd=$(jq -er '.cwd | select(type == "string" and length > 0)' <<<"$input")
repo_root=$(git -C "$cwd" rev-parse --show-toplevel)
gofmt_bin=${GOFMT_BIN:-gofmt}

edit_succeeded() {
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

edit_succeeded || exit 0

paths=$(
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
)

formatted=0
while IFS= read -r path; do
	[[ -n "$path" ]] || continue
	if [[ "$path" = "$repo_root/"* ]]; then
		path=${path#"$repo_root/"}
	fi
	case "$path" in
		/*|../*|*/../*|vendor/*|*/vendor/*) continue ;;
		*.go) ;;
		*) continue ;;
	esac
	file="$repo_root/$path"
	[[ -f "$file" ]] || continue
	[[ -n "$(git -C "$repo_root" status --porcelain -- "$path")" ]] || continue
	if ! "$gofmt_bin" -w -- "$file"; then
		echo "format-go hook: gofmt failed for a modified Go file" >&2
		exit 2
	fi
	formatted=$((formatted + 1))
done <<<"$paths"

if (( formatted > 0 )); then
	jq -n --arg message "gofmt applied to $formatted modified Go file(s)." \
		'{systemMessage: $message}'
fi
