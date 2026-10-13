#!/usr/bin/env bash
# opencode-shim-bg.sh — detached supervised opencode launcher (mirrors codex-shim-bg.sh).
#
# Returns immediately; the ShimSupervisor runs inside the detached process so lane_stopped/run_closed
# fire when opencode exits, past the 5-min Bash-tool ceiling. Dedicated run per task on the dashboard.
#
# Part of vo-telemetry: src/cc-host/shims/. Review the integration before installing it.
#
# Usage:
#   opencode-shim-bg.sh <provider/model> <prompt-source> [extra opencode-run args]
# Poll $TASK_DIR/exit_code; read $TASK_DIR/stdout.log.
# Overrides: CC_HOST_DIR, CC_HOST_PYTHON, OPENCODE_BIN, OPENCODE_BG_LAUNCHER.
set -euo pipefail
umask 077

MODEL="${1:?model required (provider/model, e.g. kimi-for-coding/k2p6)}"
SOURCE="${2:?prompt source required (filepath or - for stdin)}"
shift 2

CC_HOST_DIR="${CC_HOST_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PY="${CC_HOST_PYTHON:-python3}"
# Allowlist the provider/model slug (defense in depth atop the env-var passing below).
case "$MODEL" in
  "" | *[!A-Za-z0-9._/@:-]*) echo "opencode-shim-bg: invalid model slug '$MODEL'" >&2; exit 64 ;;
esac
PROVIDER="${MODEL%%/*}"

if ! command -v opencode >/dev/null 2>&1 && [ -x "$HOME/.opencode/bin/opencode" ]; then
  export OPENCODE_BIN="$HOME/.opencode/bin/opencode"
fi

TASK_ID="opencode-$(date +%Y%m%dT%H%M%S)-$$"
TASK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/vo-telemetry-opencode.XXXXXXXX")"

if [ "$SOURCE" = "-" ]; then
  cat > "$TASK_DIR/prompt.txt"
else
  [ -r "$SOURCE" ] || { echo "opencode-shim-bg: cannot read $SOURCE" >&2; exit 66; }
  cat "$SOURCE" > "$TASK_DIR/prompt.txt"
fi
printf '%s\n' "$@" > "$TASK_DIR/opencode_args.txt"

if [ -n "${OPENCODE_BG_LAUNCHER:-}" ]; then LAUNCHER="$OPENCODE_BG_LAUNCHER"
elif command -v setsid >/dev/null 2>&1; then LAUNCHER="setsid nohup"
elif command -v nohup  >/dev/null 2>&1; then LAUNCHER="nohup"
else echo "opencode-shim-bg: neither setsid nor nohup found — cannot detach" >&2; exit 78; fi

# Command-injection hardening: pass every VALUE (python, provider, model, task dir, args) as an env
# var and reference it BY NAME inside a fully single-quoted bash -c body — nothing is interpolated
# into the script text, so a quote in MODEL/PROVIDER cannot break out of the detached shell. The
# forwarded args are pre-escaped with `printf %q` and rebuilt with `eval set --` (safe: %q output is
# shell-reparseable to the originals).
ESC_ARGS="$(printf '%q ' "$@")"
$LAUNCHER env \
  PYTHONPATH="${CC_HOST_DIR}${PYTHONPATH:+:$PYTHONPATH}" \
  CC_HOST_TASK_ID="$TASK_ID" \
  CC_HOST_ALLOW_UNRESTRICTED="${CC_HOST_ALLOW_UNRESTRICTED:-0}" \
  CC_HOST_CHILD_TIMEOUT_SEC="${CC_HOST_CHILD_TIMEOUT_SEC:-3600}" \
  OPENCODE_ENABLE_TELEMETRY="${OPENCODE_ENABLE_TELEMETRY:-1}" \
  OPENCODE_OTLP_PROTOCOL="${OPENCODE_OTLP_PROTOCOL:-http/protobuf}" \
  OPENCODE_OTLP_ENDPOINT="${OPENCODE_OTLP_ENDPOINT:-http://localhost:4318}" \
  OPENCODE_RESOURCE_ATTRIBUTES="${OPENCODE_RESOURCE_ATTRIBUTES:-service.name=opencode}" \
  ${OPENCODE_BIN:+OPENCODE_BIN="$OPENCODE_BIN"} \
  SHIM_PY="$PY" SHIM_PROVIDER="$PROVIDER" SHIM_MODEL="$MODEL" SHIM_TASK_DIR="$TASK_DIR" SHIM_ARGS="$ESC_ARGS" \
  bash -c '
    eval "set -- $SHIM_ARGS"
    "$SHIM_PY" -m cc_host.shim --cli opencode --provider "$SHIM_PROVIDER" --model "$SHIM_MODEL" --self-attribute -- -m "$SHIM_MODEL" "$@" \
      < "$SHIM_TASK_DIR/prompt.txt" > "$SHIM_TASK_DIR/stdout.log" 2> "$SHIM_TASK_DIR/stderr.log"
    rc=$?
    date -u +%Y-%m-%dT%H:%M:%SZ > "$SHIM_TASK_DIR/completed_at"
    # Publish exit_code last and atomically: pollers treat its existence as "done".
    echo "$rc" > "$SHIM_TASK_DIR/exit_code.tmp"
    mv "$SHIM_TASK_DIR/exit_code.tmp" "$SHIM_TASK_DIR/exit_code"
  ' </dev/null >/dev/null 2>&1 &

BG_PID=$!
disown "$BG_PID" 2>/dev/null || true
echo "$BG_PID" > "$TASK_DIR/launcher_pid"
echo "$LAUNCHER" > "$TASK_DIR/detachment_method"
echo "$TASK_ID" > "$TASK_DIR/agent_id_hint"
date -u +%Y-%m-%dT%H:%M:%SZ > "$TASK_DIR/started_at"

# Surface the sandbox posture to the caller: the detached supervisor's stderr bypass-warning goes to
# /dev/null, so this metadata line is the operator's only signal that the job runs unrestricted.
if [ "${CC_HOST_ALLOW_UNRESTRICTED:-0}" = "0" ]; then
  SANDBOX_STATUS="sandboxed (opencode permission gate ON)"
else
  SANDBOX_STATUS="BYPASSED — no sandbox/approvals (CC_HOST_ALLOW_UNRESTRICTED=1)"
fi

cat <<EOF
task_id: $TASK_ID
task_dir: $TASK_DIR
agent_id: opencode:$TASK_ID
sandbox: $SANDBOX_STATUS
launcher_pid: $BG_PID
launcher: $LAUNCHER
started_at: $(cat "$TASK_DIR/started_at")
stdout: $TASK_DIR/stdout.log
stderr: $TASK_DIR/stderr.log
status: while [ ! -f $TASK_DIR/exit_code ]; do sleep 30; done; cat $TASK_DIR/exit_code
result: cat $TASK_DIR/stdout.log
EOF
