#!/usr/bin/env bash
# codex-shim-bg.sh — detached supervised codex launcher.
#
# Returns immediately (~1s) with task metadata; the vo-telemetry ShimSupervisor runs INSIDE the detached
# process, so run_started/lane_started fire at launch and lane_stopped/run_closed fire when codex
# exits — surviving the 5-min Bash-tool ceiling AND subagent/session reaping. The task shows as its
# own dedicated run on the liveness dashboard.
#
# Part of vo-telemetry: src/cc-host/shims/. Review the integration before installing it.
#
# Usage:
#   codex-shim-bg.sh <prompt-source> [extra codex-exec args]
#   <prompt-source>  filepath, OR "-" to read from stdin
# Poll $TASK_DIR/exit_code (exists = done; contents = code); read $TASK_DIR/stdout.log for output.
#
# Sandbox BYPASS is opt-in (CC_HOST_ALLOW_UNRESTRICTED=1); default is sandboxed workspace-write.
# Overrides: CC_HOST_DIR, CC_HOST_PYTHON, CODEX_BG_LAUNCHER (detachment prefix).
set -euo pipefail
umask 077

SOURCE="${1:?prompt source required (filepath or - for stdin)}"
shift

CC_HOST_DIR="${CC_HOST_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PY="${CC_HOST_PYTHON:-python3}"

TASK_ID="codex-$(date +%Y%m%dT%H%M%S)-$$"
TASK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/vo-telemetry-codex.XXXXXXXX")"

if [ "$SOURCE" = "-" ]; then
  cat > "$TASK_DIR/prompt.txt"
else
  [ -r "$SOURCE" ] || { echo "codex-shim-bg: cannot read $SOURCE" >&2; exit 66; }
  cat "$SOURCE" > "$TASK_DIR/prompt.txt"
fi
printf '%s\n' "$@" > "$TASK_DIR/codex_args.txt"

CODEX_MODEL="${CODEX_MODEL:-}"
CODEX_CONFIG="${CC_HOST_CODEX_CONFIG_FILE:-$HOME/.codex/config.toml}"
if [ -z "$CODEX_MODEL" ] && [ -r "$CODEX_CONFIG" ]; then
  CODEX_MODEL="$(sed -n 's/^model *= *"\(.*\)".*/\1/p' "$CODEX_CONFIG" 2>/dev/null | head -1 || true)"
fi
CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"
_prev=""
for _a in "$@"; do
  case "$_prev" in -m|--model) CODEX_MODEL="$_a" ;; esac
  case "$_a" in --model=*) CODEX_MODEL="${_a#--model=}" ;; esac
  _prev="$_a"
done
case "$CODEX_MODEL" in
  "" | *[!A-Za-z0-9._/@:-]*) echo "codex-shim-bg: invalid model slug '$CODEX_MODEL'" >&2; exit 64 ;;
esac

if [ -n "${CODEX_BG_LAUNCHER:-}" ]; then LAUNCHER="$CODEX_BG_LAUNCHER"
elif command -v setsid >/dev/null 2>&1; then LAUNCHER="setsid nohup"
elif command -v nohup  >/dev/null 2>&1; then LAUNCHER="nohup"
else echo "codex-shim-bg: neither setsid nor nohup found — cannot detach" >&2; exit 78; fi

# The detached inner command IS the supervisor (not bare codex), so the never-drop lane_stopped /
# run_closed fire from the same process that waits on codex. CLAUDE_CODE_SESSION_ID + TRACEPARENT are
# inherited via the environment; CC_HOST_TASK_ID makes the dedicated run/lane human-traceable.
# Command-injection hardening: pass every VALUE (python, model, task dir, args) as an env var and
# reference it BY NAME inside a fully single-quoted bash -c body — nothing is interpolated into the
# script text, so a quote in CODEX_MODEL cannot break out of the detached shell. Forwarded args are
# pre-escaped with `printf %q` and rebuilt with `eval set --` (safe: %q output is shell-reparseable).
ESC_ARGS="$(printf '%q ' "$@")"
$LAUNCHER env \
  PYTHONPATH="${CC_HOST_DIR}${PYTHONPATH:+:$PYTHONPATH}" \
  CC_HOST_TASK_ID="$TASK_ID" \
  CC_HOST_ALLOW_UNRESTRICTED="${CC_HOST_ALLOW_UNRESTRICTED:-0}" \
  CC_HOST_CHILD_TIMEOUT_SEC="${CC_HOST_CHILD_TIMEOUT_SEC:-3600}" \
  OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES:+${OTEL_RESOURCE_ATTRIBUTES},}gen_ai.request.model=${CODEX_MODEL}" \
  SHIM_PY="$PY" SHIM_MODEL="$CODEX_MODEL" SHIM_TASK_DIR="$TASK_DIR" SHIM_ARGS="$ESC_ARGS" \
  bash -c '
    eval "set -- $SHIM_ARGS"
    "$SHIM_PY" -m cc_host.shim --cli codex --provider openai --model "$SHIM_MODEL" --self-attribute -- "$@" \
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

# Surface the sandbox posture: the detached supervisor's stderr bypass-warning goes to /dev/null, so
# this is the operator's only signal that the job runs unrestricted.
if [ "${CC_HOST_ALLOW_UNRESTRICTED:-0}" = "0" ]; then
  SANDBOX_STATUS="sandboxed (--sandbox workspace-write)"
else
  SANDBOX_STATUS="BYPASSED — no sandbox/approvals (CC_HOST_ALLOW_UNRESTRICTED=1)"
fi

cat <<EOF
task_id: $TASK_ID
task_dir: $TASK_DIR
agent_id: codex:$TASK_ID
sandbox: $SANDBOX_STATUS
launcher_pid: $BG_PID
launcher: $LAUNCHER
started_at: $(cat "$TASK_DIR/started_at")
stdout: $TASK_DIR/stdout.log
stderr: $TASK_DIR/stderr.log
status: while [ ! -f $TASK_DIR/exit_code ]; do sleep 30; done; cat $TASK_DIR/exit_code
result: cat $TASK_DIR/stdout.log
EOF
