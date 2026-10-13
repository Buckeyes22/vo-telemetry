#!/usr/bin/env bash
# codex-shim.sh — supervised `codex exec` via the vo-telemetry ShimSupervisor.
#
# Emits run_started/lane_started/tool_*/lane_stopped/run_closed so a codex subagent appears as a
# LIVE LANE on the liveness dashboard (in addition to its gen_ai trace spans). Without the
# supervisor, a Bash-launched codex never trips Claude Code's SubagentStart/Stop hooks and is
# invisible to the liveness plane.
#
# Part of vo-telemetry: src/cc-host/shims/. Review the integration before installing it.
#
# Usage:
#   codex-shim.sh <prompt-source> [extra codex-exec args]
#   <prompt-source>  filepath, OR "-" to read from stdin
# Exits with codex's exit code. Stdout = codex output.
#
# The supervisor injects `exec --json --skip-git-repo-check` itself, so pass ONLY forwarded
# codex-exec args here (e.g. -m gpt-5.4 to override the model; -p/--profile selects a config block,
# NOT a model). Sandbox/approval BYPASS is opt-in; the default is sandboxed (--sandbox workspace-write).
#
# 5-MIN CEILING: when dispatched via a Claude Code subagent's Bash tool, the 300s timeout still
# applies. For long runs use codex-shim-bg.sh (detached; the supervisor runs inside the detached
# process so lane_stopped/run_closed still fire).
#
# Overrides: CC_HOST_DIR (path to src/cc-host), CC_HOST_PYTHON (python interpreter).
set -euo pipefail

SOURCE="${1:?prompt source required (filepath or - for stdin)}"
shift

CC_HOST_DIR="${CC_HOST_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PY="${CC_HOST_PYTHON:-python3}"
export PYTHONPATH="${CC_HOST_DIR}${PYTHONPATH:+:$PYTHONPATH}"

# Keep the child sandboxed unless explicitly configured otherwise.
: "${CC_HOST_ALLOW_UNRESTRICTED:=0}"
export CC_HOST_ALLOW_UNRESTRICTED

# Model label for trace spans: codex reports usage but no model attribute, and its Rust OTel SDK
# honours OTEL_RESOURCE_ATTRIBUTES, so the collector can copy the label onto each span. Default =
# config.toml model; an explicit -p/--profile/-m/--model/-c model= override in the forwarded args wins.
CODEX_MODEL="${CODEX_MODEL:-}"
CODEX_CONFIG="${CC_HOST_CODEX_CONFIG_FILE:-$HOME/.codex/config.toml}"
if [ -z "$CODEX_MODEL" ] && [ -r "$CODEX_CONFIG" ]; then
  CODEX_MODEL="$(sed -n 's/^model *= *"\(.*\)".*/\1/p' "$CODEX_CONFIG" 2>/dev/null | head -1 || true)"
fi
CODEX_MODEL="${CODEX_MODEL:-gpt-5.5}"
_prev=""
for _a in "$@"; do
  case "$_prev" in -p|--profile|-m|--model) CODEX_MODEL="$_a" ;; esac
  case "$_a" in model=*) CODEX_MODEL="${_a#model=}" ;; esac
  _prev="$_a"
done
export OTEL_RESOURCE_ATTRIBUTES="${OTEL_RESOURCE_ATTRIBUTES:+${OTEL_RESOURCE_ATTRIBUTES},}gen_ai.request.model=${CODEX_MODEL}"

if [ "$SOURCE" = "-" ]; then
  exec "$PY" -m cc_host.shim --cli codex --provider openai --model "$CODEX_MODEL" --self-attribute -- "$@"
else
  [ -r "$SOURCE" ] || { echo "codex-shim: cannot read $SOURCE" >&2; exit 66; }
  exec "$PY" -m cc_host.shim --cli codex --provider openai --model "$CODEX_MODEL" --self-attribute -- "$@" < "$SOURCE"
fi
