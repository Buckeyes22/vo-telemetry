#!/usr/bin/env bash
# qwen-shim.sh — supervised `qwen` headless run via the vo-telemetry ShimSupervisor.
#
# Part of vo-telemetry: src/cc-host/shims/. Review the integration before installing it.
#
# Usage:
#   qwen-shim.sh <prompt-source> [extra qwen args]
#   <prompt-source>  filepath, OR "-" to read from stdin
# Exits with qwen's exit code. Stdout = qwen output.
set -euo pipefail

SOURCE="${1:?prompt source required (filepath or - for stdin)}"
shift

CC_HOST_DIR="${CC_HOST_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PY="${CC_HOST_PYTHON:-python3}"
export PYTHONPATH="${CC_HOST_DIR}${PYTHONPATH:+:$PYTHONPATH}"

: "${CC_HOST_ALLOW_UNRESTRICTED:=0}"
export CC_HOST_ALLOW_UNRESTRICTED

if ! command -v qwen >/dev/null 2>&1; then
  if [ -x "$HOME/.local/bin/qwen" ]; then
    export QWEN_BIN="$HOME/.local/bin/qwen"
  elif [ -x "$HOME/.qwen/bin/qwen" ]; then
    export QWEN_BIN="$HOME/.qwen/bin/qwen"
  fi
fi

QWEN_MODEL="${QWEN_MODEL:-example-model}"
: "${QWEN_AUTH_TYPE:=openai}"
: "${QWEN_OPENAI_BASE_URL:=http://127.0.0.1:8080/v1}"
: "${QWEN_OPENAI_API_KEY:?Set QWEN_OPENAI_API_KEY for your local model server}"
export QWEN_AUTH_TYPE QWEN_OPENAI_BASE_URL QWEN_OPENAI_API_KEY
_prev=""
for _a in "$@"; do
  case "$_prev" in -m|--model) QWEN_MODEL="$_a" ;; esac
  case "$_a" in --model=*) QWEN_MODEL="${_a#--model=}" ;; esac
  _prev="$_a"
done

if [ "$SOURCE" = "-" ]; then
  exec "$PY" -m cc_host.shim --cli qwen --provider qwen --model "$QWEN_MODEL" --self-attribute -- --model "$QWEN_MODEL" "$@"
else
  [ -r "$SOURCE" ] || { echo "qwen-shim: cannot read $SOURCE" >&2; exit 66; }
  exec "$PY" -m cc_host.shim --cli qwen --provider qwen --model "$QWEN_MODEL" --self-attribute -- --model "$QWEN_MODEL" "$@" < "$SOURCE"
fi
