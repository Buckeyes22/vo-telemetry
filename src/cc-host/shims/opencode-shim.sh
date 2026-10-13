#!/usr/bin/env bash
# opencode-shim.sh — supervised `opencode run` via the vo-telemetry ShimSupervisor (emits a live lane).
#
# Part of vo-telemetry: src/cc-host/shims/. Review the integration before installing it.
#
# Usage:
#   opencode-shim.sh <provider/model> <prompt-source> [extra opencode-run args]
#   <provider/model>  e.g. kimi-for-coding/k2p6, zai-coding-plan/glm-5.1, openai/gpt-5.5
#   <prompt-source>   filepath, OR "-" to read from stdin
# Exits with opencode's exit code.
#
# The supervisor injects `run --format json` (and --dangerously-skip-permissions when unrestricted);
# this wrapper passes `-m <model>`. Permission bypass is opt-in; the default keeps the permission gate.
# Overrides: CC_HOST_DIR, CC_HOST_PYTHON, OPENCODE_BIN.
set -euo pipefail

MODEL="${1:?model required (provider/model, e.g. kimi-for-coding/k2p6)}"
SOURCE="${2:?prompt source required (filepath or - for stdin)}"
shift 2

CC_HOST_DIR="${CC_HOST_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
PY="${CC_HOST_PYTHON:-python3}"
export PYTHONPATH="${CC_HOST_DIR}${PYTHONPATH:+:$PYTHONPATH}"

# Keep the permission gate unless explicitly configured otherwise.
: "${CC_HOST_ALLOW_UNRESTRICTED:=0}"
export CC_HOST_ALLOW_UNRESTRICTED

# Provider = segment before '/' (kimi-for-coding/k2p6 -> kimi-for-coding); used for lane attribution.
PROVIDER="${MODEL%%/*}"

# Preserve the prior shim's binary fallback when opencode isn't on PATH.
if ! command -v opencode >/dev/null 2>&1 && [ -x "$HOME/.opencode/bin/opencode" ]; then
  export OPENCODE_BIN="$HOME/.opencode/bin/opencode"
fi

# opencode gen_ai telemetry -> local edge agent (unchanged); the supervisor adds the liveness lane.
: "${OPENCODE_ENABLE_TELEMETRY:=1}"
: "${OPENCODE_OTLP_PROTOCOL:=http/protobuf}"
: "${OPENCODE_OTLP_ENDPOINT:=http://localhost:4318}"
: "${OPENCODE_RESOURCE_ATTRIBUTES:=service.name=opencode}"
export OPENCODE_ENABLE_TELEMETRY OPENCODE_OTLP_PROTOCOL OPENCODE_OTLP_ENDPOINT OPENCODE_RESOURCE_ATTRIBUTES

# Prompt on stdin (supervisor passes its stdin through to opencode, which reads it natively when no
# positional message arg is given — avoids ARG_MAX overflow on large bundles).
if [ "$SOURCE" = "-" ]; then
  exec "$PY" -m cc_host.shim --cli opencode --provider "$PROVIDER" --model "$MODEL" --self-attribute -- -m "$MODEL" "$@"
else
  [ -r "$SOURCE" ] || { echo "opencode-shim: cannot read $SOURCE" >&2; exit 66; }
  exec "$PY" -m cc_host.shim --cli opencode --provider "$PROVIDER" --model "$MODEL" --self-attribute -- -m "$MODEL" "$@" < "$SOURCE"
fi
