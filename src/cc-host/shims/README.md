# Supervised CLI shims

From the repository root, with your CLI installed and configured:

```bash
src/cc-host/shims/codex-shim.sh prompt.txt
src/cc-host/shims/opencode-shim.sh provider/model prompt.txt
```

Use `-` instead of a file to pass a prompt on stdin. Background variants use
`nohup` or `setsid` and place task output under temporary task directories.
They run actual model CLIs and may incur provider costs. No live model run
is necessary for the synthetic demonstration or tests.

Qwen requires an OpenAI-compatible server configured by
`QWEN_OPENAI_BASE_URL`, `QWEN_OPENAI_API_KEY`, and `QWEN_MODEL`:

```bash
export QWEN_OPENAI_BASE_URL=http://127.0.0.1:8080/v1
export QWEN_MODEL=your-model-id
# Supply your own key through your local environment.
src/cc-host/shims/qwen-shim.sh prompt.txt
```

No provider credential is bundled. `CC_HOST_PYTHON` selects an interpreter;
`CC_HOST_DIR` can override the default relative package location. Keep
`CC_HOST_ALLOW_UNRESTRICTED=0` unless you explicitly choose unrestricted CLI
execution. The Qwen test uses a dummy key and a fake executable.

Qwen credentials are translated from `QWEN_OPENAI_API_KEY` to `OPENAI_API_KEY`
only in the child environment, following [Qwen's documented authentication](https://qwenlm.github.io/qwen-code-docs/en/users/configuration/auth/).
The supervisor rejects explicit `--openai-api-key` arguments. Environment-based
credentials can still be inspected by sufficiently privileged local processes;
this is not encrypted credential storage. Live CLI compatibility remains untested.

Background capture uses `umask 077` and a unique `mktemp` directory under
`TMPDIR` (or `/tmp`). Prompts, arguments and logs remain sensitive and are retained
there until you deliberately remove them. They are not automatically deleted.
Codex config is optional; `CC_HOST_CODEX_CONFIG_FILE` can select an alternate
lookup path. A missing config falls back to the default model or explicit `-m`.
