# Hermes ACP captures

Captured on macOS arm64 on 2026-09-09 using the actual `hermes acp` process from
NousResearch/hermes-agent commit `5505042f40b850979704c24037238f46cff6c8bf`
(Hermes 0.21.0), Python 3.11.16, and Python agent-client-protocol 0.9.0.
The server negotiated ACP protocol version 1. Client writes and server stdout
frames appear in observation order, preserving every identifier and response.

The language model was a deterministic localhost OpenAI-compatible streaming
HTTP server supplied by `hermes-capture.py`. Hermes itself, session handling,
agent execution, tool dispatch, approval bridge, and protocol serialization were
real. No external provider authentication or model quality is verified by these
captures. No user credentials or Hermes configuration were used or changed;
each subprocess received an isolated temporary `HERMES_HOME` and workspace.

| Fixture | Observed behavior |
| --- | --- |
| `hermes-text.jsonl` | Initialize, new session, text prompt, text chunk, usage, end_turn |
| `hermes-permission.jsonl` | Real write_file invocation, diff approval request, allow_once response, text, end_turn |
| `hermes-cancel.jsonl` | Cancel during a pending prompt, stopReason=cancelled |

The permission fixture approves an edit to a disposable `sample.txt`. The
permission request contains actual `type=diff` content. Hermes advertises model
choices in `session/new` under `models`; IDs encode provider and model, such as
`custom:fixture-model`. Selection uses `session/set_model` with `modelId`.

Reproduce with an existing Hermes installation, or install the cloned source
into a dedicated external virtual environment using `uv pip install -e
'/path/to/hermes-agent[acp]'`. Upstream prohibits wheel installs.

```sh
HERMES_CAPTURE_COMMAND=/path/to/venv/bin/hermes python3 \
  src-tauri/tests/fixtures/acp/hermes-capture.py
```

The harness never edits the source clone. It starts its own localhost endpoint,
uses a non-secret fixture key, and removes temporary Hermes homes on completion.

For an AcpEngine live smoke against the same deterministic model server, run the
harness with `--serve`. It prints environment values for a temporary isolated
Hermes home and keeps the localhost backend alive until interrupted. Pass those
values to the ignored Rust smoke test. The server returns `Hermes ACP fixture
complete.` for a text prompt; a prompt containing `permission` elicits the edit
approval, and `cancel` delays the response for cancellation testing.

The text fixture also captures a successful real `session/set_model` request
using the exact `currentModelId` returned by the session.
