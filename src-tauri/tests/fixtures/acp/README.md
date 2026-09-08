# ACP fixtures

`agy-*.jsonl` contains actual client writes and server stdout frames in observation order,
recorded on macOS arm64 with authenticated agy on 2026-09-09. Requests are identified by
`method`; responses preserve their original request IDs. Stderr is not part of the transport.
No server frames or identifiers were synthesized or replaced.

Adapter: [shubzkothekar/antigravity-acp v1.1.0](https://github.com/shubzkothekar/antigravity-acp/releases/tag/v1.1.0),
asset `agy-acp-darwin-arm64`, SHA-256:

```text
9ef7afa432341c05d6c049d143349ea71fbb48989813ba625054a7224e2804fc
```

The adapter was launched with `AGY_BIN=/opt/homebrew/bin/agy`. Without that override,
the adapter attempted to download agy v1.0.13 from a URL returning HTTP 404.
The Rust dependency is exactly `agent-client-protocol` 2.0.0 with `unstable`, resolving
schema 1.5.0. The real adapter negotiated protocol version 1, not crate version 2.

| Files | Observed behavior |
| --- | --- |
| `agy-handshake`, `agy-prompt` | Initialize, durable session/new, search/read tools, text, end_turn, idle cancel |
| `agy-permission-handshake`, `agy-permission` | Requested an edit to a disposable sample file; read/edit tools and text completed without a protocol permission request |
| `agy-shell-handshake`, `agy-shell` | Requested approval for a harmless printf command; only configuration updates arrived during the 40-second capture window, then cancellation was sent |
| `agy-cancel-handshake`, `agy-cancel` | Cancelled a pending prompt after 100 ms; server returned stopReason=cancelled |

All prompts used `/tmp/panes-acp-capture-workspace`, containing only a disposable
`sample.txt`. Authentication worked. **Real `session/request_permission` coverage is
still missing:** neither the edit attempt nor the shell attempt yielded that request.
Permission request/response coverage comes from the deterministic fake subprocess,
validated through the pinned Rust schema. Real thinking and usage frames were also
not observed; they are covered by the fake subprocess.

`fake_server.py` is deliberately synthetic. It covers bidirectional request-ID collisions
during initialization, fragmented NDJSON, session new/load, text/thinking chunks,
replacement tool snapshots, permissions, cancellation, duplicate responses, and failures.
It requires Python 3; tests resolve `python3` with `python` as a fallback.

Run the deterministic suite:

```sh
cd src-tauri
cargo test engines::acp:: --lib
```

The optional live smoke test exercises AcpEngine itself with an authenticated server:

```sh
AGY_BIN=/path/to/agy PANES_ACP_SMOKE_COMMAND=/path/to/verified/agy-acp \
  cargo test acp_real_server_smoke --lib -- --ignored
```

## Substrate boundary

Only `engines::acp` is added to the module list. No EngineHandle variant, product
profile, engine allowlist, sandbox policy, or frontend registration changes here.
The substrate accepts text with the server's default model. Explicit model selection,
attachments, plan mode, and panes sandbox/approval policy settings fail explicitly;
product-specific selection and capability wiring belong to the following PRs.

A healthy process stays alive between turns so start_thread can reuse its session.
Cancellation, timeout, malformed transport, process loss, and archive terminate and
reap the runtime; a later start_thread uses session/load if supported. Unix launches
use a separate process group so descendants are terminated during cleanup.
Protocol writes and event delivery have bounded waits; a disconnected or persistently
backpressured consumer fails the turn. A closed consumer cannot receive its terminal
event, but the dispatcher still completes cleanup and releases all turn senders.
