# Antigravity chat

Panes drives the installed `agy` CLI through the third-party
[shubzkothekar/antigravity-acp](https://github.com/shubzkothekar/antigravity-acp/tree/v1.1.0)
adapter. This is not a first-party Google ACP integration.

The default adapter is **v1.1.0**, asset `agy-acp-darwin-arm64`, SHA-256:

```
9ef7afa432341c05d6c049d143349ea71fbb48989813ba625054a7224e2804fc
```

Install the official agy CLI and run `agy` to sign in. In Panes onboarding,
select Antigravity and copy the adapter installation command. It downloads the
pinned Apple Silicon macOS asset, verifies its checksum, and installs it at
`~/.local/bin/agy-acp`. Recheck readiness, then select Antigravity and a model
in the chat model picker.

Panes resolves `agy-acp` from PATH or `~/.local/bin/agy-acp` and verifies the
checksum before launching it. Other platforms require an explicit compatible
adapter override. Set `PANES_AGY_ACP_COMMAND` to an executable path or command
name, or set the provider binary path in Settings. Explicit overrides are
user-managed and bypass the default checksum pin. Arguments belong in provider
launch arguments; the command override is not interpreted by a shell.

The adapter uses `AGY_BIN` or PATH to locate the CLI; provider environment values
are forwarded. Automatic CLI download is disabled by default. Account setup runs
`agy`, not the adapter. Provider home-directory overrides are unsupported.

Models use the slugs reported by `agy models` and are selected through
`session/set_config_option` with `configId: "model"`. The fallback catalog was
verified against the installed CLI on 2026-09-09 and the recorded v1.1.0 fixtures.
Gemini 3.8 Flash (High) is the default.

Approvals use the original ACP permission options. This profile has no Panes
sandbox, global approval policy, steering, attachments, or diff support.
Readiness verifies ACP initialization; model authentication is exercised on the
first text turn.

For a live smoke test with installed agy and the adapter:

```sh
cd src-tauri
cargo test acp_agy_real_profile_smoke --lib -- --ignored --nocapture
```
