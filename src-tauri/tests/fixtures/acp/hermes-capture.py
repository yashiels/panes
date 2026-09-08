import argparse
import asyncio
import json
import os
import tempfile
import threading
import time
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

root = Path(__file__).resolve().parent
hermes_command = os.environ.get("HERMES_CAPTURE_COMMAND", "hermes")


class Backend(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(
            json.dumps(
                {"object": "list", "data": [{"id": "fixture-model", "object": "model"}]}
            ).encode()
        )

    def do_POST(self):
        body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        messages = body.get("messages", [])
        prompt = str([m for m in messages if m.get("role") == "user"][-1:])
        tool = any(m.get("role") == "tool" for m in messages)
        if "cancel" in prompt:
            time.sleep(3)
        msg = {"role": "assistant", "content": "Hermes ACP fixture complete."}
        finish = "stop"
        if "permission" in prompt and not tool:
            msg = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "fixture-write-1",
                        "type": "function",
                        "function": {
                            "name": "write_file",
                            "arguments": json.dumps(
                                {"path": "sample.txt", "content": "Fixture edit.\n"}
                            ),
                        },
                    }
                ],
            }
            finish = "tool_calls"
        result = {
            "id": "fixture-completion",
            "object": "chat.completion",
            "created": 1788912000,
            "model": "fixture-model",
            "choices": [{"index": 0, "message": msg, "finish_reason": finish}],
            "usage": {"prompt_tokens": 10, "completion_tokens": 5, "total_tokens": 15},
        }
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        delta = dict(msg)
        if "tool_calls" in delta:
            delta["tool_calls"][0]["index"] = 0
        chunk = {
            "id": "fixture-completion",
            "object": "chat.completion.chunk",
            "created": 1788912000,
            "model": "fixture-model",
            "choices": [{"index": 0, "delta": delta, "finish_reason": None}],
        }
        self.wfile.write(("data: " + json.dumps(chunk) + "\n\n").encode())
        chunk["choices"] = [{"index": 0, "delta": {}, "finish_reason": finish}]
        chunk["usage"] = result["usage"]
        self.wfile.write(
            ("data: " + json.dumps(chunk) + "\n\ndata: [DONE]\n\n").encode()
        )


def configure_home(home):
    (Path(home) / "config.yaml").write_text(
        f"model:\n  provider: custom\n  default: fixture-model\n  base_url: http://127.0.0.1:{server.server_port}/v1\n  api_mode: chat_completions\n  context_length: 131072\nproviders: {{}}\nagent:\n  max_turns: 3\nmemory:\n  memory_enabled: false\n  user_profile_enabled: false\n"
    )


async def capture(name):
    with tempfile.TemporaryDirectory(prefix="panes-hermes-") as home:
        workspace = Path(home) / "workspace"
        workspace.mkdir()
        (workspace / "sample.txt").write_text("Original.\n")
        configure_home(home)
        env = {
            k: v
            for k, v in os.environ.items()
            if not any(
                s in k
                for s in ["KEY", "TOKEN", "SECRET", "HERMES", "OPENAI", "ANTHROPIC"]
            )
        }
        env.update(
            HERMES_HOME=home,
            HERMES_ACP_SKIP_CONFIGURED_MCP="1",
            CUSTOM_API_KEY="fixture-only",
            PYTHONUNBUFFERED="1",
        )
        stderr = open("/tmp/panes-hermes-" + name + ".stderr", "w")
        p = await asyncio.create_subprocess_exec(
            hermes_command,
            "acp",
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=stderr,
        )
        frames = []

        async def send(v):
            frames.append(v)
            p.stdin.write((json.dumps(v) + "\n").encode())
            await p.stdin.drain()

        async def read_until(rid):
            while True:
                line = await asyncio.wait_for(p.stdout.readline(), 50)
                if not line:
                    raise RuntimeError("EOF")
                v = json.loads(line)
                frames.append(v)
                if v.get("method") == "session/request_permission":
                    opts = v["params"]["options"]
                    opt = next(o for o in opts if o["kind"] == "allow_once")
                    await send(
                        {
                            "jsonrpc": "2.0",
                            "id": v["id"],
                            "result": {
                                "outcome": {
                                    "outcome": "selected",
                                    "optionId": opt["optionId"],
                                }
                            },
                        }
                    )
                if v.get("id") == rid and "method" not in v:
                    return v

        try:
            await send(
                {
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "initialize",
                    "params": {
                        "protocolVersion": 1,
                        "clientCapabilities": {},
                        "clientInfo": {"name": "panes-fixture-capture", "version": "1"},
                    },
                }
            )
            await read_until(1)
            await send(
                {
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "session/new",
                    "params": {"cwd": str(workspace), "mcpServers": []},
                }
            )
            v = await read_until(2)
            sid = v["result"]["sessionId"]
            if name == "text":
                await send(
                    {
                        "jsonrpc": "2.0",
                        "id": 4,
                        "method": "session/set_model",
                        "params": {
                            "sessionId": sid,
                            "modelId": v["result"]["models"]["currentModelId"],
                        },
                    }
                )
                selected = await read_until(4)
                assert "result" in selected, selected
            await send(
                {
                    "jsonrpc": "2.0",
                    "id": 3,
                    "method": "session/prompt",
                    "params": {
                        "sessionId": sid,
                        "prompt": [{"type": "text", "text": name + " fixture"}],
                    },
                }
            )
            if name == "cancel":
                await asyncio.sleep(1)
                await send(
                    {
                        "jsonrpc": "2.0",
                        "method": "session/cancel",
                        "params": {"sessionId": sid},
                    }
                )
            print(name, await read_until(3))
        finally:
            (root / ("hermes-" + name + ".jsonl")).write_text(
                "".join(json.dumps(v, separators=(",", ":")) + "\n" for v in frames)
            )
            p.terminate()
            await p.wait()
            stderr.close()


server = ThreadingHTTPServer(("127.0.0.1", 0), Backend)
threading.Thread(target=server.serve_forever, daemon=True).start()


async def main():
    for name in ["text", "permission", "cancel"]:
        await capture(name)


parser = argparse.ArgumentParser()
parser.add_argument("--serve", action="store_true")
args = parser.parse_args()
try:
    if args.serve:
        with tempfile.TemporaryDirectory(prefix="panes-hermes-smoke-") as home:
            configure_home(home)
            print(
                json.dumps(
                    {
                        "HERMES_HOME": home,
                        "CUSTOM_API_KEY": "fixture-only",
                        "HERMES_ACP_SKIP_CONFIGURED_MCP": "1",
                        "PANES_HERMES_SMOKE_COMMAND": hermes_command,
                    }
                ),
                flush=True,
            )
            threading.Event().wait()
    else:
        asyncio.run(main())
finally:
    server.shutdown()
