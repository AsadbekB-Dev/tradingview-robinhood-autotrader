from __future__ import annotations

import asyncio
import json
import logging
import queue
import threading
import webbrowser
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import parse_qs, urlparse

log = logging.getLogger("asads.robinhood.mcp")


class ToolClient(Protocol):
    """Small sync interface used by the trading worker.

    Production uses :class:`RobinhoodMCPClient`; tests can inject a stub without
    importing or contacting the MCP SDK.
    """

    def start(self, timeout: float = 180.0) -> None: ...
    def stop(self) -> None: ...
    def call_tool(self, name: str, arguments: dict[str, Any] | None = None, timeout: float = 30.0) -> dict[str, Any]: ...
    def tool_names(self) -> list[str]: ...
    def tool_schema(self, name: str) -> dict[str, Any] | None: ...


@dataclass
class _Call:
    name: str
    arguments: dict[str, Any]
    done: threading.Event = field(default_factory=threading.Event)
    result: dict[str, Any] | None = None
    error: BaseException | None = None


class _LoopbackOAuthCallback:
    """One-use loopback callback listener for the standard MCP OAuth flow."""

    def __init__(self) -> None:
        self._result_queue: queue.Queue[str] = queue.Queue(maxsize=1)
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):  # noqa: N802 - stdlib callback name
                try:
                    owner._result_queue.put_nowait(self.path)
                except queue.Full:
                    pass
                body = (
                    b"Robinhood MCP authorization received. You can close this tab "
                    b"and return to the Asads trader window."
                )
                self.send_response(200)
                self.send_header("Content-Type", "text/plain; charset=utf-8")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, fmt, *args):  # noqa: A003
                return

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = int(self.server.server_address[1])
        self.redirect_uri = f"http://127.0.0.1:{self.port}/callback"
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        try:
            self.server.shutdown()
            self.server.server_close()
        except Exception:
            pass

    async def redirect_handler(self, auth_url: str) -> None:
        self.start()
        print("\nRobinhood authorization is required once for this Python MCP client.")
        print("Opening your browser now. If it does not open, use this URL:\n")
        print(auth_url)
        print()
        webbrowser.open(auth_url, new=1, autoraise=True)

    async def callback_handler(self):
        from mcp.client.auth import AuthorizationCodeResult

        path = await asyncio.wait_for(asyncio.to_thread(self._result_queue.get), timeout=300.0)
        params = parse_qs(urlparse(path).query)
        if "error" in params:
            raise RuntimeError(f"Robinhood OAuth error: {params['error'][0]}")
        code = params.get("code", [None])[0]
        if not code:
            raise RuntimeError("Robinhood OAuth callback did not contain an authorization code")
        return AuthorizationCodeResult(
            code=code,
            state=params.get("state", [None])[0],
            iss=params.get("iss", [None])[0],
        )


class _JsonTokenStorage:
    """Persistent MCP OAuth tokens/client registration.

    The file contains bearer/refresh credentials. It is intentionally kept under
    the local state directory and should never be uploaded or committed.
    """

    def __init__(self, path: Path, OAuthToken, OAuthClientInformationFull):
        self.path = path
        self.OAuthToken = OAuthToken
        self.OAuthClientInformationFull = OAuthClientInformationFull
        self._lock = threading.Lock()

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _write(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        tmp.replace(self.path)
        try:
            self.path.chmod(0o600)
        except Exception:
            pass

    @staticmethod
    def _dump_model(value: Any) -> dict[str, Any]:
        if hasattr(value, "model_dump"):
            return value.model_dump(mode="json", by_alias=True, exclude_none=True)
        if hasattr(value, "dict"):
            return value.dict(by_alias=True, exclude_none=True)
        raise TypeError(f"Unsupported OAuth model: {type(value)!r}")

    async def get_tokens(self):
        with self._lock:
            raw = self._read().get("tokens")
        if not raw:
            return None
        return self.OAuthToken.model_validate(raw)

    async def set_tokens(self, tokens) -> None:
        with self._lock:
            data = self._read()
            data["tokens"] = self._dump_model(tokens)
            self._write(data)

    async def get_client_info(self):
        with self._lock:
            raw = self._read().get("client_info")
        if not raw:
            return None
        return self.OAuthClientInformationFull.model_validate(raw)

    async def set_client_info(self, client_info) -> None:
        with self._lock:
            data = self._read()
            data["client_info"] = self._dump_model(client_info)
            self._write(data)


class RobinhoodMCPClient:
    """Persistent authenticated Streamable-HTTP client for Robinhood Trading MCP.

    The client exposes the server's dynamic tools and input schemas. The v0.7
    worker uses those schemas to keep live option placement aligned with the
    order that was immediately reviewed.
    """

    def __init__(
        self,
        *,
        url: str = "https://agent.robinhood.com/mcp/trading",
        state_dir: str | Path = "state",
        client_name: str = "Asads AUTOMATION",
    ) -> None:
        self.url = url
        self.state_dir = Path(state_dir)
        self.client_name = client_name
        self.oauth_path = self.state_dir / "robinhood_mcp_oauth.json"
        self._calls: queue.Queue[_Call | None] = queue.Queue()
        self._ready = threading.Event()
        self._failed = threading.Event()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._startup_error: BaseException | None = None
        self._tool_names: list[str] = []
        self._tool_schemas: dict[str, dict[str, Any]] = {}

    def start(self, timeout: float = 180.0) -> None:
        if self._thread and self._thread.is_alive():
            if self._ready.wait(timeout=timeout):
                return
        else:
            self._thread = threading.Thread(target=self._thread_main, name="RobinhoodMCP", daemon=True)
            self._thread.start()

        if not self._ready.wait(timeout=timeout):
            if self._startup_error:
                raise RuntimeError(f"Robinhood MCP startup failed: {self._startup_error}") from self._startup_error
            raise TimeoutError("Timed out waiting for Robinhood MCP connection/authentication")
        if self._failed.is_set():
            raise RuntimeError(f"Robinhood MCP startup failed: {self._startup_error}")

    def stop(self) -> None:
        self._stop.set()
        self._calls.put(None)
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=8.0)

    def tool_names(self) -> list[str]:
        return list(self._tool_names)

    def tool_schema(self, name: str) -> dict[str, Any] | None:
        schema = self._tool_schemas.get(str(name))
        return json.loads(json.dumps(schema)) if isinstance(schema, dict) else None

    def call_tool(
        self,
        name: str,
        arguments: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        if not self._ready.is_set():
            self.start()
        if self._failed.is_set():
            raise RuntimeError(f"Robinhood MCP is unavailable: {self._startup_error}")
        call = _Call(name=name, arguments=arguments or {})
        self._calls.put(call)
        if not call.done.wait(timeout=timeout):
            raise TimeoutError(f"Robinhood MCP tool timed out: {name}")
        if call.error:
            raise RuntimeError(f"Robinhood MCP tool failed ({name}): {call.error}") from call.error
        return call.result or {}

    def _thread_main(self) -> None:
        try:
            asyncio.run(self._async_main())
        except BaseException as exc:
            self._startup_error = exc
            self._failed.set()
            self._ready.set()
            log.exception("Robinhood MCP thread failed")

    async def _async_main(self) -> None:
        # Lazy imports keep unit tests runnable even before requirements are installed.
        import httpx2
        from pydantic import AnyUrl
        from mcp import ClientSession
        from mcp.client.auth import OAuthClientProvider
        from mcp.client.streamable_http import streamable_http_client
        from mcp.shared.auth import OAuthClientInformationFull, OAuthClientMetadata, OAuthToken

        callback = _LoopbackOAuthCallback()
        storage = _JsonTokenStorage(self.oauth_path, OAuthToken, OAuthClientInformationFull)
        metadata = OAuthClientMetadata(
            client_name=self.client_name,
            redirect_uris=[AnyUrl(callback.redirect_uri)],
            grant_types=["authorization_code", "refresh_token"],
            response_types=["code"],
        )
        oauth = OAuthClientProvider(
            server_url=self.url,
            client_metadata=metadata,
            storage=storage,
            redirect_handler=callback.redirect_handler,
            callback_handler=callback.callback_handler,
        )

        try:
            timeout = httpx2.Timeout(30.0, read=300.0)
            async with httpx2.AsyncClient(auth=oauth, follow_redirects=True, timeout=timeout) as http_client:
                async with streamable_http_client(self.url, http_client=http_client) as streams:
                    # SDK 1.x returned three values; 2.x returns two. Supporting
                    # either makes local upgrades less brittle.
                    read_stream, write_stream = streams[0], streams[1]
                    async with ClientSession(read_stream, write_stream) as session:
                        await session.initialize()
                        listed = await session.list_tools()
                        self._tool_names = [t.name for t in listed.tools]
                        self._tool_schemas = {}
                        for tool in listed.tools:
                            raw_schema = getattr(tool, "inputSchema", None)
                            if raw_schema is None:
                                raw_schema = getattr(tool, "input_schema", None)
                            if hasattr(raw_schema, "model_dump"):
                                raw_schema = raw_schema.model_dump(mode="json")
                            if isinstance(raw_schema, dict):
                                self._tool_schemas[str(tool.name)] = raw_schema
                        self._ready.set()
                        log.info("Robinhood MCP connected with %d tools", len(self._tool_names))

                        while not self._stop.is_set():
                            call = await asyncio.to_thread(self._calls.get)
                            if call is None:
                                break
                            try:
                                raw = await session.call_tool(call.name, arguments=call.arguments)
                                call.result = self._extract_result(raw)
                            except BaseException as exc:
                                call.error = exc
                            finally:
                                call.done.set()
        finally:
            callback.stop()

    @staticmethod
    def _extract_result(result: Any) -> dict[str, Any]:
        is_error = bool(getattr(result, "is_error", getattr(result, "isError", False)))
        structured = getattr(result, "structured_content", None)
        if structured is None:
            structured = getattr(result, "structuredContent", None)

        if is_error:
            pieces: list[str] = []
            for item in getattr(result, "content", []) or []:
                text = getattr(item, "text", None)
                if text:
                    pieces.append(str(text))
            raise RuntimeError("; ".join(pieces) or "MCP server returned an error")

        if isinstance(structured, dict):
            return structured
        if structured is not None:
            return {"result": structured}

        for item in getattr(result, "content", []) or []:
            text = getattr(item, "text", None)
            if not text:
                continue
            try:
                parsed = json.loads(text)
            except Exception:
                continue
            if isinstance(parsed, dict):
                return parsed
            return {"result": parsed}
        return {}
