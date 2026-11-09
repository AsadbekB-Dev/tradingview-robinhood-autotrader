from __future__ import annotations

import argparse
import logging
from logging.handlers import RotatingFileHandler
import os
import secrets
import sys
import threading
import time
from pathlib import Path
from typing import Literal

import uvicorn
from fastapi import FastAPI, Header, HTTPException, Query, Request
from pydantic import BaseModel, Field

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config
from app.robinhood import PersistentRobinhoodWorker

TRADINGVIEW_WEBHOOK_IPS = {
    "52.89.214.238",
    "34.212.75.30",
    "54.218.53.128",
    "52.32.178.7",
}


class LiveFillReport(BaseModel):
    strategy: str = Field(default="MAIN", min_length=1, max_length=32)
    kind: Literal["entry", "exit"]
    fill_price: float = Field(gt=0)
    order_id: str | None = None
    filled_at: str | None = None
    qty: int | None = Field(default=None, ge=1)


class LiveOrderRegistration(BaseModel):
    strategy: str = Field(default="MAIN", min_length=1, max_length=32)
    kind: Literal["entry", "exit"]
    order_id: str = Field(min_length=1)


class ClearIntentRequest(BaseModel):
    strategy: str = Field(default="MAIN", min_length=1, max_length=32)
    kind: Literal["entry", "exit"]
    reason: str = Field(default="external_clear", min_length=1, max_length=200)


def _load_or_create_webhook_secret(cfg: dict) -> str:
    configured = str(cfg.get("server", {}).get("webhook_secret", "")).strip()
    if configured and configured != "change-me":
        return configured

    state_dir = Path(cfg.get("persistent_worker", {}).get("state_dir", "state"))
    state_dir.mkdir(parents=True, exist_ok=True)
    secret_path = state_dir / "webhook_secret.txt"
    if secret_path.exists():
        saved = secret_path.read_text(encoding="utf-8").strip()
        if saved:
            return saved

    generated = secrets.token_urlsafe(32)
    secret_path.write_text(generated + "\n", encoding="utf-8")
    try:
        secret_path.chmod(0o600)
    except Exception:
        pass
    return generated


def build_app(worker: PersistentRobinhoodWorker, cfg: dict, webhook_secret: str) -> FastAPI:
    app = FastAPI(
        title="Asads AUTOMATION Robinhood MCP Worker",
        version="0.7.8",
        docs_url=None,
        redoc_url=None,
        openapi_url=None,
    )
    max_webhook_body = int(cfg.get("server", {}).get("webhook_max_body_bytes", 8192))
    allow_legacy_callbacks = bool(cfg.get("server", {}).get("allow_legacy_execution_callbacks", False))

    def require_local(request: Request) -> None:
        # cloudflared injects this header, so tunneled callers cannot touch local
        # status, intent, journal, or fill-report endpoints.
        if request.headers.get("cf-connecting-ip"):
            raise HTTPException(status_code=403, detail="local endpoint")
        host = request.client.host if request.client else ""
        if host not in {"127.0.0.1", "::1", "localhost"}:
            raise HTTPException(status_code=403, detail="local endpoint")

    @app.get("/health")
    def health(request: Request):
        require_local(request)
        state = worker.status()
        return {
            "ok": bool(state.get("ready")),
            "mode": "robinhood_mcp_real_data",
            "mcp_connected": state.get("mcp_connected"),
            "agentic_account_last4": state.get("agentic_account_last4"),
            "share_price": state.get("share_price"),
            "buying_power": state.get("buying_power"),
            "current_expiration": state.get("current_expiration"),
            "zero_dte_ready": state.get("zero_dte_ready"),
            "last_error": state.get("last_error"),
        }

    @app.get("/status")
    def status(request: Request):
        require_local(request)
        return worker.status()

    @app.post("/control/shutdown", status_code=202)
    def control_shutdown(request: Request):
        require_local(request)
        return worker.request_shutdown("desktop dashboard requested shutdown")

    @app.get("/execution/intent")
    def execution_intent(request: Request):
        require_local(request)
        return {"intents": worker.current_order_intents()}

    @app.post("/execution/order-registered")
    def order_registered(report: LiveOrderRegistration, request: Request):
        """Register a real Robinhood order ID (legacy/backward-compatible path).

        The worker then uses read-only get_option_orders polling to detect the
        actual terminal state/fill and journal it. This endpoint never places an order.
        """
        require_local(request)
        if not allow_legacy_callbacks:
            raise HTTPException(status_code=410, detail="legacy execution callbacks are disabled")
        tracked = worker.register_live_order(kind=report.kind, order_id=report.order_id, strategy=report.strategy)
        return {"ok": True, "registered_order": tracked}

    @app.post("/execution/clear-intent")
    def clear_intent(report: ClearIntentRequest, request: Request):
        require_local(request)
        if not allow_legacy_callbacks:
            raise HTTPException(status_code=410, detail="legacy execution callbacks are disabled")
        return worker.clear_pending_intent(kind=report.kind, reason=report.reason, strategy=report.strategy)

    @app.post("/execution/live-fill")
    def live_fill(report: LiveFillReport, request: Request):
        """Legacy/local callback for a broker-confirmed fill.

        Normal v0.7 full-auto operation discovers fills by polling Robinhood.
        This compatibility endpoint never places an order and never creates a
        synthetic fill.
        """
        require_local(request)
        if not allow_legacy_callbacks:
            raise HTTPException(status_code=410, detail="legacy execution callbacks are disabled")
        if report.kind == "entry":
            position = worker.record_live_entry_fill(
                fill_price=report.fill_price,
                strategy=report.strategy,
                order_id=report.order_id,
                filled_at=report.filled_at,
                qty=report.qty,
            )
            return {"ok": True, "tracked_position": position}
        trade = worker.record_live_exit_fill(
            fill_price=report.fill_price,
            strategy=report.strategy,
            order_id=report.order_id,
            filled_at=report.filled_at,
            qty=report.qty,
        )
        return {"ok": True, "trade": trade}

    @app.get("/journal/stats")
    def journal_stats(request: Request):
        require_local(request)
        return worker.trade_journal.stats()

    @app.get("/journal/trades")
    def journal_trades(request: Request, limit: int = Query(default=50, ge=1, le=500)):
        require_local(request)
        return {"trades": worker.trade_journal.recent_trades(limit)}

    def authorize(request: Request, header_secret: str | None, query_secret: str | None) -> None:
        supplied = header_secret or query_secret or ""
        if not supplied or not secrets.compare_digest(str(supplied), webhook_secret):
            raise HTTPException(status_code=401, detail="bad webhook secret")

        cf_ip = (request.headers.get("cf-connecting-ip") or "").strip()
        enforce = bool(cfg.get("server", {}).get("enforce_tradingview_ip_allowlist", True))
        if enforce:
            if cf_ip and cf_ip not in TRADINGVIEW_WEBHOOK_IPS:
                raise HTTPException(status_code=403, detail="untrusted webhook source")
            # Direct local calls are useful for diagnostics, but if the server is
            # ever rebound by mistake, do not let remote callers bypass the
            # TradingView allowlist merely by omitting Cloudflare's header.
            host = request.client.host if request.client else ""
            if not cf_ip and host not in {"127.0.0.1", "::1", "localhost"}:
                raise HTTPException(status_code=403, detail="untrusted webhook source")

    @app.post("/webhook/tradingview", status_code=202)
    async def tradingview_webhook(
        request: Request,
        x_asads_secret: str | None = Header(default=None),
        secret: str | None = Query(default=None),
    ):
        authorize(request, x_asads_secret, secret)
        raw_body = await request.body()
        if len(raw_body) > max_webhook_body:
            raise HTTPException(status_code=413, detail="webhook body too large")
        body = raw_body.decode("utf-8", errors="replace").strip()
        if not body:
            raise HTTPException(status_code=400, detail="empty body")

        # TradingView abandons slow webhooks, so acknowledge immediately and let
        # the persistent MCP worker do the market-data/review work asynchronously.
        worker.note_webhook_receipt(source_ip=(request.headers.get("cf-connecting-ip") or None))
        if not worker.enqueue_signal(body):
            raise HTTPException(status_code=503, detail="signal queue full; alert was not accepted")
        return {"ok": True, "queued": True}

    return app


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()

    os.environ["ASADS_CONFIG"] = args.config
    cfg = load_config()
    log_dir = Path(cfg.get("persistent_worker", {}).get("state_dir", "state")) / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_format = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    root_logger.handlers.clear()
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    file_handler = RotatingFileHandler(
        log_dir / "worker.log", maxBytes=2_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(log_format)
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
    # The MCP SDK currently probes a GET stream Robinhood answers with 405 and
    # may attempt a DELETE session close Robinhood answers with 400. POST tool
    # traffic remains healthy; keep these SDK transport details out of the normal
    # trading console unless they become real errors.
    logging.getLogger("httpx2").setLevel(logging.WARNING)
    logging.getLogger("mcp.client.streamable_http").setLevel(logging.ERROR)

    worker = PersistentRobinhoodWorker(cfg)
    worker.start()
    if not worker.wait_until_ready(timeout=float(cfg.get("robinhood_mcp", {}).get("startup_timeout_seconds", 300))):
        state = worker.status()
        worker.stop()
        raise RuntimeError(f"Robinhood MCP worker did not become ready: {state.get('last_error')}")

    host = str(cfg.get("server", {}).get("host", "127.0.0.1"))
    if host not in {"127.0.0.1", "::1", "localhost"}:
        worker.stop()
        raise RuntimeError("For safety, the BOT webhook server must bind to localhost only.")
    port = int(cfg.get("server", {}).get("port", 8787))
    webhook_secret = _load_or_create_webhook_secret(cfg)

    print()
    print("Asads AUTOMATION v0.7.8 HARDENED worker is running.")
    print(f"Agentic account: ****{worker.status().get('agentic_account_last4')}")
    print(f"Local status: http://{host}:{port}/status")
    print(f"Current order intent: http://{host}:{port}/execution/intent")
    print(f"TradingView receiver: http://{host}:{port}/webhook/tradingview")
    print(f"Trade stats: http://{host}:{port}/journal/stats")
    print(f"Trade journal files: {worker.trade_journal.root.resolve()}")
    print("No Selenium, paper execution, virtual balance, or synthetic fills exist in this build.")
    print("MAIN + SCALPER route through one webhook and independently own live option lots/orders.")
    print("ENTRY/EXIT use live Robinhood MCP data, review, submit, and monitor real option orders.")
    print(f"Automatic live order submission: {'ON' if worker.status().get('auto_submit_live_orders') else 'OFF'}")
    print("Legacy STOP TRADING QQQ alerts are ignored; BOT idles safely after hours and keeps the webhook alive.")
    print("CTRL+C remains available as an emergency/manual stop.")
    print()
    print("TradingView webhook auth is ON.")
    print("Webhook path: /webhook/tradingview?secret=<stored locally>")
    print(r"Secret file: state\webhook_secret.txt")
    print("Keep the webhook secret and state/robinhood_mcp_oauth.json private.")
    print()

    app = build_app(worker, cfg, webhook_secret)
    config = uvicorn.Config(app, host=host, port=port, log_level="info", access_log=False, lifespan="off")
    server = uvicorn.Server(config)

    def watch_worker_shutdown() -> None:
        while True:
            if worker.shutdown_requested_event.wait(timeout=0.10):
                state = worker.status()
                print()
                print(f"Closing worker + webhook server: {state.get('shutdown_reason')}")
                server.should_exit = True
                return
            if worker.failed_event.is_set():
                server.should_exit = True
                return
            if worker.thread is not None and not worker.thread.is_alive():
                server.should_exit = True
                return
            time.sleep(0.02)

    threading.Thread(target=watch_worker_shutdown, name="AsadsShutdownWatcher", daemon=True).start()

    try:
        server.run()
    except KeyboardInterrupt:
        # Normal CTRL+C shutdown on Python 3.13 should not dump a traceback.
        pass
    finally:
        worker.stop()


if __name__ == "__main__":
    main()
