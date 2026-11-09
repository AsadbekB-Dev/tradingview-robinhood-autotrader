from __future__ import annotations

import hashlib
from collections import deque
import json
import logging
import queue
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from app.contract_selector import ContractSelector
from app.models import OptionContract, PositionCandidate, TradeSignal
from app.risk import RiskGate
from app.signal_parser import is_non_actionable_tradingview_alert, parse_tradingview_alert
from app.trade_journal import TradeJournal
from app.windowing import strike_window
from app.robinhood.mcp_client import RobinhoodMCPClient, ToolClient

log = logging.getLogger("asads.robinhood.worker")
ET = ZoneInfo("America/New_York")


def _f(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _data(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    d = payload.get("data")
    return d if isinstance(d, dict) else payload


def _now_et() -> datetime:
    return datetime.now(ET)


def _deep_merge(base: dict[str, Any], override: dict[str, Any] | None) -> dict[str, Any]:
    out = json.loads(json.dumps(base or {}))
    for key, value in (override or {}).items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = _deep_merge(out[key], value)
        else:
            out[key] = value
    return out


@dataclass
class SignalJob:
    raw: str
    received_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc).replace(tzinfo=None))
    done: threading.Event = field(default_factory=threading.Event)
    result: dict[str, Any] | None = None
    error: str | None = None


class PersistentRobinhoodWorker:
    """Real-data Robinhood Trading MCP worker.

    Production flow:
      TradingView webhook -> parse -> live Robinhood account/QQQ data ->
      0DTE contract+quantity ranking -> Robinhood ``review_option_order`` ->
      Robinhood ``place_option_order`` -> live order polling -> fill journal.

    There is no Selenium, browser scraping, virtual balance, simulated fill, or
    paper execution in this worker. A trade journal is opened/closed only after a
    real Robinhood order fill is observed through ``get_option_orders``.
    """

    REQUIRED_TOOLS = {
        "get_accounts",
        "get_portfolio",
        "get_equity_quotes",
        "get_option_chains",
        "get_option_instruments",
        "get_option_quotes",
        "get_option_orders",
        "get_option_positions",
        "review_option_order",
        "place_option_order",
        "cancel_option_order",
    }

    def __init__(
        self,
        cfg: dict,
        *,
        tool_client: ToolClient | None = None,
        user_data_dir: str | None = None,  # ignored; kept only for painless v0.3 upgrades
        profile: str | None = None,        # ignored; kept only for painless v0.3 upgrades
    ) -> None:
        self.cfg = cfg
        self.base_worker_cfg = cfg.get("persistent_worker", {}) or {}
        self.base_execution_cfg = cfg.get("execution", {}) or {}
        self.base_selector_cfg = cfg.get("selector", {}) or {}
        self.mcp_cfg = cfg.get("robinhood_mcp", {}) or {}

        configured = cfg.get("strategies") if isinstance(cfg.get("strategies"), dict) else {}
        if configured:
            names = ["MAIN"] + [str(k).upper() for k in configured if str(k).upper() != "MAIN"]
            self.strategy_names = [
                name for name in names
                if bool((configured.get(name) or configured.get(name.lower()) or {}).get("enabled", True))
            ]
            if "MAIN" not in self.strategy_names:
                self.strategy_names.insert(0, "MAIN")
        else:
            # Legacy configs/tests remain a one-strategy worker.
            self.strategy_names = ["MAIN"]
        self.multi_strategy_enabled = len(self.strategy_names) > 1
        self.strategy_profiles: dict[str, dict[str, Any]] = {}
        for name in self.strategy_names:
            raw_profile = configured.get(name) or configured.get(name.lower()) or {}
            self.strategy_profiles[name] = raw_profile if isinstance(raw_profile, dict) else {}

        def profile_cfg(name: str, section: str, base: dict[str, Any]) -> dict[str, Any]:
            override = self.strategy_profiles.get(name, {}).get(section, {})
            return _deep_merge(base, override if isinstance(override, dict) else {})

        self.strategy_execution_cfg = {
            name: profile_cfg(name, "execution", self.base_execution_cfg) for name in self.strategy_names
        }
        self.strategy_worker_cfg = {
            name: profile_cfg(name, "persistent_worker", self.base_worker_cfg) for name in self.strategy_names
        }
        self.strategy_selector_cfg = {
            name: profile_cfg(name, "selector", self.base_selector_cfg) for name in self.strategy_names
        }
        self.strategy_selectors = {
            name: ContractSelector(self.strategy_selector_cfg[name]) for name in self.strategy_names
        }
        self.strategy_risks = {
            name: RiskGate(self.strategy_execution_cfg[name]) for name in self.strategy_names
        }

        self.active_strategy = "MAIN"
        self.worker_cfg = self.strategy_worker_cfg["MAIN"]
        self.selector = self.strategy_selectors["MAIN"]
        self.risk = self.strategy_risks["MAIN"]

        # State/journal are shared; each strategy gets separate intent/order files.
        self.state_dir = Path(self.base_worker_cfg.get("state_dir", "state"))
        self.state_dir.mkdir(parents=True, exist_ok=True)
        journal_root = cfg.get("journal", {}).get("directory", str(self.state_dir / "trade_journal"))
        self.trade_journal = TradeJournal(journal_root)
        self.intent_path = self.state_dir / "order_intent.json"
        self.registered_order_path = self.state_dir / "registered_order.json"

        self.client: ToolClient = tool_client or RobinhoodMCPClient(
            url=str(self.mcp_cfg.get("url", "https://agent.robinhood.com/mcp/trading")),
            state_dir=self.state_dir,
            client_name=str(self.mcp_cfg.get("client_name", "Asads Multi-Strategy Trader")),
        )

        self.account_number: str | None = None
        self.account_meta: dict[str, Any] | None = None
        self.chain_id: str | None = None
        self.expiration: date | None = None
        self.instruments: dict[str, list[dict[str, Any]]] = {"CALL": [], "PUT": []}
        self._instruments_loaded_for: date | None = None

        # These fields represent the currently-active logical strategy. v0.7
        # context-switches them around the existing hardened execution path.
        self.pending_entry_intent: dict[str, Any] | None = None
        self.pending_exit_intent: dict[str, Any] | None = None
        self.last_selected_position: dict[str, Any] | None = None
        self.tracked_position: dict[str, Any] | None = None
        self._position_opened_monotonic: float | None = None
        self._shutdown_after_exit = False
        self._shutdown_after_pending_order = False
        self._deferred_exit_signal: TradeSignal | None = None
        self._last_deferred_exit_retry = 0.0
        self.registered_order: dict[str, Any] | None = None
        self._last_order_poll = 0.0

        self.signal_dedupe_seconds = float(self.base_worker_cfg.get("signal_dedupe_seconds", 20.0))
        self.signal_queue_maxsize = max(8, int(self.base_worker_cfg.get("signal_queue_maxsize", 256)))
        self.jobs_per_loop = max(1, int(self.base_worker_cfg.get("jobs_per_loop", 1)))
        self.cancel_retry_seconds = max(0.25, float(self.base_worker_cfg.get("cancel_retry_seconds", 1.0)))
        self.broker_position_reconcile_seconds = max(1.0, float(self.base_worker_cfg.get("broker_position_reconcile_seconds", 5.0)))
        self._last_position_reconcile = 0.0
        self.auto_submit_live_orders = bool(self.base_execution_cfg.get("auto_submit_live_orders", False))
        self.max_total_open_positions = int(
            self.base_execution_cfg.get(
                "max_total_open_positions",
                self.base_execution_cfg.get("max_open_positions", max(1, len(self.strategy_names))),
            )
        )
        default_total_cap = float(self.base_execution_cfg.get("max_trade_budget_dollars", 200.0)) * max(1, len(self.strategy_names))
        self.max_total_committed_dollars = float(
            self.base_execution_cfg.get("max_total_committed_dollars", default_total_cap)
        )
        self.shared_quote_cache_seconds = float(self.base_worker_cfg.get("shared_quote_cache_seconds", 0.60))
        self._quote_cache: dict[str, tuple[float, float, list[OptionContract], float | None]] = {}

        self.recent_signal_path = self.state_dir / "recent_signal_hashes.json"
        saved_recent = self._load_json_file(self.recent_signal_path) or {}
        self._recent_signal_hashes: dict[str, float] = {}
        for key, value in saved_recent.items():
            try:
                self._recent_signal_hashes[str(key)] = float(value)
            except Exception:
                continue
        self._quote_latency_ms: deque[float] = deque(maxlen=100)
        self.selection_snapshots_path = self.state_dir / "selection_snapshots.jsonl"
        self.signal_audit_path = self.state_dir / "signal_audit.jsonl"
        self.learning_cfg = cfg.get("learning", {}) if isinstance(cfg.get("learning"), dict) else {}
        self.learning_events_path = self.state_dir / "learning_events.jsonl"

        self.account_refresh_seconds = float(self.base_worker_cfg.get("account_refresh_seconds", 5.0))
        self.console_status_seconds = float(self.base_worker_cfg.get("console_status_seconds", 10.0))
        self._last_account_refresh = 0.0
        self._last_status_log = 0.0
        self.underlying_stop_watchdog_seconds = float(self.base_worker_cfg.get("underlying_stop_watchdog_seconds", 1.0))
        self._last_underlying_stop_watchdog = 0.0

        # Active profile values are refreshed again by _activate_strategy().
        self.order_status_poll_seconds = 0.35
        self.entry_intent_ttl_seconds = 12.0
        self.live_entry_order_timeout_seconds = 12.0
        self.live_exit_order_timeout_seconds = 2.5
        self.max_exit_reprice_attempts = 2
        self.deferred_exit_retry_seconds = 0.50
        self.max_quote_age_seconds = 3.0
        self.require_quote_timestamp = True
        self.position_killswitch_minutes = 25.0

        self.jobs: queue.Queue[SignalJob] = queue.Queue(maxsize=self.signal_queue_maxsize)
        self.urgent_jobs: queue.Queue[SignalJob] = queue.Queue(maxsize=self.signal_queue_maxsize)
        self.stop_event = threading.Event()
        self.ready_event = threading.Event()
        self.failed_event = threading.Event()
        self.shutdown_requested_event = threading.Event()
        self.thread: threading.Thread | None = None
        self.lock = threading.Lock()

        self.strategy_runtime: dict[str, dict[str, Any]] = {}
        for name in self.strategy_names:
            self.strategy_runtime[name] = {
                "pending_entry_intent": None,
                "pending_exit_intent": None,
                "last_selected_position": None,
                "tracked_position": None,
                "position_opened_monotonic": None,
                "shutdown_after_exit": False,
                "shutdown_after_pending_order": False,
                "deferred_exit_signal": None,
                "last_deferred_exit_retry": 0.0,
                "registered_order": None,
                "last_order_poll": 0.0,
                "killswitch_armed": False,
                "killswitch_deadline": None,
                "last_killswitch": None,
                "last_signal": None,
                "last_decision": None,
            }

        self.state: dict[str, Any] = {
            "version_mode": "ROBINHOOD_MCP_REAL_DATA_MULTI_STRATEGY",
            "version": "0.7.8-hardened",
            "strategy_sources": list(self.strategy_names),
            "multi_strategy_enabled": self.multi_strategy_enabled,
            "auto_submit_live_orders": self.auto_submit_live_orders,
            "max_total_open_positions": self.max_total_open_positions,
            "max_total_committed_dollars": self.max_total_committed_dollars,
            "ready": False,
            "started_at": None,
            "mcp_connected": False,
            "mcp_tool_count": 0,
            "agentic_account_last4": None,
            "agentic_allowed": None,
            "option_level": None,
            "account_type": None,
            "buying_power": None,
            "portfolio_value": None,
            "share_price": None,
            "last_underlying_quote_bid": None,
            "last_underlying_quote_ask": None,
            "last_underlying_quote_updated_at": None,
            "last_underlying_quote_age_seconds": None,
            "current_expiration": None,
            "current_dte": None,
            "zero_dte_ready": False,
            "watched_window": {"CALL": [], "PUT": []},
            "last_quote_batch_ms": None,
            "last_quote_updated_at": None,
            "last_quote_age_seconds": None,
            "last_quote_fresh_count": None,
            "last_quote_stale_count": None,
            "last_quote_missing_timestamp_count": None,
            "quote_latency_avg_ms": None,
            "quote_latency_p95_ms": None,
            "last_selection_snapshot": None,
            "learning_shadow_enabled": bool(self.learning_cfg.get("enabled", True)),
            "learning_shadow_only": True,
            "last_signal": None,
            "last_decision": None,
            "last_error": None,
            "pending_entry_intent": None,
            "pending_exit_intent": None,
            "tracked_position": None,
            "tracked_positions": {},
            "open_positions_count": 0,
            "position_killswitch_minutes": self.position_killswitch_minutes,
            "killswitch_armed": False,
            "killswitch_deadline": None,
            "last_killswitch": None,
            "last_underlying_stop_watchdog": None,
            "shutdown_blocked": False,
            "shutdown_block_reason": None,
            "registered_order": None,
            "registered_orders": {},
            "last_order_status": None,
            "last_cancel_request": None,
            "manual_reconcile_required": False,
            "unmanaged_live_positions": [],
            "last_webhook_received_at": None,
            "last_webhook_source": None,
            "signal_queue_depth": 0,
            "urgent_signal_queue_depth": 0,
            "last_intent_expiry": None,
            "shutdown_requested": False,
            "shutdown_reason": None,
            "session_entry_open": True,
            "session_entry_cutoff_et": str(self.base_worker_cfg.get("fallback_entry_cutoff_et", "23:59")),
            "session_force_flat_et": str(self.base_worker_cfg.get("fallback_force_exit_et", "23:59")),
            "session_auto_shutdown_et": str(self.base_worker_cfg.get("auto_shutdown_when_flat_et", "23:59")),
            "session_phase": "STARTING",
            "last_entry_latency": None,
            "last_end_of_day_action": None,
            "stop_after_exit": False,
            "stop_after_pending_order": False,
            "strategies": {},
            "journal_open_trade": self.trade_journal.load_open_trade(),
            "journal_open_trades": self.trade_journal.load_open_trades(),
            "journal_stats": self.trade_journal.stats(),
        }
        self._activate_strategy("MAIN", save_current=False)
        self._sync_state()

    # ---------------- multi-strategy context ----------------

    def _strategy_paths(self, strategy: str) -> tuple[Path, Path]:
        strategy = str(strategy).upper()
        if strategy == "MAIN":
            return self.state_dir / "order_intent.json", self.state_dir / "registered_order.json"
        safe = "".join(ch for ch in strategy if ch.isalnum() or ch in "_-" ) or "STRATEGY"
        return (
            self.state_dir / f"order_intent_{safe}.json",
            self.state_dir / f"registered_order_{safe}.json",
        )

    def _save_active_strategy(self) -> None:
        runtimes = getattr(self, "strategy_runtime", None)
        strategy = getattr(self, "active_strategy", None)
        if not runtimes or strategy not in runtimes:
            return
        rt = runtimes[strategy]
        rt.update({
            "pending_entry_intent": self.pending_entry_intent,
            "pending_exit_intent": self.pending_exit_intent,
            "last_selected_position": self.last_selected_position,
            "tracked_position": self.tracked_position,
            "position_opened_monotonic": self._position_opened_monotonic,
            "shutdown_after_exit": self._shutdown_after_exit,
            "shutdown_after_pending_order": self._shutdown_after_pending_order,
            "deferred_exit_signal": self._deferred_exit_signal,
            "last_deferred_exit_retry": self._last_deferred_exit_retry,
            "registered_order": self.registered_order,
            "last_order_poll": self._last_order_poll,
        })

    def _activate_strategy(self, strategy: str, *, save_current: bool = True) -> None:
        strategy = str(strategy).upper()
        if strategy not in self.strategy_runtime:
            raise ValueError(f"Unknown/disabled strategy source: {strategy}")
        if save_current:
            self._save_active_strategy()
        self.active_strategy = strategy
        rt = self.strategy_runtime[strategy]
        self.pending_entry_intent = rt.get("pending_entry_intent")
        self.pending_exit_intent = rt.get("pending_exit_intent")
        self.last_selected_position = rt.get("last_selected_position")
        self.tracked_position = rt.get("tracked_position")
        self._position_opened_monotonic = rt.get("position_opened_monotonic")
        self._shutdown_after_exit = bool(rt.get("shutdown_after_exit"))
        self._shutdown_after_pending_order = bool(rt.get("shutdown_after_pending_order"))
        self._deferred_exit_signal = rt.get("deferred_exit_signal")
        self._last_deferred_exit_retry = float(rt.get("last_deferred_exit_retry") or 0.0)
        self.registered_order = rt.get("registered_order")
        self._last_order_poll = float(rt.get("last_order_poll") or 0.0)

        self.worker_cfg = self.strategy_worker_cfg[strategy]
        self.selector = self.strategy_selectors[strategy]
        self.risk = self.strategy_risks[strategy]
        execution_cfg = self.strategy_execution_cfg[strategy]
        self.order_status_poll_seconds = float(self.worker_cfg.get("order_status_poll_seconds", 0.35))
        self.entry_intent_ttl_seconds = float(self.worker_cfg.get("entry_intent_ttl_seconds", 12.0))
        self.live_entry_order_timeout_seconds = float(self.worker_cfg.get("live_entry_order_timeout_seconds", 12.0))
        self.live_exit_order_timeout_seconds = float(self.worker_cfg.get("live_exit_order_timeout_seconds", 2.5))
        self.max_exit_reprice_attempts = int(self.worker_cfg.get("max_exit_reprice_attempts", 2))
        self.deferred_exit_retry_seconds = float(self.worker_cfg.get("deferred_exit_retry_seconds", 0.50))
        self.position_killswitch_minutes = float(self.worker_cfg.get("position_killswitch_minutes", 25.0))
        self.max_quote_age_seconds = float(execution_cfg.get("max_quote_age_seconds", 3.0))
        self.require_quote_timestamp = bool(execution_cfg.get("require_quote_timestamp", True))
        self.intent_path, self.registered_order_path = self._strategy_paths(strategy)

    def _runtime(self, strategy: str | None = None) -> dict[str, Any]:
        return self.strategy_runtime[str(strategy or self.active_strategy).upper()]

    def _strategy_open_count(self) -> int:
        self._save_active_strategy()
        return sum(1 for rt in self.strategy_runtime.values() if rt.get("tracked_position"))

    def _pending_entry_count(self, *, exclude: str | None = None) -> int:
        self._save_active_strategy()
        ex = str(exclude).upper() if exclude else None
        return sum(
            1 for name, rt in self.strategy_runtime.items()
            if name != ex and rt.get("pending_entry_intent")
        )

    def _pending_entry_reserved_dollars(self, *, exclude: str | None = None) -> float:
        """Reserve local buying power for other strategy orders still pending.

        Robinhood normally reduces buying power as soon as an order is accepted,
        but this extra reservation closes the small race where the second webhook
        is processed before the portfolio endpoint reflects the first order.
        """
        self._save_active_strategy()
        ex = str(exclude).upper() if exclude else None
        total = 0.0
        for name, rt in self.strategy_runtime.items():
            if name == ex:
                continue
            intent = rt.get("pending_entry_intent")
            if not isinstance(intent, dict):
                continue
            payload = intent.get("mcp_order_payload") or {}
            try:
                qty = int(float(payload.get("quantity") or intent.get("selected_position", {}).get("qty") or 0))
                price = float(payload.get("price") or 0.0)
                total += max(0.0, qty * price * 100.0)
            except Exception:
                continue
        return total

    def _locally_committed_dollars(self, *, include_pending: bool = True) -> float:
        self._save_active_strategy()
        total = 0.0
        for rt in self.strategy_runtime.values():
            pos = rt.get("tracked_position")
            if isinstance(pos, dict):
                try:
                    total += float(pos.get("capital_deployed") or 0.0)
                except Exception:
                    pass
            if include_pending:
                intent = rt.get("pending_entry_intent")
                if isinstance(intent, dict):
                    try:
                        payload = intent.get("mcp_order_payload") or {}
                        qty = int(float(payload.get("quantity") or intent.get("selected_position", {}).get("qty") or 0))
                        price = float(payload.get("price") or 0.0)
                        total += max(0.0, qty * price * 100.0)
                    except Exception:
                        pass
        return total

    def current_order_intents(self) -> dict[str, dict[str, Any]]:
        self._save_active_strategy()
        out: dict[str, dict[str, Any]] = {}
        for name, rt in self.strategy_runtime.items():
            intent = rt.get("pending_exit_intent") or rt.get("pending_entry_intent")
            if intent:
                out[name] = self._public_intent(intent) or {}
        return out

    # ---------------- public API ----------------

    def start(self) -> None:
        if self.thread and self.thread.is_alive():
            return
        self.thread = threading.Thread(target=self._run, name="RobinhoodMCPWorker", daemon=True)
        self.thread.start()

    def wait_for_user_confirmation(self, timeout: float | None = None) -> bool:
        # MCP builds have no Selenium startup ENTER. Kept so older wrappers do not break.
        return self.wait_until_ready(timeout=timeout or 180.0)

    def wait_until_ready(self, timeout: float = 180.0) -> bool:
        end = time.time() + timeout
        while time.time() < end:
            if self.failed_event.is_set():
                return False
            if self.ready_event.is_set():
                return True
            time.sleep(0.1)
        return False

    def stop(self) -> None:
        self.stop_event.set()
        if self.thread and self.thread.is_alive():
            self.thread.join(timeout=8.0)
        try:
            self.client.stop()
        except Exception:
            pass

    def request_shutdown(self, reason: str = "manual dashboard stop") -> dict[str, Any]:
        return self._request_shutdown(reason)

    def status(self) -> dict[str, Any]:
        with self.lock:
            return json.loads(json.dumps(self.state, default=str))

    def current_order_intent(self, strategy: str = "MAIN") -> dict[str, Any] | None:
        name = str(strategy).upper()
        if name not in self.strategy_runtime:
            return None
        self._save_active_strategy()
        rt = self.strategy_runtime[name]
        intent = rt.get("pending_exit_intent") or rt.get("pending_entry_intent")
        return json.loads(json.dumps(intent, default=str)) if intent else None

    @staticmethod
    def _signal_is_urgent(raw: str) -> bool:
        u = " ".join(str(raw).upper().split())
        return "EXIT QQQ" in u or "STOP TRADING QQQ" in u

    def submit_signal(self, raw: str, timeout: float = 30.0) -> dict[str, Any]:
        job = SignalJob(raw=raw)
        target = self.urgent_jobs if self._signal_is_urgent(raw) else self.jobs
        try:
            target.put(job, timeout=min(max(timeout, 0.1), 2.0))
        except queue.Full as exc:
            raise RuntimeError("signal queue is full; refusing to silently drop TradingView alert") from exc
        if not job.done.wait(timeout=timeout):
            raise TimeoutError("TradingView signal processing timed out")
        if job.error:
            raise RuntimeError(job.error)
        return job.result or {}

    def enqueue_signal(self, raw: str) -> bool:
        target = self.urgent_jobs if self._signal_is_urgent(raw) else self.jobs
        try:
            target.put_nowait(SignalJob(raw=raw))
            self._set_state(signal_queue_depth=self.jobs.qsize(), urgent_signal_queue_depth=self.urgent_jobs.qsize())
            return True
        except queue.Full:
            self._set_state(last_error="TradingView signal queue is full; webhook was rejected instead of silently dropped")
            return False

    def note_webhook_receipt(self, *, source_ip: str | None = None) -> None:
        self._set_state(
            last_webhook_received_at=datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            last_webhook_source=source_ip or "direct/local",
        )

    def register_live_order(self, *, kind: str, order_id: str, strategy: str | None = None) -> dict[str, Any]:
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        kind = str(kind).upper().strip()
        if kind not in {"ENTRY", "EXIT"}:
            raise ValueError("kind must be ENTRY or EXIT")
        order_id = str(order_id).strip()
        if not order_id:
            raise ValueError("order_id is required")
        if kind == "ENTRY" and not self.pending_entry_intent:
            raise RuntimeError("No reviewed entry intent is pending")
        if kind == "EXIT" and not self.pending_exit_intent:
            raise RuntimeError("No reviewed exit intent is pending")
        self.registered_order = {
            "strategy": self.active_strategy,
            "kind": kind,
            "order_id": order_id,
            "registered_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        self._write_registered_order()
        self._sync_state()
        return json.loads(json.dumps(self.registered_order))

    def _request_cancel_registered_order(self, *, reason: str) -> bool:
        reg = self.registered_order
        if not reg or not self.account_number:
            return False
        if reg.get("cancel_accepted") is True:
            return True
        previous = reg.get("cancel_requested_at")
        if previous:
            try:
                prior = datetime.fromisoformat(str(previous).replace("Z", "+00:00"))
                now_dt = datetime.now(prior.tzinfo) if prior.tzinfo else datetime.now()
                if (now_dt - prior).total_seconds() < self.cancel_retry_seconds:
                    return False
            except Exception:
                pass

        requested_at = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        reg["cancel_requested_at"] = requested_at
        reg["cancel_reason"] = reason
        reg["cancel_attempts"] = int(reg.get("cancel_attempts") or 0) + 1
        try:
            self._write_registered_order()
        except Exception as exc:
            log.warning("Could not persist cancel-attempt state before broker request: %s", exc)
        try:
            response = self.client.call_tool(
                "cancel_option_order",
                {"account_number": self.account_number, "order_id": str(reg["order_id"])},
                timeout=15.0,
            )
            accepted = bool(_data(response).get("accepted", True))
            reg["cancel_accepted"] = accepted
            self._set_state(last_cancel_request={
                "order_id": reg.get("order_id"),
                "kind": reg.get("kind"),
                "reason": reason,
                "requested_at": requested_at,
                "accepted": accepted,
            })
            try:
                self._write_registered_order()
            except Exception as persist_exc:
                log.warning("Could not persist accepted cancel state: %s", persist_exc)
            self._sync_state()
            return accepted
        except Exception as exc:
            reg["cancel_error"] = str(exc)
            try:
                self._write_registered_order()
            except Exception as persist_exc:
                log.warning("Could not persist failed cancel state: %s", persist_exc)
            self._set_state(
                last_cancel_request={
                    "order_id": reg.get("order_id"),
                    "kind": reg.get("kind"),
                    "reason": reason,
                    "requested_at": requested_at,
                    "accepted": False,
                    "error": str(exc),
                },
                last_error=f"Could not request cancellation for live option order {reg.get('order_id')}: {exc}",
            )
            self._sync_state()
            return False

    def _check_registered_order_timeout(self, strategy: str | None = None) -> bool:
        """Cancel stale live orders so their terminal state can be reconciled.

        ENTRY orders keep the existing stale-cancel behavior. EXIT orders are
        more urgent: after a short timeout the BOT requests cancellation and,
        only after Robinhood confirms the old order is terminal with zero fill,
        re-quotes/reviews/replaces it at the current natural sell price. This
        avoids both indefinite stranded exits and duplicate sell orders.
        """
        if strategy is None and self.multi_strategy_enabled:
            original = self.active_strategy
            fired = False
            for name in self.strategy_names:
                self._activate_strategy(name)
                fired = self._check_registered_order_timeout(strategy=name) or fired
            self._activate_strategy(original)
            if fired:
                self._sync_state()
            return fired
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())

        reg = self.registered_order
        if not reg:
            return False
        if reg.get("cancel_accepted") is True:
            return False
        kind = str(reg.get("kind") or "").upper()
        timeout_seconds = (
            self.live_entry_order_timeout_seconds if kind == "ENTRY"
            else self.live_exit_order_timeout_seconds if kind == "EXIT"
            else 0.0
        )
        if timeout_seconds <= 0:
            return False
        raw = reg.get("registered_at")
        if not raw:
            return False
        try:
            registered = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            now = datetime.now(registered.tzinfo) if registered.tzinfo else datetime.now()
            age = max(0.0, (now - registered).total_seconds())
        except Exception:
            return False
        if age < timeout_seconds:
            return False

        if kind == "EXIT":
            attempts = int((self.pending_exit_intent or {}).get("exit_reprice_attempts", 0) or 0)
            if attempts >= self.max_exit_reprice_attempts:
                # Keep polling the current natural-price order rather than
                # cancelling it with no replacement. Surface the liquidity issue.
                reg["exit_reprice_exhausted"] = True
                self._write_registered_order()
                self._set_state(
                    last_error=(
                        f"{self.active_strategy} EXIT remains unfilled after {attempts} reprices; "
                        "current sell order remains live. Verify Robinhood immediately."
                    )
                )
                return False
            reg["reprice_after_cancel"] = True
            reg["reprice_attempt_next"] = attempts + 1
            self._write_registered_order()
            return self._request_cancel_registered_order(
                reason=f"EXIT remained unfilled for {age:.1f}s; cancel-confirm then reprice attempt {attempts + 1}/{self.max_exit_reprice_attempts}"
            )

        return self._request_cancel_registered_order(
            reason=f"ENTRY remained unfilled for {age:.1f}s (limit {timeout_seconds:.1f}s)"
        )

    def _replace_exit_after_terminal(self, *, prior_order: dict[str, Any], terminal_state: str) -> bool:
        """Replace a confirmed zero-fill terminal EXIT without duplicate risk."""
        intent = self.pending_exit_intent
        if not intent or not self.tracked_position:
            return False
        attempts = int(intent.get("exit_reprice_attempts", 0) or 0)
        if attempts >= self.max_exit_reprice_attempts:
            self._set_state(
                manual_reconcile_required=True,
                last_error=(
                    f"{self.active_strategy} EXIT ended {terminal_state} after exhausting automatic reprices. "
                    "The position is still tracked; verify/close it in Robinhood."
                ),
            )
            return False

        try:
            contract = self._quote_exact_contract(self.tracked_position["contract"])
            age = self._timestamp_age_seconds(contract.updated_at)
            if self.require_quote_timestamp and age is None:
                raise RuntimeError("tracked option replacement quote has no timestamp")
            if age is not None and age > self.max_quote_age_seconds:
                raise RuntimeError(
                    f"tracked option replacement quote is stale ({age:.2f}s > {self.max_quote_age_seconds:.2f}s)"
                )
            payload, review = self._review_intent(
                contract=contract, qty=int(self.tracked_position["qty"]), action="SELL"
            )
        except Exception as exc:
            self._set_state(
                manual_reconcile_required=True,
                last_error=f"Could not re-quote/review {self.active_strategy} EXIT after {terminal_state}: {exc}",
            )
            return False

        history = list(intent.get("reprice_history") or [])
        history.append({
            "prior_order_id": prior_order.get("order_id"),
            "terminal_state": terminal_state,
            "prior_limit_price": (intent.get("mcp_order_payload") or {}).get("price"),
            "replaced_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        })
        intent["reprice_history"] = history
        intent["exit_reprice_attempts"] = attempts + 1
        intent["contract"] = contract.model_dump(mode="json")
        intent["mcp_order_payload"] = payload
        intent["broker_review"] = review
        intent["final_live_submit_performed"] = False
        intent.pop("live_order_id", None)
        intent.pop("broker_place_response", None)
        intent.pop("live_submit_error", None)
        intent["repriced_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        self.registered_order = None
        self._remove_registered_order_file()
        self._write_order_intent(intent)
        self._sync_state()
        self._submit_live_intent(intent)
        return True

    def clear_pending_intent(self, *, kind: str, reason: str = "external_clear", strategy: str | None = None) -> dict[str, Any]:
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        kind = str(kind).upper().strip()
        current_intent = self.pending_entry_intent if kind == "ENTRY" else self.pending_exit_intent if kind == "EXIT" else None
        if self.registered_order and str(self.registered_order.get("kind") or "").upper() == kind:
            raise RuntimeError(f"Refusing to clear {kind}: a live Robinhood order is still registered")
        if isinstance(current_intent, dict) and bool(current_intent.get("final_live_submit_performed")):
            raise RuntimeError(f"Refusing to clear {kind}: broker submission occurred and must be reconciled first")
        if kind == "ENTRY":
            self.pending_entry_intent = None
        elif kind == "EXIT":
            self.pending_exit_intent = None
        else:
            raise ValueError("kind must be ENTRY or EXIT")
        if self.registered_order and self.registered_order.get("kind") == kind:
            self.registered_order = None
            self._remove_registered_order_file()
        if not self.pending_entry_intent and not self.pending_exit_intent:
            self._remove_order_intent_file()
        self._set_state(last_order_status={"kind": kind, "state": "CLEARED", "reason": reason})
        self._sync_state()
        return {"ok": True, "kind": kind, "reason": reason}

    def record_live_entry_fill(
        self,
        *,
        fill_price: float,
        strategy: str | None = None,
        order_id: str | None = None,
        filled_at: str | None = None,
        qty: int | None = None,
    ) -> dict[str, Any]:
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        intent = self.pending_entry_intent
        if not intent:
            raise RuntimeError("No reviewed entry intent is available to mark filled")

        selected = intent["selected_position"]
        contract = selected["contract"]
        actual_qty = int(qty or selected["qty"])
        if actual_qty < 1:
            raise ValueError("fill quantity must be >= 1")

        opened_at = filled_at or datetime.now().isoformat(timespec="milliseconds")
        self.tracked_position = {
            "strategy": self.active_strategy,
            "intent_id": intent["intent_id"],
            "status": "OPEN_LIVE",
            "direction": intent["signal"]["direction"],
            "qty": actual_qty,
            "original_qty": actual_qty,
            "partial_exit_fills": [],
            "contract": contract,
            "signal": intent["signal"],
            "selected_position": selected,
            "entry_fill_price": float(fill_price),
            "entry_order_id": order_id,
            "entry_filled_at": opened_at,
            "capital_deployed": round(float(fill_price) * 100.0 * actual_qty, 2),
            "signal_to_decision_ms": intent.get("signal_to_decision_ms"),
            "signal_to_contract_selected_ms": intent.get("signal_to_contract_selected_ms"),
        }
        self.pending_entry_intent = None
        if self.registered_order and self.registered_order.get("kind") == "ENTRY":
            self.registered_order = None
            self._remove_registered_order_file()
        self._remove_order_intent_file()
        self.risk.record_entry()

        sig = self.tracked_position["signal"]
        journal_record = {
            "strategy": self.active_strategy,
            "trade_id": self.tracked_position["intent_id"],
            "opened_at": opened_at,
            "symbol": "QQQ",
            "direction": sig.get("direction"),
            "setup": sig.get("setup"),
            "tier": sig.get("tier"),
            "lead": sig.get("lead"),
            "opportunity": sig.get("opportunity"),
            "option_side": contract.get("side"),
            "option_id": contract.get("instrument_id"),
            "expiration": contract.get("expiration"),
            "strike": contract.get("strike"),
            "qty": actual_qty,
            "entry_option_price": float(fill_price),
            "capital_deployed": self.tracked_position["capital_deployed"],
            "underlying_entry": sig.get("entry"),
            "target": sig.get("target"),
            "stop": sig.get("stop"),
            "entry_delta": contract.get("delta"),
            "entry_gamma": contract.get("gamma"),
            "entry_theta": contract.get("theta"),
            "entry_iv": contract.get("iv"),
            "entry_spread_pct": selected.get("spread_pct"),
            "selected_score": selected.get("score"),
            "projected_target_profit_dollars": selected.get("projected_target_profit_dollars"),
            "projected_stop_profit_dollars": selected.get("projected_stop_profit_dollars"),
            "signal_to_decision_ms": intent.get("signal_to_decision_ms"),
            "signal_to_contract_selected_ms": intent.get("signal_to_contract_selected_ms"),
            "entry_order_id": order_id,
            "sellout_datetime": contract.get("sellout_datetime"),
            "fill_source": "ROBINHOOD_MCP_LIVE",
        }
        try:
            self.trade_journal.start_trade(journal_record)
            self._refresh_journal_state()
        except Exception as exc:
            # A confirmed broker fill must remain managed even if local reporting
            # storage is unavailable. Block new risk and keep watchdogs active.
            self._set_state(
                manual_reconcile_required=True,
                last_error=f"ENTRY fill confirmed but trade journal persistence failed: {exc}",
            )
            log.exception("Trade journal failed after confirmed ENTRY fill")
        self._arm_position_watchdog()

        if self.risk.stop_for_day and not self.pending_exit_intent:
            self._shutdown_after_pending_order = False
            self._shutdown_after_exit = True
            self._set_state(stop_after_pending_order=False, stop_after_exit=True)
            stop_signal = TradeSignal(
                source=self.active_strategy,
                event="EXIT",
                exit_reason="STOP_DAY received while entry order was pending",
                raw="STOP_DAY_PENDING_ENTRY_FILLED",
            )
            try:
                self._prepare_exit_intent(stop_signal, trigger="STOP_DAY_AFTER_PENDING_ENTRY_FILL")
            except Exception as exc:
                self._set_state(
                    manual_reconcile_required=True,
                    last_error=f"Entry filled after STOP_DAY but automatic emergency exit could not be submitted: {exc}",
                )
        elif self._deferred_exit_signal and not self.pending_exit_intent:
            deferred = self._deferred_exit_signal
            try:
                self._prepare_exit_intent(deferred, trigger="EXIT_RECEIVED_WHILE_ENTRY_PENDING")
                self._deferred_exit_signal = None
            except Exception as exc:
                self._set_state(
                    manual_reconcile_required=True,
                    last_error=f"Entry filled after an EXIT alert but automatic sell-to-close could not be submitted: {exc}",
                )

        self._write_learning_event("ENTRY_FILL", json.loads(json.dumps(self.tracked_position, default=str)))
        self._sync_state()
        return json.loads(json.dumps(self.tracked_position, default=str))

    def record_live_exit_fill(
        self,
        *,
        fill_price: float,
        strategy: str | None = None,
        order_id: str | None = None,
        filled_at: str | None = None,
        qty: int | None = None,
    ) -> dict[str, Any] | None:
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        if not self.tracked_position:
            raise RuntimeError("No confirmed live position is tracked")

        tp = self.tracked_position
        remaining_qty = int(tp.get("qty") or 0)
        actual_qty = int(qty or remaining_qty)
        if actual_qty < 1 or actual_qty > remaining_qty:
            raise ValueError(f"exit fill quantity {actual_qty} is invalid for remaining tracked quantity {remaining_qty}")

        prior_fills = list(tp.get("partial_exit_fills") or [])
        current_fill = {
            "qty": actual_qty,
            "price": float(fill_price),
            "order_id": order_id,
            "filled_at": filled_at or datetime.now().isoformat(timespec="milliseconds"),
        }
        all_fills = prior_fills + [current_fill]
        new_remaining = remaining_qty - actual_qty

        if new_remaining > 0:
            # A non-terminal/local callback may report part of a working exit. Keep
            # exact remaining ownership and do not close the journal prematurely.
            tp["qty"] = new_remaining
            tp["partial_exit_fills"] = all_fills
            self.tracked_position = tp
            if self.pending_exit_intent:
                self.pending_exit_intent["qty"] = new_remaining
                self.pending_exit_intent["partial_exit_fills"] = all_fills
                self._write_order_intent(self.pending_exit_intent)
            self._set_state(
                manual_reconcile_required=True,
                last_error=(
                    f"{self.active_strategy} EXIT partially filled {actual_qty}; {new_remaining} contract(s) remain tracked. "
                    "BOT will only replace after the broker confirms the prior order is terminal."
                ),
            )
            self._sync_state()
            return None

        total_qty = sum(int(x.get("qty") or 0) for x in all_fills)
        if total_qty <= 0:
            raise RuntimeError("No executable EXIT quantity was recorded")
        weighted_exit = sum(float(x.get("price") or 0.0) * int(x.get("qty") or 0) for x in all_fills) / total_qty

        exit_signal = (self.pending_exit_intent or {}).get("signal", {})
        trigger = (self.pending_exit_intent or {}).get("trigger", "EXTERNAL_LIVE_EXIT")
        trade = None
        try:
            trade = self.trade_journal.close_trade({
            "closed_at": current_fill["filled_at"],
            "underlying_exit": exit_signal.get("exit_price"),
            "exit_option_price": float(weighted_exit),
            "exit_price_source": "ROBINHOOD_MCP_LIVE_FILL_WEIGHTED",
            "fill_source": "ROBINHOOD_MCP_LIVE",
            "exit_order_id": order_id,
            "exit_trigger": trigger,
            "exit_reason": exit_signal.get("exit_reason") or trigger,
            "exit_reaction_ms": (self.pending_exit_intent or {}).get("exit_reaction_ms"),
            "exit_fill_count": len(all_fills),
            "exit_fill_details": all_fills,
            }, strategy=self.active_strategy)
        except Exception as exc:
            self._set_state(
                manual_reconcile_required=True,
                last_error=f"EXIT fill confirmed but trade journal close failed: {exc}",
            )
            log.exception("Trade journal failed after confirmed EXIT fill")

        self.tracked_position = None
        self.pending_exit_intent = None
        if self.registered_order and self.registered_order.get("kind") == "EXIT":
            self.registered_order = None
            self._remove_registered_order_file()
        self._remove_order_intent_file()
        self.last_selected_position = None
        self._clear_position_watchdog()
        try:
            self._refresh_journal_state()
        except Exception as exc:
            log.warning("Journal state refresh failed after confirmed EXIT fill: %s", exc)
        self._sync_state()

        self._write_learning_event("EXIT_FILL", trade or {"strategy": self.active_strategy, "order_id": order_id, "fill_price": weighted_exit})
        if self._shutdown_after_exit:
            self._shutdown_after_exit = False
            self._request_shutdown("Live EXIT fill completed after session/STOP condition")
        return trade

    # ---------------- worker loop ----------------

    def _run(self) -> None:
        try:
            self._set_state(started_at=datetime.now().isoformat(timespec="seconds"))
            self.client.start(timeout=float(self.mcp_cfg.get("startup_timeout_seconds", 300.0)))
            names = set(self.client.tool_names())
            missing = sorted(self.REQUIRED_TOOLS - names) if names else []
            if missing:
                raise RuntimeError(f"Robinhood MCP is missing required tools: {', '.join(missing)}")

            self._discover_agentic_account()
            self._refresh_account(force=True)
            self._refresh_chain_metadata(force=True)
            self._restore_runtime_state()
            self._refresh_live_positions_state()
            self._set_state(
                ready=True,
                mcp_connected=True,
                mcp_tool_count=len(names),
            )
            self.ready_event.set()
            log.info("Robinhood MCP worker ready")

            while not self.stop_event.is_set():
                self._process_jobs(max_jobs=self.jobs_per_loop)
                self._expire_unsubmitted_entry_intent()
                self._poll_registered_order()
                self._check_registered_order_timeout()
                self._retry_deferred_exit_signal()
                self._check_underlying_hard_stop_watchdog()
                self._check_position_killswitch()
                self._check_end_of_day_session()
                now = time.monotonic()
                if now - self._last_account_refresh >= self.account_refresh_seconds:
                    self._refresh_account(force=True, quiet=True)
                if now - self._last_position_reconcile >= self.broker_position_reconcile_seconds:
                    self._last_position_reconcile = now
                    try:
                        self._refresh_live_positions_state()
                    except Exception as exc:
                        log.warning("Live-position reconciliation failed: %s", exc)
                if now - self._last_status_log >= self.console_status_seconds:
                    self._last_status_log = now
                    s = self.status()
                    log.info(
                        "MCP ready | BP=$%s | 0DTE=%s | open strategy positions=%s",
                        s.get("buying_power"),
                        s.get("zero_dte_ready"),
                        int(s.get("open_positions_count") or 0),
                    )
                time.sleep(0.03)
        except BaseException as exc:
            self.failed_event.set()
            self._set_state(ready=False, last_error=str(exc))
            log.exception("Robinhood MCP worker failed")
        finally:
            self.ready_event.set()
            try:
                self.client.stop()
            except Exception:
                pass

    def _process_jobs(self, max_jobs: int = 1) -> None:
        processed = 0
        while processed < max(1, int(max_jobs)):
            job = None
            try:
                job = self.urgent_jobs.get_nowait()
            except queue.Empty:
                try:
                    job = self.jobs.get_nowait()
                except queue.Empty:
                    break
            try:
                job.result = self._handle_signal(job.raw, received_at=job.received_at)
            except BaseException as exc:
                job.error = str(exc)
                digest = hashlib.sha256(job.raw.strip().encode("utf-8", errors="replace")).hexdigest()
                self._recent_signal_hashes.pop(digest, None)
                self._persist_recent_signal_hashes()
                self._set_state(last_error=str(exc))
                log.exception("Signal failed: %s", job.raw)
            finally:
                processed += 1
                job.done.set()
                self._set_state(signal_queue_depth=self.jobs.qsize(), urgent_signal_queue_depth=self.urgent_jobs.qsize())

    # ---------------- persistence / order-status reconciliation ----------------

    def _expire_unsubmitted_entry_intent(self, strategy: str | None = None) -> bool:
        if strategy is None and self.multi_strategy_enabled:
            original = self.active_strategy
            fired = False
            for name in self.strategy_names:
                self._activate_strategy(name)
                fired = self._expire_unsubmitted_entry_intent(strategy=name) or fired
            self._activate_strategy(original)
            if fired:
                self._sync_state()
            return fired
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        intent = self.pending_entry_intent
        if not intent or (self.registered_order and self.registered_order.get("kind") == "ENTRY"):
            return False
        if bool(intent.get("final_live_submit_performed")):
            # A live submission with no locally registered broker order is an
            # ambiguous state. Never expire it and risk a duplicate entry.
            self._set_state(
                manual_reconcile_required=True,
                last_error="A submitted ENTRY has no registered Robinhood order ID; verify Robinhood before continuing.",
            )
            return False
        created_raw = intent.get("created_at")
        if not created_raw:
            return False
        try:
            created = datetime.fromisoformat(str(created_raw))
            now = datetime.now(created.tzinfo) if created.tzinfo else datetime.now()
            age = (now - created).total_seconds()
        except Exception:
            return False
        if age < self.entry_intent_ttl_seconds:
            return False
        expired = {
            "intent_id": intent.get("intent_id"),
            "expired_at": datetime.now().isoformat(timespec="milliseconds"),
            "age_seconds": round(age, 3),
            "reason": "reviewed entry intent was not registered as a real broker order in time",
        }
        self.pending_entry_intent = None
        self._remove_order_intent_file()
        self._set_state(last_intent_expiry=expired)
        self._write_last_decision({"ok": False, "event": "ENTRY", "expired": True, **expired})
        self._sync_state()
        return True

    def _load_json_file(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
            return value if isinstance(value, dict) else None
        except Exception:
            return None

    def _restore_runtime_state(self) -> None:
        """Restore each logical strategy independently and reconcile to Robinhood.

        Robinhood aggregates quantities when MAIN and SCALPER own the exact same
        contract. The journal preserves logical lot ownership; restart recovery
        verifies that the broker's aggregate quantity is at least the sum of the
        logical lots instead of assigning the whole broker position to one source.
        """
        if not self.account_number:
            return

        original = self.active_strategy
        for strategy in self.strategy_names:
            self._activate_strategy(strategy)
            saved_intent = self._load_json_file(self.intent_path)
            if saved_intent:
                kind = str(saved_intent.get("kind", "")).upper()
                if kind == "ENTRY":
                    self.pending_entry_intent = saved_intent
                elif kind == "EXIT":
                    self.pending_exit_intent = saved_intent

            saved_order = self._load_json_file(self.registered_order_path)
            if saved_order and saved_order.get("order_id") and saved_order.get("kind") in {"ENTRY", "EXIT"}:
                saved_order.setdefault("strategy", strategy)
                self.registered_order = saved_order

            if saved_intent and bool(saved_intent.get("final_live_submit_performed")) and not self.registered_order:
                self._set_state(
                    manual_reconcile_required=True,
                    last_error=(
                        f"Recovered a submitted {strategy} order intent without its Robinhood order ID; "
                        "verify the Agentic account before continuing."
                    ),
                )

            open_trade = self.trade_journal.load_open_trade(strategy)
            if not open_trade:
                if self.pending_exit_intent or (self.registered_order and self.registered_order.get("kind") == "EXIT"):
                    self._set_state(manual_reconcile_required=True)
                self._save_active_strategy()
                continue

            option_id = str(open_trade.get("option_id") or "").strip()
            if not option_id:
                self._set_state(manual_reconcile_required=True)
                self._save_active_strategy()
                continue

            payload = _data(self.client.call_tool(
                "get_option_positions",
                {"account_number": self.account_number, "option_ids": option_id, "nonzero": True},
                timeout=15.0,
            ))
            positions = payload.get("positions") or payload.get("results") or []
            live = next(
                (x for x in positions if str(x.get("option_id") or "") == option_id and (_f(x.get("quantity"), 0.0) or 0.0) > 0),
                None,
            )
            if not live:
                self._set_state(
                    manual_reconcile_required=True,
                    last_error=f"{strategy} journal says a trade is open but Robinhood reports no matching position.",
                )
                self._save_active_strategy()
                continue

            quote_payload = _data(self.client.call_tool(
                "get_option_quotes", {"instrument_ids": [option_id]}, timeout=15.0
            ))
            q_results = quote_payload.get("results") or []
            q_item = q_results[0] if q_results else {}
            quote = q_item.get("quote") if isinstance(q_item, dict) else {}
            if not isinstance(quote, dict):
                quote = {}

            contract = {
                "symbol": "QQQ",
                "instrument_id": option_id,
                "expiration": open_trade.get("expiration"),
                "strike": open_trade.get("strike"),
                "side": open_trade.get("option_side"),
                "bid": float(_f(quote.get("bid_price"), 0.0) or 0.0),
                "ask": float(_f(quote.get("ask_price"), 0.0) or 0.0),
                "mark": _f(quote.get("mark_price"), None),
                "volume": _i(quote.get("volume"), 0),
                "open_interest": _i(quote.get("open_interest"), 0),
                "delta": _f(quote.get("delta"), open_trade.get("entry_delta")),
                "gamma": _f(quote.get("gamma"), open_trade.get("entry_gamma")),
                "theta": _f(quote.get("theta"), open_trade.get("entry_theta")),
                "iv": _f(quote.get("implied_volatility"), open_trade.get("entry_iv")),
                "updated_at": quote.get("updated_at"),
                "sellout_datetime": open_trade.get("sellout_datetime"),
            }
            # Preserve the journal-owned lot size. Broker quantity can include
            # another strategy's lot when both own the same contract.
            qty = max(1, _i(open_trade.get("qty"), 1))
            self.tracked_position = {
                "strategy": strategy,
                "intent_id": open_trade.get("trade_id"),
                "status": "OPEN_LIVE_RESTORED",
                "direction": open_trade.get("direction"),
                "qty": qty,
                "contract": contract,
                "signal": {
                    "source": strategy,
                    "direction": open_trade.get("direction"),
                    "setup": open_trade.get("setup"),
                    "entry": open_trade.get("underlying_entry"),
                    "target": open_trade.get("target"),
                    "stop": open_trade.get("stop"),
                    "tier": open_trade.get("tier"),
                },
                "selected_position": {
                    "qty": qty,
                    "contract": contract,
                    "score": open_trade.get("selected_score"),
                    "spread_pct": open_trade.get("entry_spread_pct"),
                },
                "entry_fill_price": open_trade.get("entry_option_price"),
                "entry_order_id": open_trade.get("entry_order_id"),
                "entry_filled_at": open_trade.get("opened_at"),
                "capital_deployed": open_trade.get("capital_deployed"),
            }
            self.pending_entry_intent = None
            if self.registered_order and self.registered_order.get("kind") == "ENTRY":
                self.registered_order = None
                self._remove_registered_order_file()
            if saved_intent and str(saved_intent.get("kind", "")).upper() == "ENTRY":
                self._remove_order_intent_file()
            self._arm_position_watchdog_from_timestamp(open_trade.get("opened_at"))
            self._save_active_strategy()

        self._activate_strategy(original)
        unmanaged = self._refresh_live_positions_state()
        if not unmanaged and not self.state.get("manual_reconcile_required"):
            self._set_state(manual_reconcile_required=False)
        self._sync_state()

    def _arm_position_watchdog_from_timestamp(self, opened_at: str | None) -> None:
        elapsed = 0.0
        if opened_at:
            try:
                opened = datetime.fromisoformat(str(opened_at))
                now = datetime.now(opened.tzinfo) if opened.tzinfo else datetime.now()
                elapsed = max(0.0, (now - opened).total_seconds())
            except Exception:
                elapsed = 0.0
        self._position_opened_monotonic = time.monotonic() - elapsed
        remaining = max(0.0, self.position_killswitch_minutes * 60.0 - elapsed)
        deadline = datetime.now() + timedelta(seconds=remaining)
        rt = self._runtime()
        rt["killswitch_armed"] = True
        rt["killswitch_deadline"] = deadline.isoformat(timespec="seconds")
        self._set_state(killswitch_armed=True, killswitch_deadline=deadline.isoformat(timespec="seconds"))

    def _refresh_live_positions_state(self) -> list[dict[str, Any]]:
        if not self.account_number:
            return []
        self._save_active_strategy()
        payload = _data(self.client.call_tool(
            "get_option_positions", {"account_number": self.account_number, "nonzero": True}, timeout=15.0
        ))
        position_rows = payload.get("positions") or payload.get("results") or []
        positions = [x for x in position_rows if (_f(x.get("quantity"), 0.0) or 0.0) > 0]

        managed_qty: dict[str, int] = {}
        owners: dict[str, list[str]] = {}
        for strategy, rt in self.strategy_runtime.items():
            pos = rt.get("tracked_position")
            if not isinstance(pos, dict):
                continue
            option_id = str((pos.get("contract") or {}).get("instrument_id") or "")
            if not option_id:
                continue
            qty = max(0, _i(pos.get("qty"), 0))
            managed_qty[option_id] = managed_qty.get(option_id, 0) + qty
            owners.setdefault(option_id, []).append(strategy)

        unmanaged: list[dict[str, Any]] = []
        broker_qty_by_id: dict[str, int] = {}
        for row in positions:
            option_id = str(row.get("option_id") or "")
            broker_qty = max(0, _i(row.get("quantity"), 0))
            broker_qty_by_id[option_id] = broker_qty
            expected = managed_qty.get(option_id, 0)
            if expected == broker_qty and expected > 0:
                continue
            if expected <= 0 or broker_qty > expected:
                unmanaged.append({
                    "option_id": row.get("option_id"),
                    "chain_symbol": row.get("chain_symbol"),
                    "expiration_date": row.get("expiration_date"),
                    "type": row.get("type"),
                    "quantity": row.get("quantity"),
                    "managed_quantity": expected,
                    "unmanaged_quantity": max(0, broker_qty - expected),
                })
            elif broker_qty < expected:
                # Local strategy lots claim more contracts than Robinhood holds.
                # Do not guess which strategy was manually changed.
                unmanaged.append({
                    "option_id": option_id,
                    "chain_symbol": row.get("chain_symbol"),
                    "quantity": broker_qty,
                    "managed_quantity": expected,
                    "quantity_mismatch": True,
                    "owners": owners.get(option_id, []),
                })

        # A tracked local option missing completely from Robinhood is also a mismatch.
        for option_id, expected in managed_qty.items():
            if expected > 0 and option_id not in broker_qty_by_id:
                unmanaged.append({
                    "option_id": option_id,
                    "quantity": 0,
                    "managed_quantity": expected,
                    "quantity_mismatch": True,
                    "owners": owners.get(option_id, []),
                })

        self._set_state(unmanaged_live_positions=unmanaged)
        return unmanaged

    @staticmethod
    def _weighted_execution_fill(order: dict[str, Any]) -> tuple[int, float | None, str | None]:
        total_qty = 0
        notional = 0.0
        last_ts = None
        for leg in order.get("legs") or []:
            for execution in leg.get("executions") or []:
                qty = _i(execution.get("quantity"), 0)
                price = _f(execution.get("price"), None)
                if qty > 0 and price is not None:
                    total_qty += qty
                    notional += float(price) * qty
                    last_ts = execution.get("timestamp") or last_ts
        if total_qty:
            return total_qty, notional / total_qty, last_ts

        # Some broker/order payloads can report processed quantity and an average
        # fill price without embedding per-leg execution rows. Ownership must not
        # get stuck merely because one representation is absent. Only accept fields
        # that explicitly describe an average/executed fill; never substitute the
        # working limit price as though it were an actual execution.
        processed = _i(order.get("processed_quantity"), 0)
        if processed > 0:
            for key in ("average_price", "average_fill_price", "average_execution_price", "executed_price"):
                price = _f(order.get(key), None)
                if price is not None and price > 0:
                    ts = (
                        order.get("last_transaction_at")
                        or order.get("updated_at")
                        or order.get("executed_at")
                    )
                    return processed, float(price), str(ts) if ts else None
        return processed, None, None

    def _poll_registered_order(self, strategy: str | None = None) -> None:
        if strategy is None and self.multi_strategy_enabled:
            original = self.active_strategy
            for name in self.strategy_names:
                self._activate_strategy(name)
                self._poll_registered_order(strategy=name)
            self._activate_strategy(original)
            return
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        reg = self.registered_order
        if not reg or not self.account_number:
            return
        now = time.monotonic()
        if now - self._last_order_poll < self.order_status_poll_seconds:
            return
        self._last_order_poll = now
        try:
            payload = _data(self.client.call_tool(
                "get_option_orders",
                {"account_number": self.account_number, "order_id": reg["order_id"]},
                timeout=15.0,
            ))
        except Exception as exc:
            log.warning("Order status poll failed: %s", exc)
            return

        orders = payload.get("orders") or []
        if not orders:
            return
        order = orders[0]
        state = str(order.get("state") or "").lower()
        processed = _i(order.get("processed_quantity"), 0)
        expected = _i(order.get("quantity"), 0)
        exec_qty, avg_price, filled_at = self._weighted_execution_fill(order)
        if exec_qty > 0:
            processed = max(processed, exec_qty)
        status = {
            "kind": reg.get("kind"),
            "order_id": reg.get("order_id"),
            "state": state,
            "processed_quantity": processed,
            "quantity": expected,
            "pending_quantity": order.get("pending_quantity"),
            "updated_at": order.get("updated_at"),
            "reject_reason": order.get("reject_reason"),
        }
        self._set_state(last_order_status=status)

        # Trust confirmed executions/processed quantity over a particular order
        # state spelling. A broker can report terminal/transition states after all
        # contracts have in fact executed.
        full_fill = processed > 0 and (expected <= 0 or processed >= expected)
        if full_fill and avg_price is None:
            self._set_state(
                manual_reconcile_required=True,
                last_error=(
                    f"{self.active_strategy} {reg.get('kind')} appears fully processed but Robinhood "
                    "did not provide an execution price. Live state is preserved and new risk is blocked."
                ),
            )
            self._sync_state()
            return

        if full_fill and avg_price is not None:
            if reg.get("kind") == "ENTRY":
                self.record_live_entry_fill(
                    fill_price=avg_price,
                    order_id=str(reg["order_id"]),
                    filled_at=filled_at,
                    qty=processed,
                )
            else:
                self.record_live_exit_fill(
                    fill_price=avg_price,
                    order_id=str(reg["order_id"]),
                    filled_at=filled_at,
                    qty=processed,
                )
            # The fill callbacks clear the completed order themselves. An ENTRY
            # callback may also immediately submit a deferred/STOP_DAY EXIT, so
            # never wipe ``registered_order`` here after the callback returns.
            self._sync_state()
            return

        terminal = {"rejected", "cancelled", "failed", "voided"}
        if state in terminal:
            kind = str(reg.get("kind") or "").upper()
            if processed > 0:
                # Terminal partials are no longer left as ambiguous unowned lots.
                # The broker has definitively told us how much executed, so update
                # logical ownership first. Only then may a remaining EXIT be
                # replaced, which prevents overselling the original quantity.
                prior = dict(reg)
                self.registered_order = None
                self._remove_registered_order_file()
                if kind == "ENTRY":
                    if avg_price is None:
                        self._set_state(manual_reconcile_required=True, last_error=f"{self.active_strategy} ENTRY partially filled but no execution price was reported")
                        self._sync_state()
                        return
                    self.record_live_entry_fill(
                        fill_price=avg_price, order_id=str(reg["order_id"]), filled_at=filled_at, qty=processed
                    )
                    self._set_state(
                        manual_reconcile_required=True,
                        last_error=(f"{self.active_strategy} ENTRY ended {state} after a partial {processed}/{expected} fill. "
                                    "The executed lot is tracked; new entries are blocked until reconciled."),
                    )
                    self._sync_state()
                    return

                if kind == "EXIT":
                    if avg_price is None or not self.tracked_position:
                        self._set_state(manual_reconcile_required=True, last_error=f"{self.active_strategy} EXIT partially filled but could not be safely reconciled")
                        self._sync_state()
                        return
                    remaining_before = int(self.tracked_position.get("qty") or 0)
                    if processed >= remaining_before:
                        self.record_live_exit_fill(
                            fill_price=avg_price, order_id=str(reg["order_id"]), filled_at=filled_at, qty=remaining_before
                        )
                        return
                    fill = {
                        "qty": processed, "price": float(avg_price), "order_id": str(reg["order_id"]),
                        "filled_at": filled_at or datetime.now().isoformat(timespec="milliseconds"),
                    }
                    prior_fills = list(self.tracked_position.get("partial_exit_fills") or [])
                    prior_fills.append(fill)
                    self.tracked_position["qty"] = remaining_before - processed
                    self.tracked_position["partial_exit_fills"] = prior_fills
                    if self.pending_exit_intent:
                        self.pending_exit_intent["qty"] = remaining_before - processed
                        self.pending_exit_intent["partial_exit_fills"] = prior_fills
                        self._write_order_intent(self.pending_exit_intent)
                    self._set_state(
                        manual_reconcile_required=True,
                        last_error=(f"{self.active_strategy} EXIT partially filled {processed}/{expected}; "
                                    f"{remaining_before - processed} contract(s) remain and are still managed."),
                    )
                    # Because the prior order is definitively terminal, a replacement
                    # for only the remaining strategy-owned quantity is now safe.
                    if self.pending_exit_intent and self._replace_exit_after_terminal(prior_order=prior, terminal_state=state):
                        self._sync_state()
                        return
                    self._sync_state()
                    return

            if kind == "EXIT":
                prior = dict(reg)
                # The old order is now definitively terminal with zero execution,
                # so it is safe to quote/review/place one replacement. This is the
                # point where duplicate-order risk is eliminated.
                should_replace = bool(reg.get("reprice_after_cancel")) or state in {"rejected", "failed", "voided"}
                self.registered_order = None
                self._remove_registered_order_file()
                if should_replace and self._replace_exit_after_terminal(prior_order=prior, terminal_state=state):
                    self._sync_state()
                    return
                # Automatic replacement was not possible/exhausted. Keep the EXIT
                # intent attached to the still-open tracked position for diagnostics
                # and block new entries, but do not pretend the position is flat.
                if self.pending_exit_intent:
                    self.pending_exit_intent["terminal_unfilled_state"] = state
                    self.pending_exit_intent["terminal_unfilled_order_id"] = prior.get("order_id")
                    self._write_order_intent(self.pending_exit_intent)
                self._set_state(
                    manual_reconcile_required=True,
                    last_error=(
                        f"{self.active_strategy} EXIT order ended {state} with zero fill and could not be replaced. "
                        "Position remains tracked; verify/close it in Robinhood."
                    ),
                )
                self._sync_state()
                return

            if kind == "ENTRY":
                self.pending_entry_intent = None
                self._deferred_exit_signal = None
            self.registered_order = None
            self._remove_registered_order_file()
            if not self.pending_entry_intent and not self.pending_exit_intent:
                self._remove_order_intent_file()
            self._sync_state()
            if self._shutdown_after_pending_order:
                self._shutdown_after_pending_order = False
                self._set_state(stop_after_pending_order=False)
                self._request_shutdown("STOP TRADING QQQ: pending entry order resolved without a fill")

    # ---------------- Robinhood account / market data ----------------

    def _discover_agentic_account(self) -> None:
        accounts = _data(self.client.call_tool("get_accounts", {})).get("accounts") or []
        eligible = [
            a for a in accounts
            if bool(a.get("agentic_allowed"))
            and str(a.get("state", "")).lower() == "active"
            and not bool(a.get("deactivated"))
            and not bool(a.get("permanently_deactivated"))
        ]
        if not eligible:
            raise RuntimeError("No active Robinhood Agentic account was returned by get_accounts")
        # Prefer the default agentic account when Robinhood ever exposes several.
        eligible.sort(key=lambda a: bool(a.get("is_default")), reverse=True)
        self.account_meta = eligible[0]
        self.account_number = str(self.account_meta["account_number"])
        self._set_state(
            agentic_account_last4=self.account_number[-4:],
            agentic_allowed=True,
            option_level=self.account_meta.get("option_level"),
            account_type=self.account_meta.get("type"),
        )

    def _refresh_account(self, *, force: bool = False, quiet: bool = False) -> dict[str, Any]:
        if not self.account_number:
            return {}
        if not force and time.monotonic() - self._last_account_refresh < self.account_refresh_seconds:
            return {}
        try:
            portfolio = _data(self.client.call_tool(
                "get_portfolio", {"account_number": self.account_number}, timeout=15.0
            ))
            bp_obj = portfolio.get("buying_power") or {}
            buying_power = _f(bp_obj.get("buying_power"), 0.0)
            total_value = _f(portfolio.get("total_value"), None)
            self._last_account_refresh = time.monotonic()
            self._set_state(buying_power=buying_power, portfolio_value=total_value)
            return portfolio
        except Exception as exc:
            if not quiet:
                raise
            log.warning("Account refresh failed: %s", exc)
            return {}

    def _refresh_chain_metadata(self, *, force: bool = False) -> None:
        today = _now_et().date()
        if not force and self._instruments_loaded_for == today:
            return

        chains = _data(self.client.call_tool(
            "get_option_chains", {"underlying_symbol": "QQQ"}, timeout=20.0
        )).get("chains") or []
        if not chains:
            self.chain_id = None
            self.expiration = None
            self.instruments = {"CALL": [], "PUT": []}
            # Do not cache a transient empty chain response for the entire day.
            # A later signal/startup pass is allowed to retry Robinhood.
            self._instruments_loaded_for = None
            self._set_state(current_expiration=None, current_dte=None, zero_dte_ready=False)
            return

        chain = chains[0]
        self.chain_id = str(chain.get("id") or "") or None
        expirations: list[date] = []
        for raw in chain.get("expiration_dates") or []:
            try:
                expirations.append(date.fromisoformat(str(raw)))
            except Exception:
                continue
        exp = today if today in expirations else None
        self.expiration = exp
        if not exp or not bool(chain.get("can_open_position", True)):
            self._instruments_loaded_for = None
            self.instruments = {"CALL": [], "PUT": []}
            self._set_state(
                current_expiration=str(exp) if exp else None,
                current_dte=0 if exp else None,
                zero_dte_ready=False,
            )
            return

        out: dict[str, list[dict[str, Any]]] = {"CALL": [], "PUT": []}
        for side, api_type in (("CALL", "call"), ("PUT", "put")):
            payload = _data(self.client.call_tool(
                "get_option_instruments",
                {
                    "chain_symbol": "QQQ",
                    "expiration_dates": exp.isoformat(),
                    "type": api_type,
                },
                timeout=25.0,
            ))
            for inst in payload.get("instruments") or []:
                try:
                    strike = float(inst["strike_price"])
                except Exception:
                    continue
                if str(inst.get("state", "active")).lower() != "active":
                    continue
                if str(inst.get("tradability", "tradable")).lower() != "tradable":
                    continue
                entry = dict(inst)
                entry["strike"] = strike
                entry["side"] = side
                out[side].append(entry)
            out[side].sort(key=lambda x: x["strike"])
        self.instruments = out
        if not out["CALL"] or not out["PUT"]:
            self._instruments_loaded_for = None
            self._set_state(current_expiration=exp.isoformat(), current_dte=0, zero_dte_ready=False)
            return
        self._instruments_loaded_for = today
        self._set_state(current_expiration=exp.isoformat(), current_dte=0, zero_dte_ready=True)

    @staticmethod
    def _timestamp_age_seconds(raw: str | None) -> float | None:
        if not raw:
            return None
        try:
            updated = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if updated.tzinfo is None:
                updated = updated.replace(tzinfo=timezone.utc)
            return max(0.0, (datetime.now(timezone.utc) - updated.astimezone(timezone.utc)).total_seconds())
        except Exception:
            return None

    def _current_qqq_quote(self) -> dict[str, Any]:
        quote_data = _data(self.client.call_tool(
            "get_equity_quotes", {"symbols": ["QQQ"]}, timeout=15.0
        ))
        results = quote_data.get("results") or []
        if not results:
            raise RuntimeError("Robinhood returned no QQQ quote")
        item = results[0]
        quote = item.get("quote") if isinstance(item, dict) else None
        if not isinstance(quote, dict):
            quote = item if isinstance(item, dict) else {}

        bid = _f(quote.get("bid_price"), None)
        ask = _f(quote.get("ask_price"), None)
        last = _f(quote.get("last_trade_price"), None)
        if last is None or last <= 0:
            last = _f(quote.get("last_non_reg_trade_price"), None)
        midpoint = None
        if bid is not None and ask is not None and bid > 0 and ask >= bid:
            midpoint = (bid + ask) / 2.0
        reference = midpoint if midpoint is not None and midpoint > 0 else last
        if reference is None or reference <= 0:
            raise RuntimeError("Robinhood QQQ quote did not contain a usable price")

        updated_at = (
            quote.get("updated_at")
            or quote.get("last_trade_updated_at")
            or quote.get("last_updated_at")
        )
        age = self._timestamp_age_seconds(str(updated_at) if updated_at else None)
        require_ts = bool(self.worker_cfg.get("require_underlying_quote_timestamp", False))
        if require_ts and age is None:
            raise RuntimeError("Robinhood QQQ quote is missing a usable timestamp")
        max_age = float(self.worker_cfg.get("max_underlying_quote_age_seconds", 3.0))
        if age is not None and max_age > 0 and age > max_age:
            raise RuntimeError(f"Robinhood QQQ quote is stale ({age:.2f}s > {max_age:.2f}s)")

        out = {
            "bid": float(bid) if bid is not None and bid > 0 else None,
            "ask": float(ask) if ask is not None and ask > 0 else None,
            "last": float(last) if last is not None and last > 0 else None,
            "mid": float(midpoint) if midpoint is not None and midpoint > 0 else None,
            "reference": float(reference),
            "updated_at": str(updated_at) if updated_at else None,
            "age_seconds": age,
        }
        self._set_state(
            share_price=out["reference"],
            last_underlying_quote_bid=out["bid"],
            last_underlying_quote_ask=out["ask"],
            last_underlying_quote_updated_at=out["updated_at"],
            last_underlying_quote_age_seconds=round(age, 2) if age is not None else None,
        )
        return out

    def _current_qqq_price(self) -> float:
        return float(self._current_qqq_quote()["reference"])

    def _window_instruments(
        self,
        share_price: float,
        side: str | None = None,
        *,
        each_side_override: int | None = None,
    ) -> list[dict[str, Any]]:
        each_side = int(each_side_override if each_side_override is not None else self.worker_cfg.get("window_strikes_each_side", 4))
        # Build the strike ladder from the side actually being traded. Calls and
        # puts usually share strikes, but the broker is not required to return
        # perfectly identical ladders. Using CALLs as the reference for a PUT-only
        # signal could otherwise omit a valid put strike. For BOTH, use the union.
        requested = str(side or "BOTH").upper()
        sides = (requested,) if requested in {"CALL", "PUT"} else ("CALL", "PUT")
        if requested in {"CALL", "PUT"}:
            reference = [{"strike": x["strike"]} for x in self.instruments[requested]]
        else:
            reference = [{"strike": strike} for strike in sorted({float(x["strike"]) for opt_side in sides for x in self.instruments[opt_side]})]
        selected = strike_window(reference, share_price, each_side=each_side)
        strikes = {float(x["strike"]) for x in selected}
        window: list[dict[str, Any]] = []
        for opt_side in sides:
            window.extend(x for x in self.instruments[opt_side] if float(x["strike"]) in strikes)
        watched = dict(self.state.get("watched_window") or {"CALL": [], "PUT": []})
        for opt_side in sides:
            watched[opt_side] = sorted(strikes)
        self._set_state(watched_window=watched)
        return window

    def _quote_contracts(
        self,
        side: str | None = None,
        *,
        each_side_override: int | None = None,
    ) -> tuple[float, list[OptionContract]]:
        # Cache independently by option side *and strike-window size*. This
        # preserves the sub-second cache for simultaneous MAIN/SCALPER alerts but
        # still allows a bounded adaptive expansion when a small account cannot
        # afford any contract in the initial near-spot window.
        each_side = int(each_side_override if each_side_override is not None else self.worker_cfg.get("window_strikes_each_side", 4))
        cache_key = f"{str(side or 'BOTH').upper()}:{each_side}"
        cached = self._quote_cache.get(cache_key)
        if cached is not None and self.shared_quote_cache_seconds > 0:
            cached_at, share_price, contracts, cached_quote_age = cached
            cache_age = max(0.0, time.monotonic() - cached_at)
            if cache_age <= self.shared_quote_cache_seconds:
                effective_age = None if cached_quote_age is None else cached_quote_age + cache_age
                self._set_state(
                    share_price=share_price,
                    last_quote_batch_ms=0.0,
                    last_quote_age_seconds=round(effective_age, 2) if effective_age is not None else None,
                )
                return share_price, contracts

        self._refresh_chain_metadata(force=False)
        if not self.expiration:
            raise RuntimeError("No same-day QQQ expiration is available; this bot is 0DTE-only")
        share_price = self._current_qqq_price()
        window = self._window_instruments(share_price, side=side, each_side_override=each_side)
        ids = [str(x.get("id")) for x in window if x.get("id")]
        if not ids:
            raise RuntimeError("No tradable 0DTE QQQ instrument IDs were found near spot")

        started = time.perf_counter()
        payload = _data(self.client.call_tool(
            "get_option_quotes", {"instrument_ids": ids}, timeout=15.0
        ))
        elapsed_ms = (time.perf_counter() - started) * 1000.0
        results = payload.get("results") or []
        quotes: dict[str, dict[str, Any]] = {}
        for item in results:
            q = item.get("quote") if isinstance(item, dict) else None
            if not isinstance(q, dict):
                q = item if isinstance(item, dict) else {}
            iid = str(q.get("instrument_id") or "")
            if iid:
                quotes[iid] = q

        contracts: list[OptionContract] = []
        last_updated = None
        for inst in window:
            iid = str(inst.get("id") or "")
            q = quotes.get(iid)
            if not q:
                continue
            greeks = q.get("greeks") if isinstance(q.get("greeks"), dict) else {}
            last_updated = q.get("updated_at") or last_updated
            contracts.append(OptionContract(
                symbol="QQQ",
                instrument_id=iid,
                expiration=self.expiration,
                strike=float(inst["strike"]),
                side=inst["side"],
                bid=float(_f(q.get("bid_price"), 0.0) or 0.0),
                ask=float(_f(q.get("ask_price"), 0.0) or 0.0),
                bid_size=_i(q.get("bid_size"), 0),
                ask_size=_i(q.get("ask_size"), 0),
                mark=_f(q.get("mark_price"), None),
                volume=_i(q.get("volume"), 0),
                open_interest=_i(q.get("open_interest"), 0),
                delta=_f(q.get("delta"), _f(greeks.get("delta"), None)),
                gamma=_f(q.get("gamma"), _f(greeks.get("gamma"), None)),
                theta=_f(q.get("theta"), _f(greeks.get("theta"), None)),
                iv=_f(q.get("implied_volatility"), None),
                vega=_f(q.get("vega"), _f(greeks.get("vega"), None)),
                rho=_f(q.get("rho"), _f(greeks.get("rho"), None)),
                chance_of_profit_long=_f(q.get("chance_of_profit_long"), None),
                chance_of_profit_short=_f(q.get("chance_of_profit_short"), None),
                low_fill_rate_buy_price=_f(q.get("low_fill_rate_buy_price"), None),
                high_fill_rate_buy_price=_f(q.get("high_fill_rate_buy_price"), None),
                low_fill_rate_sell_price=_f(q.get("low_fill_rate_sell_price"), None),
                high_fill_rate_sell_price=_f(q.get("high_fill_rate_sell_price"), None),
                break_even_price=_f(q.get("break_even_price"), None),
                updated_at=str(q.get("updated_at")) if q.get("updated_at") else None,
                sellout_datetime=str(inst.get("sellout_datetime")) if inst.get("sellout_datetime") else None,
                buy_blocked=False,
                expires_soon_warning=False,
            ))

        self._quote_latency_ms.append(elapsed_ms)
        latency_values = sorted(self._quote_latency_ms)
        avg_ms = sum(latency_values) / len(latency_values) if latency_values else None
        p95_ms = None
        if latency_values:
            p95_index = max(0, min(len(latency_values) - 1, int((len(latency_values) - 1) * 0.95 + 0.999999)))
            p95_ms = latency_values[p95_index]

        quote_ages: list[float] = []
        now_utc = datetime.now(timezone.utc)
        for contract in contracts:
            if not contract.updated_at:
                continue
            try:
                updated = datetime.fromisoformat(str(contract.updated_at).replace("Z", "+00:00"))
                if updated.tzinfo is None:
                    updated = updated.replace(tzinfo=timezone.utc)
                quote_ages.append(max(0.0, (now_utc - updated.astimezone(timezone.utc)).total_seconds()))
            except Exception:
                continue
        max_quote_age = max(quote_ages) if quote_ages else None

        self._set_state(
            last_quote_batch_ms=round(elapsed_ms, 1),
            last_quote_updated_at=last_updated,
            last_quote_age_seconds=round(max_quote_age, 2) if max_quote_age is not None else None,
            quote_latency_avg_ms=round(avg_ms, 1) if avg_ms is not None else None,
            quote_latency_p95_ms=round(p95_ms, 1) if p95_ms is not None else None,
        )
        self._quote_cache[cache_key] = (time.monotonic(), share_price, contracts, max_quote_age)
        return share_price, contracts

    def _fresh_contracts(self, contracts: list[OptionContract]) -> list[OptionContract]:
        """Return only contracts whose *own* quote satisfies freshness policy.

        v0.7.5 used the oldest timestamp in the whole quote batch as a single
        gate. That could block a fresh chosen contract because a different strike
        was stale, while a chosen quote with no timestamp could slip through when
        another strike happened to have one. Per-contract filtering fixes both.
        """
        fresh: list[OptionContract] = []
        stale = 0
        missing = 0
        for contract in contracts:
            age = self._timestamp_age_seconds(contract.updated_at)
            if age is None:
                if self.require_quote_timestamp:
                    missing += 1
                    continue
                fresh.append(contract)
                continue
            if age > self.max_quote_age_seconds:
                stale += 1
                continue
            fresh.append(contract)
        self._set_state(
            last_quote_fresh_count=len(fresh),
            last_quote_stale_count=stale,
            last_quote_missing_timestamp_count=missing,
        )
        return fresh

    def _write_selection_snapshot(
        self,
        *,
        signal: TradeSignal,
        share_price: float,
        budget: float,
        contracts: list[OptionContract],
        ranked: list[PositionCandidate],
    ) -> None:
        """Persist the live decision surface for later forward-analysis.

        This is intentionally observational: it does not change which contract
        wins. Keeping the complete nearby quote set and ranked candidates lets us
        later measure whether spread, delta, gamma, theta, liquidity, or fill-rate
        features actually predicted better realized option outcomes.
        """
        try:
            top = []
            for item in ranked[:8]:
                top.append({
                    "rank": len(top) + 1,
                    "score": round(float(item.score), 6),
                    "qty": int(item.qty),
                    "capital_required": round(float(item.capital_required), 2),
                    "projected_target_profit_dollars": round(float(item.projected_target_profit_dollars), 4),
                    "projected_stop_profit_dollars": round(float(item.projected_stop_profit_dollars), 4),
                    "projected_target_return": round(float(item.projected_target_return), 6),
                    "projected_stop_return": round(float(item.projected_stop_return), 6),
                    "reward_risk": round(float(item.reward_risk), 6),
                    "spread_pct": round(float(item.spread_pct), 6),
                    "budget_utilization": round(float(item.budget_utilization), 6),
                    "notes": list(item.notes),
                    "contract": item.contract.model_dump(mode="json"),
                })
            snapshot = {
                "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "signal": signal.model_dump(mode="json"),
                "share_price": float(share_price),
                "effective_budget": round(float(budget), 2),
                "quote_batch_ms": self.state.get("last_quote_batch_ms"),
                "quote_age_seconds": self.state.get("last_quote_age_seconds"),
                "contracts": [c.model_dump(mode="json") for c in contracts],
                "ranked": top,
                "chosen": top[0] if top else None,
            }
            with self.selection_snapshots_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(snapshot, default=str) + "\n")
            self._write_learning_event("SELECTION", snapshot)
            public = {
                "captured_at_utc": snapshot["captured_at_utc"],
                "share_price": snapshot["share_price"],
                "effective_budget": snapshot["effective_budget"],
                "candidate_count": len(ranked),
                "chosen": snapshot["chosen"],
            }
            self._set_state(last_selection_snapshot=public)
        except Exception as exc:
            log.warning("Could not write selector snapshot: %s", exc)

    # ---------------- order review / intent preparation ----------------

    @staticmethod
    def _parse_et_clock(raw_clock: str, *, now: datetime | None = None) -> datetime:
        now = now or _now_et()
        hh_s, mm_s = str(raw_clock).strip().split(":", 1)
        return now.replace(hour=int(hh_s), minute=int(mm_s), second=0, microsecond=0)

    def _entry_cutoff_ok(self, contract: OptionContract) -> tuple[bool, str]:
        """Fail closed before Robinhood's expiration-day opening cutoff.

        QQQ can trade later than most equity options, but Robinhood stops opening
        same-day-expiring stock/ETF option positions at 15:30 ET. We keep a
        configurable cushion and also use the contract sellout timestamp when
        Robinhood provides one. The earlier cutoff wins.
        """
        now = _now_et()
        fallback_clock = str(self.worker_cfg.get("fallback_entry_cutoff_et", "23:59")).strip()
        try:
            hard_cutoff = self._parse_et_clock(fallback_clock, now=now)
        except Exception:
            return False, "hard 0DTE entry cutoff configuration is invalid"

        if now >= hard_cutoff:
            self._set_state(session_entry_open=False)
            return False, f"past hard 0DTE entry cutoff ({hard_cutoff.strftime('%H:%M ET')})"

        raw = contract.sellout_datetime
        if not raw:
            self._set_state(session_entry_open=True)
            return True, f"before hard 0DTE cutoff ({hard_cutoff.strftime('%H:%M ET')}); Robinhood sellout timestamp unavailable"

        try:
            sellout = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if sellout.tzinfo is None:
                sellout = sellout.replace(tzinfo=timezone.utc)
            minutes = float(self.worker_cfg.get("entry_cutoff_minutes_before_sellout", 60.0))
            sellout_cutoff = sellout.astimezone(ET) - timedelta(minutes=minutes)
            cutoff = min(hard_cutoff, sellout_cutoff)
            if now >= cutoff:
                self._set_state(session_entry_open=False)
                return False, f"past configured 0DTE entry cutoff ({cutoff.strftime('%H:%M ET')})"
            self._set_state(session_entry_open=True)
            return True, f"before 0DTE cutoff ({cutoff.strftime('%H:%M ET')})"
        except Exception:
            # 0DTE is intentionally fail-closed: if Robinhood supplied a sellout
            # timestamp but we cannot parse it, do not guess that a new entry is safe.
            self._set_state(session_entry_open=False)
            return False, "Robinhood sellout timestamp is unparseable; blocking new 0DTE entry"

    def _force_exit_cutoff_for_position(self, tracked_position: dict[str, Any]) -> tuple[datetime, str]:
        now = _now_et()
        fallback_clock = str(self.base_worker_cfg.get("fallback_force_exit_et", "23:59")).strip()
        fallback = self._parse_et_clock(fallback_clock, now=now)
        contract = tracked_position.get("contract") if isinstance(tracked_position, dict) else {}
        raw = (contract or {}).get("sellout_datetime") if isinstance(contract, dict) else None
        if not raw:
            return fallback, f"fallback force-flat {fallback.strftime('%H:%M ET')}"
        try:
            sellout = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if sellout.tzinfo is None:
                sellout = sellout.replace(tzinfo=timezone.utc)
            minutes = float(self.base_worker_cfg.get("force_exit_minutes_before_sellout", 35.0))
            cutoff = sellout.astimezone(ET) - timedelta(minutes=minutes)
            return min(fallback, cutoff), f"force-flat {min(fallback, cutoff).strftime('%H:%M ET')}"
        except Exception:
            return fallback, f"fallback force-flat {fallback.strftime('%H:%M ET')}"

    def _check_end_of_day_session(self) -> bool:
        """Own the session clock without depending on a TradingView STOP_DAY alert.

        v0.7.5 deliberately *idles* after hours instead of killing the process.
        That matters with Quick Tunnels: killing/restarting cloudflared creates a
        different webhook URL. Keeping BOT alive preserves the current tunnel and
        lets it become entry-eligible again on the next trading day.

        - New entries are blocked by _entry_cutoff_ok().
        - Near expiration closeout, pending entries are cancelled and BOT-owned
          positions are exited.
        - After the configured idle clock, a flat BOT remains alive in AFTER_HOURS
          mode. Optional legacy process exit can be enabled explicitly.
        """
        now = _now_et()
        if now.weekday() >= 5:
            self._set_state(session_entry_open=False, session_phase="WEEKEND")
            return False

        try:
            entry_clock = self._parse_et_clock(str(self.base_worker_cfg.get("fallback_entry_cutoff_et", "23:59")), now=now)
            entry_open = now < entry_clock
            self._set_state(session_entry_open=entry_open)
        except Exception:
            entry_clock = now.replace(hour=0, minute=0, second=0, microsecond=0)
            entry_open = False
            self._set_state(session_entry_open=False, session_phase="CONFIG_ERROR", last_error="Invalid fallback_entry_cutoff_et configuration")

        original = self.active_strategy
        acted = False
        try:
            for name in self.strategy_names:
                self._activate_strategy(name)
                tracked = self.tracked_position
                if tracked:
                    cutoff, label = self._force_exit_cutoff_for_position(tracked)
                else:
                    try:
                        cutoff = self._parse_et_clock(str(self.base_worker_cfg.get("fallback_force_exit_et", "23:59")), now=now)
                    except Exception:
                        cutoff = now.replace(hour=15, minute=40, second=0, microsecond=0)
                    label = f"fallback force-flat {cutoff.strftime('%H:%M ET')}"

                if now >= cutoff:
                    if self.registered_order and str(self.registered_order.get("kind")) == "ENTRY":
                        if not self._deferred_exit_signal:
                            self._deferred_exit_signal = TradeSignal(
                                source=name, event="EXIT", exit_reason="END_OF_DAY_FORCE_FLAT",
                                raw=f"SOURCE {name} | EXIT QQQ | END_OF_DAY_FORCE_FLAT",
                            )
                        self._request_cancel_registered_order(reason=f"{name} end-of-day force-flat window reached")
                        acted = True
                    elif self.pending_entry_intent and not bool(self.pending_entry_intent.get("final_live_submit_performed")):
                        self.pending_entry_intent = None
                        self._remove_order_intent_file()
                        acted = True

                    if self.tracked_position and not self._deferred_exit_signal and not self.pending_exit_intent and not (self.registered_order and str(self.registered_order.get("kind")) == "EXIT"):
                        signal = TradeSignal(
                            source=name, event="EXIT", exit_reason="END_OF_DAY_FORCE_FLAT",
                            raw=f"SOURCE {name} | EXIT QQQ | END_OF_DAY_FORCE_FLAT",
                        )
                        try:
                            intent = self._prepare_exit_intent(signal, trigger="END_OF_DAY_FORCE_FLAT")
                            self._set_state(last_end_of_day_action={
                                "strategy": name, "action": "FORCE_EXIT", "at": now.isoformat(),
                                "cutoff": label, "order_id": intent.get("live_order_id"),
                            })
                        except Exception as exc:
                            # Trading session closeout is risk-reducing. Preserve the
                            # request locally and retry at the bounded deferred-EXIT
                            # cadence instead of hammering MCP every worker loop.
                            if not self.pending_exit_intent:
                                self._deferred_exit_signal = signal
                                self._last_deferred_exit_retry = 0.0
                            self._set_state(
                                manual_reconcile_required=True,
                                last_error=f"{name} end-of-day exit could not be submitted immediately; retry armed: {exc}",
                                last_end_of_day_action={"strategy": name, "action": "FORCE_EXIT_RETRY_ARMED", "at": now.isoformat(), "error": str(exc)},
                            )
                        acted = True
        finally:
            self._activate_strategy(original)

        idle_key = "after_hours_idle_et" if self.base_worker_cfg.get("after_hours_idle_et") else "auto_shutdown_when_flat_et"
        try:
            idle_clock = self._parse_et_clock(str(self.base_worker_cfg.get(idle_key, "23:59")), now=now)
        except Exception:
            idle_clock = now.replace(hour=16, minute=0, second=0, microsecond=0)

        self._save_active_strategy()
        any_open = any(rt.get("tracked_position") for rt in self.strategy_runtime.values())
        any_pending = any(rt.get("pending_entry_intent") or rt.get("pending_exit_intent") or rt.get("registered_order") for rt in self.strategy_runtime.values())

        if now >= idle_clock:
            self._set_state(session_entry_open=False)
            if not any_open and not any_pending:
                self._set_state(
                    session_phase="AFTER_HOURS_FLAT",
                    last_end_of_day_action={"action": "AFTER_HOURS_FLAT", "at": now.isoformat()},
                )
                if bool(self.base_worker_cfg.get("exit_process_end_of_day", False)):
                    self._request_shutdown("End-of-day: all strategies flat")
                    return True
            else:
                self._set_state(
                    session_phase="AFTER_HOURS_MANAGING",
                    last_end_of_day_action={
                        "action": "AFTER_HOURS_MANAGING", "at": now.isoformat(),
                        "reason": "position/order still active; BOT remains alive until flat",
                    },
                )
        elif not entry_open:
            self._set_state(session_phase="NO_NEW_ENTRIES")
        else:
            self._set_state(session_phase="TRADING")

        if acted:
            self._sync_state()
        return acted

    @staticmethod
    def _underlying_signal_guard(signal: TradeSignal, share_price: float, tolerance: float = 0.005) -> tuple[bool, str, dict[str, float | None]]:
        """Reject an entry only when Robinhood spot already invalidated/completed it.

        This is intentionally not a subjective chase filter. It only prevents
        buying after the TradingView target has already been reached or after the
        stop/failure level has already broken, with a half-cent feed tolerance.
        """
        if signal.entry is None or signal.target is None or signal.stop is None or not signal.direction:
            return True, "underlying guard unavailable", {"progress": None, "adverse": None}
        entry, target, stop = float(signal.entry), float(signal.target), float(signal.stop)
        px = float(share_price)
        if signal.direction == "LONG":
            target_dist = target - entry
            stop_dist = entry - stop
            progress = (px - entry) / target_dist if target_dist > 0 else None
            adverse = (entry - px) / stop_dist if stop_dist > 0 else None
            if target_dist > 0 and px >= target + tolerance:
                return False, "Robinhood QQQ price already passed the TradingView target; stale/chased entry blocked", {"progress": progress, "adverse": adverse}
            if stop_dist > 0 and px <= stop - tolerance:
                return False, "Robinhood QQQ price already broke the TradingView stop; invalid entry blocked", {"progress": progress, "adverse": adverse}
        else:
            target_dist = entry - target
            stop_dist = stop - entry
            progress = (entry - px) / target_dist if target_dist > 0 else None
            adverse = (px - entry) / stop_dist if stop_dist > 0 else None
            if target_dist > 0 and px <= target - tolerance:
                return False, "Robinhood QQQ price already passed the TradingView target; stale/chased entry blocked", {"progress": progress, "adverse": adverse}
            if stop_dist > 0 and px >= stop + tolerance:
                return False, "Robinhood QQQ price already broke the TradingView stop; invalid entry blocked", {"progress": progress, "adverse": adverse}
        return True, "underlying target/stop still live", {"progress": progress, "adverse": adverse}

    def _limit_price_for(self, contract: OptionContract, action: str) -> float:
        if action == "BUY":
            # Speed-first 0DTE entry: size at the live ask and submit at the ask.
            # Robinhood describes the ask as the buyer's natural price and the
            # price most likely to fill. Never exceed it, so the budget remains
            # a hard ceiling. High-fill/mark are only fallbacks if ask is missing.
            high_fill = contract.high_fill_rate_buy_price
            if high_fill is not None and high_fill > 0 and contract.ask > 0:
                high_fill = min(float(high_fill), float(contract.ask))
            candidates = [contract.ask, high_fill, contract.mark, contract.mid]
        else:
            # Risk-reducing exits prioritize getting flat. Robinhood's natural
            # sell price is the live bid; using a higher working limit can strand
            # a 0DTE exit while the underlying keeps moving.
            high_fill = contract.high_fill_rate_sell_price
            candidates = [contract.bid, high_fill, contract.mark, contract.mid]
        for value in candidates:
            if value is not None and float(value) > 0:
                # QQQ option instruments reported $0.01 minimum ticks in our MCP
                # calibration. Robinhood's review will still reject an invalid tick.
                return round(float(value) + 1e-10, 2)
        raise RuntimeError("No usable option price is available for order review")

    def _review_intent(
        self,
        *,
        contract: OptionContract,
        qty: int,
        action: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        if not self.account_number or not contract.instrument_id:
            raise RuntimeError("Missing Agentic account or option instrument ID")
        price = self._limit_price_for(contract, action)
        payload = {
            "account_number": self.account_number,
            "legs": [{
                "option_id": contract.instrument_id,
                "side": "buy" if action == "BUY" else "sell",
                "position_effect": "open" if action == "BUY" else "close",
                "ratio_quantity": 1,
            }],
            "quantity": str(int(qty)),
            "chain_symbol": "QQQ",
            "underlying_type": "equity",
            "market_hours": "regular_hours",
            "price": f"{price:.2f}",
            "time_in_force": str(self.mcp_cfg.get("time_in_force", "gfd")),
            "type": "limit",
        }
        review = self.client.call_tool("review_option_order", payload, timeout=15.0)
        review_data = _data(review)
        checks = review_data.get("order_checks")
        if isinstance(checks, dict):
            alert_type = checks.get("alertType") or checks.get("alert_type")
            if alert_type:
                raise RuntimeError(f"Robinhood order review returned broker alert: {alert_type}")
        return payload, review

    def _place_payload_from_review(self, reviewed_payload: dict[str, Any]) -> dict[str, Any]:
        """Build placement arguments from the exact reviewed order.

        Robinhood exposes MCP input schemas dynamically. When a schema is
        available, only send fields accepted by ``place_option_order`` and add
        optional idempotency / leg-ratio fields only when the live schema says
        they exist. Without a schema, use the reviewed payload unchanged.
        """
        payload = json.loads(json.dumps(reviewed_payload))
        schema = None
        getter = getattr(self.client, "tool_schema", None)
        if callable(getter):
            try:
                schema = getter("place_option_order")
            except Exception:
                schema = None

        properties = schema.get("properties", {}) if isinstance(schema, dict) else {}
        if isinstance(properties, dict) and properties:
            payload = {k: v for k, v in payload.items() if k in properties}

            legs_schema = properties.get("legs")
            items_schema = legs_schema.get("items", {}) if isinstance(legs_schema, dict) else {}
            leg_props = items_schema.get("properties", {}) if isinstance(items_schema, dict) else {}
            if isinstance(leg_props, dict) and leg_props and isinstance(payload.get("legs"), list):
                filtered_legs = []
                for leg in payload["legs"]:
                    if isinstance(leg, dict):
                        clean_leg = {k: v for k, v in leg.items() if k in leg_props}
                        if "ratio_quantity" in leg_props:
                            clean_leg.setdefault("ratio_quantity", 1)
                        filtered_legs.append(clean_leg)
                payload["legs"] = filtered_legs

            if "ref_id" in properties:
                payload.setdefault("ref_id", str(uuid.uuid4()))
        return payload

    @staticmethod
    def _placed_order_id(response: dict[str, Any]) -> str | None:
        data = _data(response)
        order = data.get("order")
        if isinstance(order, dict):
            value = order.get("id") or order.get("order_id")
            if value:
                return str(value)
        value = data.get("order_id") or data.get("id")
        return str(value) if value else None

    def _submit_live_intent(self, intent: dict[str, Any]) -> dict[str, Any]:
        """Submit one already-reviewed intent exactly once.

        A timeout or malformed placement response is treated as ambiguous and
        blocks further automation. We deliberately do not retry a live order,
        because an accepted first request followed by a retry could duplicate a
        position.
        """
        if not self.auto_submit_live_orders:
            return intent
        # Reconciliation mode must block *new risk*, not risk-reducing exits. A
        # prior partial/mismatch should never make the BOT refuse a sell-to-close.
        if self.state.get("manual_reconcile_required") and str(intent.get("kind", "")).upper() == "ENTRY":
            raise RuntimeError("Live ENTRY auto-submit blocked because manual reconciliation is required")

        place_payload = self._place_payload_from_review(intent["mcp_order_payload"])
        intent["mcp_place_payload"] = place_payload
        intent["live_submit_attempted_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
        try:
            self._write_order_intent(intent)
        except Exception as exc:
            if str(intent.get("kind") or "").upper() == "ENTRY":
                self._set_state(manual_reconcile_required=True, last_error=f"Could not persist ENTRY intent before broker submission: {exc}")
                raise RuntimeError("Refusing live ENTRY because its recovery state could not be persisted") from exc
            # EXIT is risk-reducing and the already-open journal/broker position can
            # be reconstructed after restart. Do not strand exposure because a local
            # log/state file is temporarily locked.
            log.warning("Could not persist EXIT intent before broker submission; continuing risk-reducing exit: %s", exc)
            self._set_state(last_error=f"EXIT persistence warning: {exc}")

        try:
            response = self.client.call_tool("place_option_order", place_payload, timeout=20.0)
        except Exception as exc:
            intent["live_submit_error"] = str(exc)
            try:
                self._write_order_intent(intent)
            except Exception as persist_exc:
                log.warning("Could not persist ambiguous broker placement state: %s", persist_exc)
            self._set_state(
                manual_reconcile_required=True,
                last_error=(
                    "Robinhood place_option_order did not complete cleanly. The result may be ambiguous; "
                    "check the Agentic account before restarting or sending another trade. " + str(exc)
                ),
            )
            self._sync_state()
            raise

        order_id = self._placed_order_id(response)
        if not order_id:
            intent["broker_place_response"] = response
            intent["live_submit_error"] = "Robinhood placement response contained no order ID"
            try:
                self._write_order_intent(intent)
            except Exception as persist_exc:
                log.warning("Could not persist no-order-id broker response: %s", persist_exc)
            self._set_state(
                manual_reconcile_required=True,
                last_error="Robinhood placement returned no order ID; verify the Agentic account before continuing.",
            )
            self._sync_state()
            raise RuntimeError("Robinhood place_option_order returned no order ID")

        intent["final_live_submit_performed"] = True
        intent["live_order_id"] = order_id
        intent["broker_place_response"] = response
        intent["live_submitted_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

        # Broker truth wins over local persistence. As soon as an order ID exists,
        # put it in live memory so the poller can manage it even if Defender/disk
        # temporarily blocks a state-file replace.
        self.registered_order = {
            "strategy": self.active_strategy,
            "kind": str(intent["kind"]).upper(),
            "order_id": order_id,
            "registered_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        }
        persistence_errors = []
        try:
            self._write_order_intent(intent)
        except Exception as exc:
            persistence_errors.append(f"intent: {exc}")
        try:
            self._write_registered_order()
        except Exception as exc:
            persistence_errors.append(f"registered-order: {exc}")
        if persistence_errors:
            self._set_state(
                manual_reconcile_required=True,
                last_error=(
                    "Live Robinhood order is registered in memory but recovery-state persistence failed: "
                    + "; ".join(persistence_errors)
                ),
            )
        else:
            self._set_state(last_error=None)
        self._sync_state()
        return intent

    def _prepare_entry_intent(
        self,
        signal: TradeSignal,
        candidate: PositionCandidate,
        *,
        signal_started: float,
        contract_selected_at: float,
    ) -> dict[str, Any]:
        payload, review = self._review_intent(
            contract=candidate.contract, qty=candidate.qty, action="BUY"
        )
        # Robinhood review can take long enough for a fast intraday signal to
        # become stale. Re-check locally immediately before creating/submitting
        # the live intent; exits are deliberately not freshness-blocked.
        fresh, fresh_reason = self.risk.signal_is_fresh(signal)
        if not fresh:
            raise RuntimeError(f"ENTRY became stale during Robinhood review: {fresh_reason}")
        now = datetime.now().isoformat(timespec="milliseconds")
        intent = {
            "intent_id": f"entry-{self.active_strategy.lower()}-{int(time.time() * 1000)}",
            "strategy": self.active_strategy,
            "kind": "ENTRY",
            "created_at": now,
            "trigger": "TRADINGVIEW_ENTRY",
            "signal": signal.model_dump(mode="json"),
            "selected_position": candidate.model_dump(mode="json"),
            "mcp_order_payload": payload,
            "broker_review": review,
            "final_live_submit_performed": False,
            "signal_to_contract_selected_ms": round((contract_selected_at - signal_started) * 1000.0, 1),
            "signal_to_decision_ms": round((time.perf_counter() - signal_started) * 1000.0, 1),
        }
        self.pending_entry_intent = intent
        self.last_selected_position = intent["selected_position"]
        self._write_order_intent(intent)
        self._sync_state()
        self._submit_live_intent(intent)
        return intent

    def _quote_exact_contract(self, contract_data: dict[str, Any]) -> OptionContract:
        iid = str(contract_data.get("instrument_id") or "")
        if not iid:
            raise RuntimeError("Tracked option has no Robinhood instrument ID")
        payload = _data(self.client.call_tool(
            "get_option_quotes", {"instrument_ids": [iid]}, timeout=15.0
        ))
        results = payload.get("results") or []
        if not results:
            raise RuntimeError("Robinhood returned no quote for the tracked option")
        item = results[0]
        q = item.get("quote") if isinstance(item, dict) else item
        if not isinstance(q, dict):
            q = {}
        greeks = q.get("greeks") if isinstance(q.get("greeks"), dict) else {}
        c = dict(contract_data)
        c.update({
            "bid": float(_f(q.get("bid_price"), 0.0) or 0.0),
            "ask": float(_f(q.get("ask_price"), 0.0) or 0.0),
            "mark": _f(q.get("mark_price"), None),
            "bid_size": _i(q.get("bid_size"), 0),
            "ask_size": _i(q.get("ask_size"), 0),
            "volume": _i(q.get("volume"), 0),
            "open_interest": _i(q.get("open_interest"), 0),
            "delta": _f(q.get("delta"), _f(greeks.get("delta"), c.get("delta"))),
            "gamma": _f(q.get("gamma"), _f(greeks.get("gamma"), c.get("gamma"))),
            "theta": _f(q.get("theta"), _f(greeks.get("theta"), c.get("theta"))),
            "iv": _f(q.get("implied_volatility"), c.get("iv")),
            "low_fill_rate_buy_price": _f(q.get("low_fill_rate_buy_price"), None),
            "high_fill_rate_buy_price": _f(q.get("high_fill_rate_buy_price"), None),
            "low_fill_rate_sell_price": _f(q.get("low_fill_rate_sell_price"), None),
            "high_fill_rate_sell_price": _f(q.get("high_fill_rate_sell_price"), None),
            "updated_at": q.get("updated_at"),
        })
        return OptionContract.model_validate(c)

    def _prepare_exit_intent(self, signal: TradeSignal, *, trigger: str) -> dict[str, Any]:
        if not self.tracked_position:
            raise RuntimeError("No confirmed live position is tracked")
        started = time.perf_counter()
        contract = self._quote_exact_contract(self.tracked_position["contract"])
        payload, review = self._review_intent(
            contract=contract, qty=int(self.tracked_position["qty"]), action="SELL"
        )
        intent = {
            "intent_id": f"exit-{self.active_strategy.lower()}-{int(time.time() * 1000)}",
            "strategy": self.active_strategy,
            "kind": "EXIT",
            "created_at": datetime.now().isoformat(timespec="milliseconds"),
            "trigger": trigger,
            "signal": signal.model_dump(mode="json"),
            "contract": contract.model_dump(mode="json"),
            "qty": int(self.tracked_position["qty"]),
            "mcp_order_payload": payload,
            "broker_review": review,
            "final_live_submit_performed": False,
            "exit_reprice_attempts": 0,
            "reprice_history": [],
            "exit_reaction_ms": round((time.perf_counter() - started) * 1000.0, 1),
        }
        self.pending_exit_intent = intent
        try:
            self._write_order_intent(intent)
        except Exception as exc:
            log.warning("Could not persist prepared EXIT intent; continuing risk-reducing submit: %s", exc)
            self._set_state(last_error=f"EXIT persistence warning: {exc}")
        self._sync_state()
        self._submit_live_intent(intent)
        return intent

    # ---------------- signals / watchdog ----------------

    def _retry_deferred_exit_signal(self, strategy: str | None = None) -> bool:
        """Retry an already-received risk-reducing EXIT after transient prep failure.

        TradingView webhooks are acknowledged immediately. If a later Robinhood
        quote/review step fails, TradingView will not know to resend that EXIT. Keep
        the signal locally and retry at a bounded cadence while the strategy-owned
        position remains open. Ambiguous/actually-submitted orders are never retried.
        """
        if strategy is None and self.multi_strategy_enabled:
            original = self.active_strategy
            fired = False
            for name in self.strategy_names:
                self._activate_strategy(name)
                fired = self._retry_deferred_exit_signal(strategy=name) or fired
            self._activate_strategy(original)
            if fired:
                self._sync_state()
            return fired
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        signal = self._deferred_exit_signal
        if not signal or not self.tracked_position:
            return False
        if self.pending_exit_intent or (self.registered_order and str(self.registered_order.get("kind") or "").upper() == "EXIT"):
            return False
        if self.registered_order and str(self.registered_order.get("kind") or "").upper() == "ENTRY":
            return False
        now_mono = time.monotonic()
        if now_mono - self._last_deferred_exit_retry < self.deferred_exit_retry_seconds:
            return False
        self._last_deferred_exit_retry = now_mono
        try:
            intent = self._prepare_exit_intent(signal, trigger="DEFERRED_EXIT_RETRY")
        except Exception as exc:
            # If place_option_order became ambiguous, a pending intent now exists;
            # leave it untouched and never duplicate. Pure quote/review failures keep
            # the deferred signal so the next bounded retry can try again.
            self._set_state(last_error=f"{self.active_strategy} deferred EXIT retry failed: {exc}")
            log.warning("%s deferred EXIT retry failed: %s", self.active_strategy, exc)
            return False
        self._deferred_exit_signal = None
        self._write_last_decision({
            "ok": True, "event": "EXIT", "strategy": self.active_strategy,
            "trigger": "DEFERRED_EXIT_RETRY", "intent_id": intent.get("intent_id"),
            "live_order_id": intent.get("live_order_id"),
            "final_live_submit_performed": bool(intent.get("final_live_submit_performed")),
        })
        return True

    def _handle_signal(self, raw: str, *, received_at: datetime | None = None) -> dict[str, Any]:
        signal_started = time.perf_counter()
        if is_non_actionable_tradingview_alert(raw):
            result = {
                "ok": True,
                "ignored": True,
                "event": "SETUP_ONLY",
                "reason": "non-actionable TradingView setup/ARM/STOP_DAY message ignored",
                "raw": raw.strip(),
            }
            self._write_last_decision(result)
            return result
        signal = parse_tradingview_alert(raw)
        source = str(signal.source or "MAIN").upper()
        if source not in self.strategy_runtime:
            result = {"ok": False, "event": signal.event, "strategy": source, "reason": "unknown or disabled strategy source"}
            self._set_state(last_decision=result)
            return result
        self._activate_strategy(source)

        digest = hashlib.sha256(raw.strip().encode("utf-8", errors="replace")).hexdigest()
        now_epoch = time.time()
        self._recent_signal_hashes = {
            k: t for k, t in self._recent_signal_hashes.items()
            if now_epoch - t <= self.signal_dedupe_seconds
        }
        if digest in self._recent_signal_hashes:
            result = {"ok": True, "strategy": source, "duplicate": True, "message": "duplicate TradingView alert ignored"}
            self._write_last_decision(result)
            return result
        self._recent_signal_hashes[digest] = now_epoch
        self._persist_recent_signal_hashes()

        if received_at is not None:
            signal.received_at = received_at
        signal_payload = signal.model_dump(mode="json")
        self._runtime()["last_signal"] = signal_payload
        self._set_state(last_signal=signal_payload)

        if signal.event == "STOP_DAY":
            self.risk.on_stop_day()
            if not self.multi_strategy_enabled:
                # Preserve the v0.6 one-strategy shutdown behavior for legacy
                # configs/tests. v0.7's packaged config is multi-strategy and
                # therefore treats STOP_DAY as source-specific.
                if not self.tracked_position:
                    if self.registered_order and self.registered_order.get("kind") == "ENTRY":
                        self._shutdown_after_pending_order = True
                        self._set_state(stop_after_pending_order=True)
                        self._request_cancel_registered_order(reason="STOP_DAY received while ENTRY order was pending")
                        result = {
                            "ok": True, "strategy": source, "event": "STOP_DAY",
                            "auto_shutdown": "waiting_for_pending_entry_order",
                        }
                    else:
                        self.pending_entry_intent = None
                        self.pending_exit_intent = None
                        self.registered_order = None
                        self._remove_registered_order_file()
                        self._remove_order_intent_file()
                        self._request_shutdown("STOP TRADING QQQ received while flat")
                        result = {"ok": True, "strategy": source, "event": "STOP_DAY", "auto_shutdown": "requested_now"}
                else:
                    self._shutdown_after_exit = True
                    self._set_state(stop_after_exit=True)
                    if self.auto_submit_live_orders and not self.pending_exit_intent:
                        stop_signal = TradeSignal(
                            source=source,
                            event="EXIT",
                            exit_reason="STOP TRADING QQQ",
                            raw="STOP_DAY_LIVE_POSITION",
                        )
                        intent = self._prepare_exit_intent(stop_signal, trigger="STOP_DAY")
                        result = {
                            "ok": True, "strategy": source, "event": "STOP_DAY",
                            "auto_shutdown": "armed_after_live_exit_fill",
                            "exit_order_id": intent.get("live_order_id"),
                            "final_live_submit_performed": bool(intent.get("final_live_submit_performed")),
                        }
                    else:
                        result = {"ok": True, "strategy": source, "event": "STOP_DAY", "auto_shutdown": "armed_after_live_exit_fill"}
                self._write_last_decision(result)
                return result

            # Multi-strategy: STOP_DAY stops only the source that sent it. MAIN
            # can be done for the day while SCALPER keeps trading, or vice versa.
            if self.registered_order and self.registered_order.get("kind") == "ENTRY":
                cancel_requested = self._request_cancel_registered_order(
                    reason=f"{source} STOP_DAY received while ENTRY order was pending"
                )
                result = {
                    "ok": True,
                    "strategy": source,
                    "event": "STOP_DAY",
                    "strategy_stopped_for_day": True,
                    "pending_entry_order": True,
                    "cancel_requested": cancel_requested,
                    "message": f"{source} stopped for the day; pending entry cancellation requested.",
                }
            elif self.tracked_position:
                if self.pending_exit_intent:
                    result = {
                        "ok": True,
                        "strategy": source,
                        "event": "STOP_DAY",
                        "strategy_stopped_for_day": True,
                        "exit_already_pending": True,
                    }
                else:
                    stop_signal = TradeSignal(
                        source=source,
                        event="EXIT",
                        exit_reason=f"{source} STOP_DAY",
                        raw=f"SOURCE {source} | STOP_DAY_LIVE_POSITION",
                    )
                    intent = self._prepare_exit_intent(stop_signal, trigger="STOP_DAY")
                    result = {
                        "ok": True,
                        "strategy": source,
                        "event": "STOP_DAY",
                        "strategy_stopped_for_day": True,
                        "exit_order_id": intent.get("live_order_id"),
                        "final_live_submit_performed": bool(intent.get("final_live_submit_performed")),
                    }
            else:
                # Reviewed-but-not-submitted local intents are safe to discard.
                if self.pending_entry_intent and not bool(self.pending_entry_intent.get("final_live_submit_performed")):
                    self.pending_entry_intent = None
                    self._remove_order_intent_file()
                result = {
                    "ok": True,
                    "strategy": source,
                    "event": "STOP_DAY",
                    "strategy_stopped_for_day": True,
                    "message": f"{source} will reject new entries until BOT restart.",
                }
            self._write_last_decision(result)
            self._sync_state()
            return result

        if signal.event == "EXIT":
            if self.pending_exit_intent:
                result = {
                    "ok": True,
                    "strategy": source,
                    "event": "EXIT",
                    "duplicate_or_pending": True,
                    "intent_id": self.pending_exit_intent.get("intent_id"),
                    "message": f"{source} already has an exit intent pending.",
                }
                self._write_last_decision(result)
                return result
            if self.registered_order and self.registered_order.get("kind") == "ENTRY":
                self._deferred_exit_signal = signal
                cancel_requested = self._request_cancel_registered_order(
                    reason=f"{source} TradingView EXIT arrived before the live ENTRY order finished"
                )
                result = {
                    "ok": True,
                    "strategy": source,
                    "event": "EXIT",
                    "entry_order_pending": True,
                    "entry_order_id": self.registered_order.get("order_id") if self.registered_order else None,
                    "cancel_requested": cancel_requested,
                    "message": "EXIT arrived before entry fill; cancellation requested and any cancel-race fill will be closed immediately.",
                }
                self._write_last_decision(result)
                return result
            if not self.tracked_position:
                result = {
                    "ok": True,
                    "strategy": source,
                    "event": "EXIT",
                    "ignored": True,
                    "message": f"{source} has no confirmed live option lot to close.",
                }
                self._write_last_decision(result)
                return result
            try:
                intent = self._prepare_exit_intent(signal, trigger="TRADINGVIEW_EXIT")
            except Exception as exc:
                # The webhook has already been acknowledged to TradingView, so a
                # transient quote/review failure must not make the EXIT disappear.
                # If placement itself became ambiguous, _prepare_exit_intent leaves
                # a pending intent/manual-reconcile state and we must NOT retry it.
                if not self.pending_exit_intent:
                    self._deferred_exit_signal = signal
                    self._last_deferred_exit_retry = 0.0
                result = {
                    "ok": False, "strategy": source, "event": "EXIT",
                    "exit_retry_armed": bool(self._deferred_exit_signal and not self.pending_exit_intent),
                    "reason": f"EXIT could not be prepared/submitted immediately: {exc}",
                }
                self._set_state(last_error=result["reason"])
                self._write_last_decision(result)
                return result
            result = {
                "ok": True,
                "strategy": source,
                "event": "EXIT",
                "reviewed": True,
                "intent_id": intent["intent_id"],
                "option_id": intent["contract"].get("instrument_id"),
                "qty": intent["qty"],
                "limit_price": intent["mcp_order_payload"]["price"],
                "live_order_id": intent.get("live_order_id"),
                "final_live_submit_performed": bool(intent.get("final_live_submit_performed")),
                "message": (
                    f"{source} real Robinhood sell-to-close submitted and is being monitored."
                    if intent.get("final_live_submit_performed")
                    else f"{source} sell-to-close reviewed; auto-submit is disabled."
                ),
            }
            self._write_last_decision(result)
            return result

        # ENTRY ---------------------------------------------------------------
        if self.state.get("manual_reconcile_required"):
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": "manual live-position reconciliation is required before new entries"}
            self._write_last_decision(result)
            return result
        if self.tracked_position:
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": f"{source} already has a confirmed live position"}
            self._write_last_decision(result)
            return result
        if self.pending_entry_intent:
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": f"{source} already has a pending entry"}
            self._write_last_decision(result)
            return result

        occupied = self._strategy_open_count() + self._pending_entry_count(exclude=source)
        if occupied >= self.max_total_open_positions:
            result = {
                "ok": False,
                "strategy": source,
                "event": "ENTRY",
                "reason": f"global max of {self.max_total_open_positions} simultaneous strategy positions/orders reached",
            }
            self._write_last_decision(result)
            return result

        valid, valid_reason = self.risk.validate_entry_signal(signal)
        if not valid:
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": valid_reason}
            self._write_last_decision(result)
            return result

        fresh, reason = self.risk.signal_is_fresh(signal)
        if not fresh:
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": reason}
            self._write_last_decision(result)
            return result
        if self.risk.stop_for_day:
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": f"{source} already issued STOP_DAY"}
            self._write_last_decision(result)
            return result

        t_account = time.perf_counter()
        self._refresh_account(force=True)
        account_ms = (time.perf_counter() - t_account) * 1000.0
        t_positions = time.perf_counter()
        unmanaged = self._refresh_live_positions_state()
        positions_ms = (time.perf_counter() - t_positions) * 1000.0
        if unmanaged:
            result = {
                "ok": False,
                "strategy": source,
                "event": "ENTRY",
                "reason": "Agentic account has an unmanaged live option position/quantity not fully owned by BOT strategy lots",
                "unmanaged_positions": len(unmanaged),
            }
            self._write_last_decision(result)
            return result

        option_side = "CALL" if signal.direction == "LONG" else "PUT"
        t_quotes = time.perf_counter()
        share_price, contracts = self._quote_contracts(side=option_side)
        quote_path_ms = (time.perf_counter() - t_quotes) * 1000.0
        guard_ok, guard_reason, guard_metrics = self._underlying_signal_guard(signal, share_price)
        if not guard_ok:
            result = {
                "ok": False, "strategy": source, "event": "ENTRY", "reason": guard_reason,
                "share_price": share_price, "underlying_guard": guard_metrics,
            }
            self._write_last_decision(result)
            return result
        contracts = self._fresh_contracts(contracts)
        if not contracts:
            result = {
                "ok": False,
                "strategy": source,
                "event": "ENTRY",
                "reason": "No per-contract Robinhood option quote passed the freshness policy",
                "stale_quotes": self.state.get("last_quote_stale_count"),
                "missing_timestamps": self.state.get("last_quote_missing_timestamp_count"),
            }
            self._write_last_decision(result)
            return result

        buying_power = _f(self.state.get("buying_power"), 0.0) or 0.0
        portfolio_value = _f(self.state.get("portfolio_value"), None)
        reservation = self._pending_entry_reserved_dollars(exclude=source)
        race_safe_buying_power = max(0.0, buying_power - reservation)
        strategy_budget = self.risk.effective_trade_budget(race_safe_buying_power, portfolio_value)
        total_cap_remaining = max(0.0, self.max_total_committed_dollars - self._locally_committed_dollars(include_pending=True))
        budget = min(strategy_budget, total_cap_remaining)

        ranked = self.selector.rank_positions(
            signal, contracts, budget, today=_now_et().date(), current_underlying_price=share_price
        )

        # Bounded adaptive strike expansion: stay fast in the common case, but if
        # a small account cannot afford/find a usable contract in the initial
        # near-spot window, look farther out once instead of falsely reporting
        # that no valid 0DTE exists.
        initial_each_side = int(self.worker_cfg.get("window_strikes_each_side", 4))
        max_each_side = max(initial_each_side, int(self.worker_cfg.get("adaptive_window_max_each_side", initial_each_side)))
        expanded = False
        if not ranked and max_each_side > initial_each_side:
            expanded_started = time.perf_counter()
            share_price_2, expanded_contracts = self._quote_contracts(
                side=option_side, each_side_override=max_each_side
            )
            # If spot moved between quote calls, use the newest spot for remaining
            # target/stop distance and re-run the underlying validity guard.
            share_price = share_price_2
            guard_ok, guard_reason, guard_metrics = self._underlying_signal_guard(signal, share_price)
            if guard_ok:
                expanded_contracts = self._fresh_contracts(expanded_contracts)
                if expanded_contracts:
                    contracts = expanded_contracts
                    ranked = self.selector.rank_positions(
                        signal, contracts, budget, today=_now_et().date(), current_underlying_price=share_price
                    )
                    expanded = True
            quote_path_ms += (time.perf_counter() - expanded_started) * 1000.0

        self._write_selection_snapshot(
            signal=signal,
            share_price=share_price,
            budget=budget,
            contracts=contracts,
            ranked=ranked,
        )
        if not ranked:
            result = {
                "ok": False,
                "strategy": source,
                "event": "ENTRY",
                "reason": f"no 0DTE contract+quantity fits live ${budget:.2f} budget and execution gates",
                "share_price": share_price,
                "budget": round(budget, 2),
                "pending_reservation": round(reservation, 2),
            }
            self._write_last_decision(result)
            return result

        candidate = ranked[0]
        contract_selected_at = time.perf_counter()
        cutoff_ok, cutoff_reason = self._entry_cutoff_ok(candidate.contract)
        if not cutoff_ok:
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": cutoff_reason}
            self._write_last_decision(result)
            return result

        approved, why = self.risk.approve_budgeted_entry(signal, candidate, open_positions=0)
        if not approved:
            result = {"ok": False, "strategy": source, "event": "ENTRY", "reason": why}
            self._write_last_decision(result)
            return result

        t_submit = time.perf_counter()
        try:
            intent = self._prepare_entry_intent(
                signal,
                candidate,
                signal_started=signal_started,
                contract_selected_at=contract_selected_at,
            )
        except Exception as exc:
            result = {
                "ok": False,
                "strategy": source,
                "event": "ENTRY",
                "reason": f"ENTRY was not submitted after final review: {exc}",
            }
            self._set_state(last_error=result["reason"])
            self._write_last_decision(result)
            return result
        review_and_place_ms = (time.perf_counter() - t_submit) * 1000.0
        latency = {
            "account_refresh_ms": round(account_ms, 1),
            "position_reconcile_ms": round(positions_ms, 1),
            "quote_path_ms": round(quote_path_ms, 1),
            "review_and_place_ms": round(review_and_place_ms, 1),
            "total_signal_to_submit_ms": round((time.perf_counter() - signal_started) * 1000.0, 1),
        }
        self._set_state(last_entry_latency=latency)
        result = {
            "ok": True,
            "strategy": source,
            "event": "ENTRY",
            "reviewed": True,
            "intent_id": intent["intent_id"],
            "side": candidate.contract.side,
            "option_id": candidate.contract.instrument_id,
            "expiration": candidate.contract.expiration.isoformat(),
            "strike": candidate.contract.strike,
            "qty": candidate.qty,
            "capital_required_at_ask": candidate.capital_required,
            "limit_price": intent["mcp_order_payload"]["price"],
            "selector_score": round(candidate.score, 4),
            "quote_batch_ms": self.state.get("last_quote_batch_ms"),
            "adaptive_window_expanded": expanded,
            "signal_to_decision_ms": intent["signal_to_decision_ms"],
            "latency": latency,
            "underlying_guard": guard_metrics,
            "cutoff": cutoff_reason,
            "live_order_id": intent.get("live_order_id"),
            "final_live_submit_performed": bool(intent.get("final_live_submit_performed")),
            "message": (
                f"{source} real Robinhood buy-to-open submitted and is being monitored."
                if intent.get("final_live_submit_performed")
                else f"{source} buy-to-open reviewed; auto-submit is disabled."
            ),
        }
        self._write_last_decision(result)
        return result

    def _arm_position_watchdog(self) -> str | None:
        if not self.tracked_position:
            return None
        self._position_opened_monotonic = time.monotonic()
        deadline = datetime.now() + timedelta(minutes=self.position_killswitch_minutes)
        rt = self._runtime()
        rt["killswitch_armed"] = True
        rt["killswitch_deadline"] = deadline.isoformat(timespec="seconds")
        self._set_state(killswitch_armed=True, killswitch_deadline=deadline.isoformat(timespec="seconds"))
        return deadline.isoformat(timespec="seconds")

    def _clear_position_watchdog(self) -> None:
        self._position_opened_monotonic = None
        rt = self._runtime()
        rt["killswitch_armed"] = False
        rt["killswitch_deadline"] = None
        self._set_state(killswitch_armed=False, killswitch_deadline=None)

    def _check_position_killswitch(self, strategy: str | None = None) -> bool:
        if strategy is None and self.multi_strategy_enabled:
            original = self.active_strategy
            fired = False
            for name in self.strategy_names:
                self._activate_strategy(name)
                fired = self._check_position_killswitch(strategy=name) or fired
            self._activate_strategy(original)
            if fired:
                self._sync_state()
            return fired
        if strategy is not None and str(strategy).upper() != self.active_strategy:
            self._activate_strategy(str(strategy).upper())
        if not self.tracked_position or self._position_opened_monotonic is None:
            return False
        if time.monotonic() - self._position_opened_monotonic < self.position_killswitch_minutes * 60.0:
            return False
        if self._deferred_exit_signal or self.pending_exit_intent or (self.registered_order and str(self.registered_order.get("kind") or "").upper() == "EXIT"):
            return False

        signal = TradeSignal(
            source=self.active_strategy,
            event="EXIT",
            exit_reason=f"{self.position_killswitch_minutes:g}-minute missing-EXIT failsafe",
            raw="POSITION_TIMEOUT",
        )
        try:
            intent = self._prepare_exit_intent(signal, trigger="POSITION_TIMEOUT")
        except Exception as exc:
            # A safety watchdog must never crash the worker. Leave the watchdog
            # armed so the next loop can retry a transient quote/review failure. If
            # placement is ambiguous, pending_exit_intent prevents duplicate submit.
            if not self.pending_exit_intent:
                self._deferred_exit_signal = signal
                self._last_deferred_exit_retry = 0.0
            self._set_state(last_error=f"{self.active_strategy} position-timeout EXIT failed; deferred retry armed: {exc}")
            log.warning("%s position-timeout EXIT failed: %s", self.active_strategy, exc)
            return False
        self._position_opened_monotonic = None  # fire once; position stays open until real fill
        event = {
            "trigger": "POSITION_TIMEOUT",
            "fired_at": datetime.now().isoformat(timespec="milliseconds"),
            "intent_id": intent["intent_id"],
            "live_order_id": intent.get("live_order_id"),
            "final_live_submit_performed": bool(intent.get("final_live_submit_performed")),
        }
        rt = self._runtime()
        rt["last_killswitch"] = event
        rt["killswitch_armed"] = False
        rt["killswitch_deadline"] = None
        self._set_state(last_killswitch=event, killswitch_armed=False, killswitch_deadline=None)
        self._write_last_decision({
            "ok": True,
            "event": "EXIT",
            "trigger": "POSITION_TIMEOUT",
            "intent_id": intent["intent_id"],
            "reviewed": True,
            "live_order_id": intent.get("live_order_id"),
            "final_live_submit_performed": bool(intent.get("final_live_submit_performed")),
        })
        return True

    def _has_live_exit_in_flight(self) -> bool:
        return bool(
            self.pending_exit_intent
            or (self.registered_order and str(self.registered_order.get("kind") or "").upper() == "EXIT")
        )

    def _check_underlying_hard_stop_watchdog(self) -> bool:
        """Independent catastrophe stop using the original TradingView QQQ stop.

        TradingView remains responsible for dynamic/custom management. This only
        protects a live BOT-owned option lot if the alert/tunnel path dies. One QQQ
        quote is shared across all strategies and the check is throttled.
        """
        now_mono = time.monotonic()
        if self.underlying_stop_watchdog_seconds <= 0:
            return False
        if now_mono - self._last_underlying_stop_watchdog < self.underlying_stop_watchdog_seconds:
            return False
        self._last_underlying_stop_watchdog = now_mono
        self._save_active_strategy()
        exposed = [name for name, rt in self.strategy_runtime.items() if isinstance(rt.get("tracked_position"), dict)]
        if not exposed:
            return False
        try:
            original = self.active_strategy
            strict_age = min(float(self.strategy_worker_cfg[n].get("max_underlying_quote_age_seconds", 3.0)) for n in exposed)
            # _current_qqq_price reads the active profile, so activate a strategy
            # then temporarily apply the strictest watchdog age across open lots.
            self._activate_strategy(exposed[0])
            saved_age = self.worker_cfg.get("max_underlying_quote_age_seconds")
            self.worker_cfg["max_underlying_quote_age_seconds"] = strict_age
            saved_require_ts = self.worker_cfg.get("require_underlying_quote_timestamp")
            strict_require_ts = any(bool(self.strategy_worker_cfg[n].get("require_underlying_quote_timestamp", False)) for n in exposed)
            self.worker_cfg["require_underlying_quote_timestamp"] = strict_require_ts
            try:
                qqq = self._current_qqq_quote()
            finally:
                if saved_age is None:
                    self.worker_cfg.pop("max_underlying_quote_age_seconds", None)
                else:
                    self.worker_cfg["max_underlying_quote_age_seconds"] = saved_age
                if saved_require_ts is None:
                    self.worker_cfg.pop("require_underlying_quote_timestamp", None)
                else:
                    self.worker_cfg["require_underlying_quote_timestamp"] = saved_require_ts
            fired = False
            for name in exposed:
                self._activate_strategy(name)
                tp = self.tracked_position or {}
                if self._has_live_exit_in_flight():
                    continue
                sig = tp.get("signal") if isinstance(tp.get("signal"), dict) else {}
                stop = _f(sig.get("stop"), None)
                direction = str(tp.get("direction") or sig.get("direction") or "").upper()
                if stop is None:
                    continue
                if direction == "LONG":
                    spot = float(qqq.get("bid") or qqq["reference"])
                    basis = "BID" if qqq.get("bid") else "REFERENCE"
                    breached = spot <= stop
                elif direction == "SHORT":
                    spot = float(qqq.get("ask") or qqq["reference"])
                    basis = "ASK" if qqq.get("ask") else "REFERENCE"
                    breached = spot >= stop
                else:
                    continue
                if not breached:
                    continue
                signal = TradeSignal(
                    source=name, event="EXIT", exit_price=spot,
                    exit_reason="BOT backup underlying hard stop", raw="UNDERLYING_HARD_STOP"
                )
                intent = self._prepare_exit_intent(signal, trigger="UNDERLYING_HARD_STOP")
                event = {
                    "strategy": name, "spot": spot, "spot_basis": basis, "stop": stop, "direction": direction,
                    "quote_updated_at": qqq.get("updated_at"), "quote_age_seconds": qqq.get("age_seconds"),
                    "fired_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                    "live_order_id": intent.get("live_order_id"),
                }
                self._runtime()["last_underlying_stop_watchdog"] = event
                self._set_state(last_underlying_stop_watchdog=event)
                self._write_last_decision({"ok": True, "event": "EXIT", "trigger": "UNDERLYING_HARD_STOP", **event})
                fired = True
            self._activate_strategy(original)
            return fired
        except Exception as exc:
            self._set_state(last_error=f"Underlying hard-stop watchdog quote/check failed while exposure exists: {exc}")
            log.warning("Underlying hard-stop watchdog failed: %s", exc)
            return False

    def _live_exposure_summary(self) -> list[str]:
        self._save_active_strategy()
        reasons: list[str] = []
        unmanaged = self.state.get("unmanaged_live_positions") or []
        if unmanaged:
            reasons.append(f"broker reports {len(unmanaged)} unmanaged/mismatched live option position(s)")
        for name, rt in self.strategy_runtime.items():
            tp = rt.get("tracked_position")
            reg = rt.get("registered_order")
            intent = rt.get("pending_exit_intent") or rt.get("pending_entry_intent")
            if isinstance(tp, dict):
                reasons.append(f"{name} tracked position qty={tp.get('qty')}")
            if isinstance(reg, dict):
                reasons.append(f"{name} live {reg.get('kind')} order {reg.get('order_id')}")
            elif isinstance(intent, dict) and bool(intent.get("final_live_submit_performed")):
                reasons.append(f"{name} submitted {intent.get('kind')} intent with unresolved broker state")
        return reasons

    # ---------------- state / files ----------------

    def _persist_recent_signal_hashes(self) -> None:
        try:
            tmp = self.recent_signal_path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(self._recent_signal_hashes, indent=2), encoding="utf-8")
            tmp.replace(self.recent_signal_path)
        except Exception as exc:
            log.warning("Could not persist signal dedupe state: %s", exc)

    def _set_state(self, **updates: Any) -> None:
        with self.lock:
            self.state.update(updates)

    def _sync_state(self) -> None:
        self._save_active_strategy()
        strategy_public: dict[str, dict[str, Any]] = {}
        tracked_positions: dict[str, Any] = {}
        registered_orders: dict[str, Any] = {}
        pending_entries: dict[str, Any] = {}
        pending_exits: dict[str, Any] = {}
        any_killswitch = False
        deadlines: list[str] = []

        for name in self.strategy_names:
            rt = self.strategy_runtime[name]
            pos = rt.get("tracked_position")
            reg = rt.get("registered_order")
            pe = rt.get("pending_entry_intent")
            px = rt.get("pending_exit_intent")
            if pos:
                tracked_positions[name] = pos
            if reg:
                registered_orders[name] = reg
            if pe:
                pending_entries[name] = self._public_intent(pe)
            if px:
                pending_exits[name] = self._public_intent(px)
            armed = bool(rt.get("killswitch_armed"))
            deadline = rt.get("killswitch_deadline")
            any_killswitch = any_killswitch or armed
            if armed and deadline:
                deadlines.append(str(deadline))
            strategy_public[name] = {
                "enabled": True,
                "stopped_for_day": bool(self.strategy_risks[name].stop_for_day),
                "pending_entry_intent": self._public_intent(pe),
                "pending_exit_intent": self._public_intent(px),
                "registered_order": reg,
                "tracked_position": pos,
                "killswitch_armed": armed,
                "killswitch_deadline": deadline,
                "last_killswitch": rt.get("last_killswitch"),
                "last_signal": rt.get("last_signal"),
                "last_decision": rt.get("last_decision"),
                "entry_timeout_seconds": float(self.strategy_worker_cfg[name].get("live_entry_order_timeout_seconds", 12.0)),
                "position_killswitch_minutes": float(self.strategy_worker_cfg[name].get("position_killswitch_minutes", 25.0)),
                "max_quote_age_seconds": float(self.strategy_execution_cfg[name].get("max_quote_age_seconds", 3.0)),
                "max_trade_budget_dollars": float(self.strategy_execution_cfg[name].get("max_trade_budget_dollars", self.base_execution_cfg.get("max_trade_budget_dollars", 200.0))),
            }

        legacy_rt = self.strategy_runtime.get("MAIN") or self.strategy_runtime[self.strategy_names[0]]
        legacy_position = legacy_rt.get("tracked_position")
        if not legacy_position and len(tracked_positions) == 1:
            legacy_position = next(iter(tracked_positions.values()))

        try:
            stats = self.trade_journal.stats()
        except OSError as exc:
            # Journal/dashboard metadata must never crash the live trading worker.
            log.warning("Trade-journal stats refresh unavailable: %s", exc)
            stats = self.state.get("journal_stats") or {}
        self._set_state(
            strategies=strategy_public,
            pending_entry_intent=self._public_intent(legacy_rt.get("pending_entry_intent")),
            pending_exit_intent=self._public_intent(legacy_rt.get("pending_exit_intent")),
            pending_entry_intents=pending_entries,
            pending_exit_intents=pending_exits,
            registered_order=legacy_rt.get("registered_order"),
            registered_orders=registered_orders,
            tracked_position=legacy_position,
            tracked_positions=tracked_positions,
            open_positions_count=len(tracked_positions),
            killswitch_armed=any_killswitch,
            killswitch_deadline=min(deadlines) if deadlines else None,
            journal_open_trade=self.trade_journal.load_open_trade(),
            journal_open_trades=self.trade_journal.load_open_trades(),
            journal_stats=stats,
        )

    def _public_intent(self, intent: dict[str, Any] | None) -> dict[str, Any] | None:
        if not intent:
            return None
        out = json.loads(json.dumps(intent, default=str))
        for key in ("mcp_order_payload", "mcp_place_payload"):
            payload = out.get(key)
            if isinstance(payload, dict) and payload.get("account_number"):
                payload["account_number"] = f"****{str(payload['account_number'])[-4:]}"
        return out

    def _write_order_intent(self, intent: dict[str, Any]) -> None:
        # Full account number is kept only in this local file because an external
        # submitter needs the exact Robinhood payload. Never upload the state dir.
        tmp = self.intent_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(intent, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.intent_path)

    def _remove_order_intent_file(self) -> None:
        try:
            self.intent_path.unlink(missing_ok=True)
        except Exception:
            pass

    def _write_registered_order(self) -> None:
        if not self.registered_order:
            return
        tmp = self.registered_order_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(self.registered_order, indent=2, default=str), encoding="utf-8")
        tmp.replace(self.registered_order_path)

    def _remove_registered_order_file(self) -> None:
        try:
            self.registered_order_path.unlink(missing_ok=True)
        except Exception:
            pass

    def _write_learning_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if not bool(self.learning_cfg.get("enabled", True)):
            return
        row = {
            "schema_version": 1,
            "captured_at_utc": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "event_type": str(event_type),
            "strategy": self.active_strategy,
            "shadow_only": True,
            "payload": payload,
        }
        try:
            with self.learning_events_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(row, default=str) + "\n")
        except Exception as exc:
            log.warning("Could not write learning event: %s", exc)

    def _write_last_decision(self, decision: dict[str, Any]) -> None:
        payload = dict(decision)
        payload.setdefault("strategy", self.active_strategy)
        payload.setdefault("decided_at_utc", datetime.now(timezone.utc).isoformat(timespec="milliseconds"))
        self._runtime()["last_decision"] = payload
        self._set_state(last_decision=payload)
        try:
            (self.state_dir / "last_decision.json").write_text(
                json.dumps(payload, indent=2, default=str), encoding="utf-8"
            )
            (self.state_dir / f"last_decision_{self.active_strategy}.json").write_text(
                json.dumps(payload, indent=2, default=str), encoding="utf-8"
            )
            signal = self._runtime().get("last_signal") or {}
            audit = {
                "decided_at_utc": payload.get("decided_at_utc"),
                "strategy": payload.get("strategy"),
                "event": payload.get("event"),
                "ok": payload.get("ok"),
                "ignored": payload.get("ignored", False),
                "reason": payload.get("reason") or payload.get("message"),
                "order_id": payload.get("live_order_id") or payload.get("exit_order_id"),
                "signal_raw": payload.get("raw") or (signal.get("raw") if isinstance(signal, dict) else None),
                "decision": payload,
            }
            with self.signal_audit_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(audit, default=str) + "\n")
            self._write_learning_event("DECISION", audit)
        except Exception:
            pass
        self._sync_state()

    def _refresh_journal_state(self) -> None:
        self._set_state(
            journal_open_trade=self.trade_journal.load_open_trade(),
            journal_open_trades=self.trade_journal.load_open_trades(),
            journal_stats=self.trade_journal.stats(),
        )

    def _request_shutdown(self, reason: str) -> dict[str, Any]:
        exposure = self._live_exposure_summary()
        if exposure:
            message = "Shutdown blocked while live exposure/order state exists: " + "; ".join(exposure)
            self._set_state(
                shutdown_requested=False, shutdown_blocked=True, shutdown_block_reason=message,
                shutdown_reason=reason, last_error=message,
            )
            self._sync_state()
            log.warning(message)
            return {"ok": False, "shutdown_requested": False, "blocked": True, "reason": message}
        self._set_state(
            shutdown_requested=True, shutdown_blocked=False, shutdown_block_reason=None,
            shutdown_reason=reason, stop_after_exit=False, stop_after_pending_order=False,
        )
        self.shutdown_requested_event.set()
        self.stop_event.set()
        log.info("Graceful shutdown requested: %s", reason)
        return {"ok": True, "shutdown_requested": True, "blocked": False}
