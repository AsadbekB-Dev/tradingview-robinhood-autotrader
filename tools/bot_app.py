from __future__ import annotations

import json
import os
import queue
import re
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
import webbrowser
from datetime import datetime
from pathlib import Path
from typing import Any

import tkinter as tk
from tkinter import messagebox, ttk

ROOT = Path(__file__).resolve().parents[1]
STATE_DIR = ROOT / "state"
TOOLS_DIR = ROOT / "tools"
WORKER_SCRIPT = TOOLS_DIR / "run_persistent_worker.py"
CLOUDFLARED = TOOLS_DIR / "cloudflared.exe"
CONFIG_PATH = ROOT / "config.yaml"
PUBLIC_URL_FILE = STATE_DIR / "cloudflare_public_url.txt"
WEBHOOK_URL_FILE = STATE_DIR / "tradingview_webhook_url.txt"
SECRET_FILE = STATE_DIR / "webhook_secret.txt"
CLOUDFLARE_LOG = STATE_DIR / "logs" / "cloudflared.log"
TUNNEL_TOKEN_FILE = STATE_DIR / "cloudflare_tunnel_token.txt"
LOCAL_BASE = "http://127.0.0.1:8787"
QUICK_URL_RE = re.compile(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com")
CREATE_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


def load_yaml() -> dict[str, Any]:
    try:
        import yaml
    except Exception:
        return {}
    if not CONFIG_PATH.exists():
        return {}
    try:
        return yaml.safe_load(CONFIG_PATH.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def http_json(path: str, timeout: float = 1.0) -> dict[str, Any] | None:
    try:
        req = urllib.request.Request(f"{LOCAL_BASE}{path}", headers={"User-Agent": "AsadsDashboard/0.7"})
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="replace")
        parsed = json.loads(data)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    except Exception:
        return None


def http_post(path: str, timeout: float = 1.0) -> bool:
    try:
        req = urllib.request.Request(
            f"{LOCAL_BASE}{path}",
            data=b"{}",
            method="POST",
            headers={"User-Agent": "AsadsDashboard/0.7", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= int(resp.status) < 300
    except Exception:
        return False


def http_post_json(path: str, timeout: float = 1.0) -> dict[str, Any] | None:
    try:
        req = urllib.request.Request(
            f"{LOCAL_BASE}{path}",
            data=b"{}", method="POST",
            headers={"User-Agent": "AsadsDashboard/0.7.8-hardened", "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            data = resp.read().decode("utf-8", errors="replace")
        parsed = json.loads(data)
        return parsed if isinstance(parsed, dict) else {"value": parsed}
    except Exception:
        return None


def mask_url(url: str | None) -> str:
    if not url:
        return ""
    return re.sub(r"([?&]secret=)[^&]+", r"\1••••••••", url)


def short_text(value: Any, limit: int = 140) -> str:
    if value is None:
        return "—"
    if isinstance(value, (dict, list)):
        value = json.dumps(value, default=str, separators=(",", ":"))
    text = str(value).replace("\n", " ")
    return text if len(text) <= limit else text[: limit - 1] + "…"


class BotController:
    def __init__(self, events: queue.Queue[tuple[str, Any]]) -> None:
        self.events = events
        self.cfg = load_yaml()
        self.worker: subprocess.Popen[str] | None = None
        self.tunnel: subprocess.Popen[str] | None = None
        self.stop_event = threading.Event()
        self.webhook_url: str | None = None
        self.public_url: str | None = None
        self.previous_public_url: str | None = None
        self.tunnel_mode = str(self.cfg.get("tunnel", {}).get("mode", "quick")).strip().lower()
        self._reader_threads: list[threading.Thread] = []
        self._backup_done = False

    def emit(self, kind: str, payload: Any = None) -> None:
        self.events.put((kind, payload))

    def log(self, text: str) -> None:
        self.emit("log", text.rstrip())

    def _python(self) -> str:
        venv = ROOT / ".venv" / "Scripts" / "python.exe"
        if venv.exists():
            return str(venv)
        return sys.executable

    def _read_stream(self, proc: subprocess.Popen[str], prefix: str) -> None:
        if not proc.stdout:
            return
        try:
            for line in proc.stdout:
                if self.stop_event.is_set():
                    break
                line = line.rstrip("\r\n")
                if line:
                    self.log(f"[{prefix}] {line}")
        except Exception as exc:
            self.log(f"[{prefix}] log reader stopped: {exc}")

    def _spawn(self, args: list[str], prefix: str) -> subprocess.Popen[str]:
        proc = subprocess.Popen(
            args,
            cwd=str(ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            creationflags=CREATE_NO_WINDOW if os.name == "nt" else 0,
        )
        thread = threading.Thread(target=self._read_stream, args=(proc, prefix), daemon=True)
        thread.start()
        self._reader_threads.append(thread)
        return proc

    def start_async(self) -> None:
        threading.Thread(target=self._start, daemon=True).start()

    def _start(self) -> None:
        try:
            self.stop_event.clear()
            STATE_DIR.mkdir(parents=True, exist_ok=True)
            (STATE_DIR / "logs").mkdir(parents=True, exist_ok=True)
            self.emit("phase", "Starting Robinhood MCP worker…")

            existing = http_json("/health", timeout=0.75)
            if existing:
                raise RuntimeError("Another BOT worker is already running on port 8787.")
            if not WORKER_SCRIPT.exists():
                raise RuntimeError(f"Worker script is missing: {WORKER_SCRIPT}")
            if not CLOUDFLARED.exists():
                raise RuntimeError("cloudflared.exe is missing. Run SETUP_BOT.cmd once.")

            self.worker = self._spawn([self._python(), "-u", str(WORKER_SCRIPT)], "WORKER")
            startup_timeout = float(self.cfg.get("robinhood_mcp", {}).get("startup_timeout_seconds", 300))
            deadline = time.monotonic() + startup_timeout + 15
            ready = False
            while time.monotonic() < deadline and not self.stop_event.is_set():
                if self.worker.poll() is not None:
                    raise RuntimeError(f"Worker exited during startup (code {self.worker.returncode}).")
                health = http_json("/health", timeout=1.0)
                if health and health.get("ok"):
                    ready = True
                    break
                time.sleep(0.35)
            if not ready:
                raise RuntimeError("Robinhood MCP worker did not become ready before the startup timeout.")

            secret_deadline = time.monotonic() + 10
            while time.monotonic() < secret_deadline and not SECRET_FILE.exists():
                time.sleep(0.20)
            if not SECRET_FILE.exists():
                raise RuntimeError("The worker did not create state\\webhook_secret.txt.")
            secret = SECRET_FILE.read_text(encoding="utf-8").strip()
            if not secret:
                raise RuntimeError("Webhook secret file is empty.")

            if PUBLIC_URL_FILE.exists():
                self.previous_public_url = PUBLIC_URL_FILE.read_text(encoding="utf-8").strip() or None

            self.emit("phase", "Starting Cloudflare tunnel…")
            # Once the Robinhood worker is ready it may already be restoring or
            # managing a live position from disk. A tunnel startup failure is
            # therefore NOT controller-fatal: keep the worker alive and retry
            # inbound connectivity until Cloudflare recovers.
            while not self.stop_event.is_set() and self.worker and self.worker.poll() is None:
                try:
                    self.public_url = None
                    self._start_tunnel()
                    if not self.public_url:
                        raise RuntimeError("Cloudflare started but no public URL could be determined.")
                    break
                except Exception as exc:
                    self.log(f"[TUNNEL] Startup/recovery failed: {exc}; Robinhood worker remains alive.")
                    self.emit("tunnel_down", {"code": None, "startup": True, "error": str(exc)})
                    self._terminate(self.tunnel, graceful_seconds=1.0)
                    self.tunnel = None
                    self.public_url = None
                    time.sleep(2.0)

            if not self.public_url:
                if self.worker and self.worker.poll() is not None:
                    raise RuntimeError(f"Worker exited while Cloudflare was unavailable (code {self.worker.returncode}).")
                raise RuntimeError("BOT was stopped before Cloudflare connectivity became ready.")

            tunnel_verified = self._verify_public_tunnel()
            self.webhook_url = f"{self.public_url}/webhook/tradingview?secret={secret}"
            PUBLIC_URL_FILE.write_text(self.public_url, encoding="utf-8")
            WEBHOOK_URL_FILE.write_text(self.webhook_url, encoding="utf-8")
            self.emit("webhook", {
                "public_url": self.public_url,
                "masked_webhook": mask_url(self.webhook_url),
                "changed": bool(self.previous_public_url and self.previous_public_url != self.public_url),
                "first": not bool(self.previous_public_url),
                "mode": self.tunnel_mode,
                "verified": tunnel_verified,
            })
            self.emit("running", True)

            while not self.stop_event.is_set():
                if self.worker and self.worker.poll() is not None:
                    code = self.worker.returncode
                    self.log(f"[BOT] Worker exited (code {code}). Stopping tunnel.")
                    self.emit("child_exit", {"child": "worker", "code": code})
                    break
                if self.tunnel and self.tunnel.poll() is not None:
                    code = self.tunnel.returncode
                    self.log(f"[BOT] Cloudflare tunnel exited (code {code}). Robinhood worker stays alive; attempting tunnel recovery.")
                    self.emit("tunnel_down", {"code": code})
                    self.tunnel = None
                    # Inbound alerts are temporarily unavailable, but live position
                    # management must never die merely because cloudflared did.
                    while not self.stop_event.is_set() and self.worker and self.worker.poll() is None:
                        try:
                            time.sleep(2.0)
                            old_public = self.public_url
                            self._start_tunnel()
                            if not self.public_url:
                                raise RuntimeError("restarted tunnel produced no public URL")
                            verified = self._verify_public_tunnel()
                            secret = SECRET_FILE.read_text(encoding="utf-8").strip()
                            self.webhook_url = f"{self.public_url}/webhook/tradingview?secret={secret}"
                            PUBLIC_URL_FILE.write_text(self.public_url, encoding="utf-8")
                            WEBHOOK_URL_FILE.write_text(self.webhook_url, encoding="utf-8")
                            self.emit("webhook", {
                                "public_url": self.public_url,
                                "masked_webhook": mask_url(self.webhook_url),
                                "changed": bool(old_public and old_public != self.public_url),
                                "first": False,
                                "mode": self.tunnel_mode,
                                "verified": verified,
                                "recovered": True,
                            })
                            self.log("[BOT] Cloudflare tunnel recovered; Robinhood worker was never stopped.")
                            break
                        except Exception as exc:
                            self.log(f"[TUNNEL] Recovery attempt failed: {exc}; retrying while worker remains active.")
                            self.tunnel = None
                    continue
                time.sleep(0.5)
        except Exception as exc:
            self.emit("error", str(exc))
        finally:
            # Worker failure/startup failure is a controller-fatal condition. Normal
            # user shutdown goes through stop_async(), which first asks the worker
            # whether it is safe to stop.
            if not self.stop_event.is_set():
                self._stop_processes(force=True)
            self.emit("running", False)

    def _start_tunnel(self) -> None:
        CLOUDFLARE_LOG.parent.mkdir(parents=True, exist_ok=True)
        try:
            CLOUDFLARE_LOG.unlink(missing_ok=True)
        except Exception:
            pass

        tunnel_cfg = self.cfg.get("tunnel", {})
        mode = self.tunnel_mode
        if mode == "named":
            public_url = str(tunnel_cfg.get("public_url", "")).strip().rstrip("/")
            token_path = Path(str(tunnel_cfg.get("token_file", TUNNEL_TOKEN_FILE)))
            if not token_path.is_absolute():
                token_path = ROOT / token_path
            if not public_url:
                raise RuntimeError("Named tunnel mode requires tunnel.public_url in config.yaml.")
            if not token_path.exists():
                raise RuntimeError(f"Named tunnel token file is missing: {token_path}")
            self.tunnel = self._spawn([
                str(CLOUDFLARED), "tunnel", "--no-autoupdate", "run", "--token-file", str(token_path)
            ], "TUNNEL")
            time.sleep(1.0)
            if self.tunnel.poll() is not None:
                raise RuntimeError("Named Cloudflare tunnel exited during startup.")
            self.public_url = public_url
            return

        self.tunnel_mode = "quick"
        args = [
            str(CLOUDFLARED), "tunnel",
            "--url", LOCAL_BASE,
            "--no-autoupdate",
            "--loglevel", "info",
            "--logfile", str(CLOUDFLARE_LOG),
        ]
        self.tunnel = self._spawn(args, "TUNNEL")
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline and not self.stop_event.is_set():
            if self.tunnel.poll() is not None:
                raise RuntimeError(f"Cloudflare Quick Tunnel exited during startup (code {self.tunnel.returncode}).")
            if CLOUDFLARE_LOG.exists():
                text = CLOUDFLARE_LOG.read_text(encoding="utf-8", errors="replace")
                match = QUICK_URL_RE.search(text)
                if match:
                    self.public_url = match.group(0)
                    return
            time.sleep(0.20)
        raise RuntimeError("Could not detect the Cloudflare Quick Tunnel URL within 45 seconds.")

    def _verify_public_tunnel(self) -> bool:
        if not self.public_url:
            return False
        url = f"{self.public_url}/status"
        deadline = time.monotonic() + 10.0
        while time.monotonic() < deadline and not self.stop_event.is_set():
            try:
                urllib.request.urlopen(url, timeout=2.0)
                # /status should never be publicly readable. A 200 here would be
                # a security regression, so do not call that verified.
                self.log("[TUNNEL] Public /status unexpectedly returned success; expected 403.")
                return False
            except urllib.error.HTTPError as exc:
                if exc.code == 403:
                    self.log("[TUNNEL] Public route verified; local-only /status correctly returned 403.")
                    return True
            except Exception:
                pass
            time.sleep(0.5)
        self.log("[TUNNEL] Public route could not be verified yet; Cloudflare may still be propagating.")
        return False

    def copy_webhook(self, root: tk.Misc) -> bool:
        if not self.webhook_url:
            return False
        root.clipboard_clear()
        root.clipboard_append(self.webhook_url)
        root.update_idletasks()
        return True

    def stop_async(self) -> None:
        # Do not set stop_event until the worker confirms there is no live
        # position/order state. Otherwise the controller loop could tear down a
        # still-needed position manager before /control/shutdown has a say.
        threading.Thread(target=self._stop_processes, kwargs={"force": False}, daemon=False).start()

    def processes_stopped(self) -> bool:
        worker_stopped = self.worker is None or self.worker.poll() is not None
        tunnel_stopped = self.tunnel is None or self.tunnel.poll() is not None
        return worker_stopped and tunnel_stopped

    @staticmethod
    def _terminate(proc: subprocess.Popen[str] | None, graceful_seconds: float = 4.0) -> None:
        if not proc or proc.poll() is not None:
            return
        try:
            proc.terminate()
            proc.wait(timeout=graceful_seconds)
            return
        except Exception:
            pass
        try:
            proc.kill()
        except Exception:
            pass

    def _stop_processes(self, *, force: bool = False) -> bool:
        # User-requested shutdown is refused while any BOT-owned position, live
        # broker order, or unresolved submitted intent exists. Force is reserved
        # for controller-fatal startup/worker-death cleanup, not the STOP button.
        if self.worker and self.worker.poll() is None and not force:
            response = http_post_json("/control/shutdown", timeout=1.2)
            if not response or not bool(response.get("shutdown_requested")):
                reason = (response or {}).get("reason") or "Worker did not confirm a safe shutdown."
                self.log(f"[BOT] Shutdown blocked: {reason}")
                self.emit("shutdown_blocked", str(reason))
                return False
            self.stop_event.set()
            try:
                self.worker.wait(timeout=6.0)
            except Exception:
                pass
        elif force:
            self.stop_event.set()

        self._terminate(self.tunnel, graceful_seconds=2.0)
        self._terminate(self.worker, graceful_seconds=2.0)
        self._backup_nonsecret_data()
        return True

    def _backup_nonsecret_data(self) -> None:
        if self._backup_done or not bool(self.cfg.get("ui", {}).get("backup_on_shutdown", True)):
            return
        self._backup_done = True
        raw_dir = str(self.cfg.get("ui", {}).get("backup_directory", "")).strip()
        if not raw_dir:
            return
        raw_dir = os.path.expandvars(os.path.expanduser(raw_dir))
        target = Path(raw_dir) / datetime.now().strftime("%Y-%m-%d")
        try:
            target.mkdir(parents=True, exist_ok=True)
            journal = STATE_DIR / "trade_journal"
            if journal.exists():
                shutil.copytree(journal, target / "trade_journal", dirs_exist_ok=True)
            for name in ("selection_snapshots.jsonl", "signal_audit.jsonl", "learning_events.jsonl", "last_decision.json"):
                src = STATE_DIR / name
                if src.exists():
                    shutil.copy2(src, target / name)
            self.log(f"[BACKUP] Non-secret journal/selector data saved to {target}")
        except Exception as exc:
            self.log(f"[BACKUP] Backup failed: {exc}")


class BotApp(tk.Tk):
    BG = "#0b0f14"
    PANEL = "#111821"
    PANEL2 = "#16202b"
    TEXT = "#e6edf3"
    MUTED = "#8b98a5"
    GREEN = "#2fd17b"
    RED = "#ff5c64"
    YELLOW = "#f4c95d"
    BLUE = "#58a6ff"

    def __init__(self) -> None:
        super().__init__()
        self.title("Asads AUTOMATION v0.7.8 HARDENED")
        self.geometry("1120x760")
        self.minsize(960, 680)
        self.configure(bg=self.BG)
        self.protocol("WM_DELETE_WINDOW", self.on_close)
        self.events: queue.Queue[tuple[str, Any]] = queue.Queue()
        self.controller = BotController(self.events)
        self._closing = False
        self._running = False
        self._url_warning = False
        self._last_state: dict[str, Any] = {}
        self._build_style()
        self._build_ui()
        self.after(150, self._drain_events)
        self.after(800, self._poll_state)
        self.after(300, self.controller.start_async)

    def _build_style(self) -> None:
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("TFrame", background=self.BG)
        style.configure("Panel.TFrame", background=self.PANEL)
        style.configure("Panel2.TFrame", background=self.PANEL2)
        style.configure("TLabel", background=self.BG, foreground=self.TEXT, font=("Segoe UI", 10))
        style.configure("Title.TLabel", background=self.BG, foreground=self.TEXT, font=("Segoe UI Semibold", 22))
        style.configure("Sub.TLabel", background=self.BG, foreground=self.MUTED, font=("Segoe UI", 9))
        style.configure("CardTitle.TLabel", background=self.PANEL, foreground=self.MUTED, font=("Segoe UI Semibold", 9))
        style.configure("CardValue.TLabel", background=self.PANEL, foreground=self.TEXT, font=("Segoe UI Semibold", 15))
        style.configure("PanelLabel.TLabel", background=self.PANEL, foreground=self.TEXT, font=("Segoe UI", 10))
        style.configure("PanelMuted.TLabel", background=self.PANEL, foreground=self.MUTED, font=("Segoe UI", 9))
        style.configure("Action.TButton", font=("Segoe UI Semibold", 9), padding=(12, 8))
        style.configure("Danger.TButton", font=("Segoe UI Semibold", 9), padding=(12, 8))

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=(20, 16, 20, 8))
        top.pack(fill="x")
        ttk.Label(top, text="BOT", style="Title.TLabel").pack(side="left")
        auto_live = bool(self.controller.cfg.get("execution", {}).get("auto_submit_live_orders", False))
        mode_text = "FULL AUTO" if auto_live else "REVIEW ONLY"
        ttk.Label(top, text=f"QQQ 0DTE • MAIN + SCALPER • Robinhood MCP • {mode_text}", style="Sub.TLabel").pack(side="left", padx=(12, 0), pady=(10, 0))
        self.stop_btn = ttk.Button(top, text="STOP BOT", style="Danger.TButton", command=self.stop_bot)
        self.stop_btn.pack(side="right")
        self.copy_btn = ttk.Button(top, text="COPY WEBHOOK", style="Action.TButton", command=self.copy_webhook)
        self.copy_btn.pack(side="right", padx=(0, 8))
        ttk.Button(top, text="OPEN TRADINGVIEW", style="Action.TButton", command=self.open_tradingview).pack(side="right", padx=(0, 8))

        self.banner = tk.Label(self, text="Starting…", anchor="w", padx=20, pady=8,
                               bg=self.PANEL2, fg=self.YELLOW, font=("Segoe UI Semibold", 10))
        self.banner.pack(fill="x", padx=20, pady=(0, 10))

        cards = ttk.Frame(self, padding=(20, 0, 20, 10))
        cards.pack(fill="x")
        self.card_vars: dict[str, tk.StringVar] = {}
        specs = [
            ("bot", "BOT STATUS"), ("mcp", "ROBINHOOD MCP"), ("tunnel", "CLOUDFLARE"),
            ("webhook", "TRADINGVIEW"), ("bp", "BUYING POWER"), ("qqq", "QQQ"),
            ("dte", "0DTE"), ("position", "POSITIONS"),
        ]
        for idx, (key, title) in enumerate(specs):
            frame = ttk.Frame(cards, style="Panel.TFrame", padding=12)
            frame.grid(row=idx // 4, column=idx % 4, sticky="nsew", padx=5, pady=5)
            cards.grid_columnconfigure(idx % 4, weight=1)
            var = tk.StringVar(value="—")
            self.card_vars[key] = var
            ttk.Label(frame, text=title, style="CardTitle.TLabel").pack(anchor="w")
            ttk.Label(frame, textvariable=var, style="CardValue.TLabel").pack(anchor="w", pady=(3, 0))

        body = ttk.Frame(self, padding=(20, 0, 20, 12))
        body.pack(fill="both", expand=True)
        body.columnconfigure(0, weight=1)
        body.columnconfigure(1, weight=1)
        body.rowconfigure(1, weight=1)

        info = ttk.Frame(body, style="Panel.TFrame", padding=14)
        info.grid(row=0, column=0, sticky="nsew", padx=(5, 5), pady=5)
        stats = ttk.Frame(body, style="Panel.TFrame", padding=14)
        stats.grid(row=0, column=1, sticky="nsew", padx=(5, 5), pady=5)

        ttk.Label(info, text="LIVE FLOW", style="CardTitle.TLabel").pack(anchor="w")
        self.last_signal = tk.StringVar(value="Last signal: —")
        self.last_decision = tk.StringVar(value="Last decision: —")
        self.killswitch = tk.StringVar(value="Killswitch: —")
        self.intent = tk.StringVar(value="Pending intent: —")
        self.strategy_flow = tk.StringVar(value="Strategies: MAIN — • SCALPER —")
        for var in (self.last_signal, self.last_decision, self.intent, self.strategy_flow, self.killswitch):
            ttk.Label(info, textvariable=var, style="PanelLabel.TLabel", wraplength=510, justify="left").pack(anchor="w", pady=(6, 0))

        ttk.Label(stats, text="TODAY / ALL-TIME", style="CardTitle.TLabel").pack(anchor="w")
        self.stats_text = tk.StringVar(value="Waiting for journal stats…")
        ttk.Label(stats, textvariable=self.stats_text, style="PanelLabel.TLabel", justify="left").pack(anchor="w", pady=(8, 0))
        self.latency_text = tk.StringVar(value="Latency: —")
        ttk.Label(stats, textvariable=self.latency_text, style="PanelMuted.TLabel", justify="left").pack(anchor="w", pady=(8, 0))

        log_panel = ttk.Frame(body, style="Panel.TFrame", padding=10)
        log_panel.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=5, pady=(5, 0))
        log_head = ttk.Frame(log_panel, style="Panel.TFrame")
        log_head.pack(fill="x")
        ttk.Label(log_head, text="LIVE LOG", style="CardTitle.TLabel").pack(side="left")
        ttk.Button(log_head, text="OPEN JOURNAL", command=self.open_journal).pack(side="right")
        ttk.Button(log_head, text="DIAGNOSTICS", command=self.show_diagnostics).pack(side="right", padx=(0, 6))
        self.log_text = tk.Text(log_panel, bg="#080c10", fg="#c9d1d9", insertbackground=self.TEXT,
                                relief="flat", font=("Cascadia Mono", 9), height=14, wrap="word")
        self.log_text.pack(fill="both", expand=True, pady=(8, 0))
        self.log_text.configure(state="disabled")

    def append_log(self, line: str) -> None:
        self.log_text.configure(state="normal")
        stamp = datetime.now().strftime("%H:%M:%S")
        self.log_text.insert("end", f"{stamp}  {line}\n")
        lines = int(self.log_text.index("end-1c").split(".")[0])
        if lines > 1200:
            self.log_text.delete("1.0", "300.0")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _drain_events(self) -> None:
        try:
            while True:
                kind, payload = self.events.get_nowait()
                if kind == "log":
                    self.append_log(str(payload))
                elif kind == "phase":
                    self.banner.configure(text=str(payload), fg=self.YELLOW, bg=self.PANEL2)
                elif kind == "error":
                    self.banner.configure(text=f"STARTUP ERROR — {payload}", fg="white", bg="#7d1f28")
                    self.append_log(f"[ERROR] {payload}")
                    if not self._closing:
                        messagebox.showerror("BOT startup error", str(payload))
                elif kind == "webhook":
                    self._handle_webhook(payload or {})
                elif kind == "running":
                    self._running = bool(payload)
                    self.card_vars["bot"].set("RUNNING" if self._running else "STOPPED")
                    if not self._running and not self._closing and not self._url_warning:
                        self.banner.configure(text="BOT stopped.", fg=self.MUTED, bg=self.PANEL2)
                elif kind == "child_exit":
                    child = (payload or {}).get("child", "process")
                    code = int((payload or {}).get("code", -1))
                    if code == 0:
                        self.banner.configure(text=f"{child.upper()} STOPPED CLEANLY", fg=self.MUTED, bg=self.PANEL2)
                    else:
                        self.banner.configure(text=f"{child.upper()} STOPPED (code {code}) — BOT is shutting down", fg="white", bg="#7d1f28")
                elif kind == "tunnel_down":
                    self.card_vars["tunnel"].set("DOWN • RECOVERING")
                    self.banner.configure(
                        text="CLOUDFLARE DOWN — Robinhood position management is STILL ACTIVE; recovering tunnel…",
                        fg="#111111", bg=self.YELLOW,
                    )
                elif kind == "shutdown_blocked":
                    self._closing = False
                    self.banner.configure(text="STOP BLOCKED — live position/order still needs BOT management", fg="#111111", bg=self.YELLOW)
                    messagebox.showwarning("BOT is still managing exposure", str(payload))
        except queue.Empty:
            pass
        if not self._closing:
            self.after(120, self._drain_events)

    def _handle_webhook(self, data: dict[str, Any]) -> None:
        mode = str(data.get("mode", "quick")).upper()
        changed = bool(data.get("changed"))
        first = bool(data.get("first"))
        verified = bool(data.get("verified"))
        self.card_vars["tunnel"].set(f"{'VERIFIED' if verified else 'READY'} • {mode}")
        if mode == "NAMED":
            self._url_warning = False
            self.banner.configure(text="BOT READY — stable Cloudflare tunnel active", fg=self.GREEN, bg=self.PANEL2)
        elif changed or first:
            self._url_warning = True
            why = "changed" if changed else "created"
            self.banner.configure(
                text=f"TRADINGVIEW WEBHOOK {why.upper()} — click COPY WEBHOOK, paste it into BOTH TradingView alerts, then keep BOT open",
                fg="#111111", bg=self.YELLOW,
            )
            self.copy_webhook(silent=True)
            if bool(self.controller.cfg.get("ui", {}).get("open_tradingview_on_url_change", True)):
                self.after(250, self.open_tradingview)
        else:
            self._url_warning = False
            self.banner.configure(text="BOT READY — tunnel hostname matches the previous run", fg=self.GREEN, bg=self.PANEL2)

    def _poll_state(self) -> None:
        if self._closing:
            return
        state = http_json("/status", timeout=1.0)
        if state:
            self._last_state = state
            self._render_state(state)
        self.after(1500, self._poll_state)

    def _render_state(self, state: dict[str, Any]) -> None:
        self.card_vars["mcp"].set("CONNECTED" if state.get("mcp_connected") else "OFFLINE")
        bp = state.get("buying_power")
        self.card_vars["bp"].set(f"${float(bp):,.2f}" if isinstance(bp, (int, float)) else "—")
        qqq = state.get("share_price")
        self.card_vars["qqq"].set(f"${float(qqq):,.2f}" if isinstance(qqq, (int, float)) else "—")
        if not state.get("zero_dte_ready"):
            self.card_vars["dte"].set("NO 0DTE")
        elif state.get("session_entry_open", True):
            self.card_vars["dte"].set("READY")
        else:
            self.card_vars["dte"].set("ENTRY CLOSED")
        tracked_positions = state.get("tracked_positions") if isinstance(state.get("tracked_positions"), dict) else {}
        if tracked_positions:
            labels = []
            for strategy in ("MAIN", "SCALPER"):
                tracked = tracked_positions.get(strategy)
                if not isinstance(tracked, dict):
                    continue
                contract = tracked.get("contract") if isinstance(tracked.get("contract"), dict) else {}
                side = tracked.get("option_side") or tracked.get("side") or contract.get("side") or "OPT"
                strike = tracked.get("strike") or contract.get("strike") or "?"
                qty = tracked.get("qty") or "?"
                labels.append(f"{strategy[0]}:{qty}×{strike}{str(side)[0]}")
            self.card_vars["position"].set(" • ".join(labels) if labels else f"{len(tracked_positions)} OPEN")
        else:
            self.card_vars["position"].set("FLAT")
        last_webhook = state.get("last_webhook_received_at")
        self.card_vars["webhook"].set("RECEIVED" if last_webhook else "WAITING")

        raw_signal = state.get("last_signal")
        self.last_signal.set(f"Last signal: {short_text(raw_signal, 190)}")
        decision = state.get("last_decision")
        self.last_decision.set(f"Last decision: {short_text(decision, 220)}")
        pending_entries = state.get("pending_entry_intents") if isinstance(state.get("pending_entry_intents"), dict) else {}
        pending_exits = state.get("pending_exit_intents") if isinstance(state.get("pending_exit_intents"), dict) else {}
        pending_summary = {**pending_entries, **pending_exits}
        self.intent.set(f"Pending intents: {short_text(pending_summary, 210)}")
        strategies = state.get("strategies") if isinstance(state.get("strategies"), dict) else {}
        flow_parts = []
        for strategy in ("MAIN", "SCALPER"):
            st = strategies.get(strategy) if isinstance(strategies.get(strategy), dict) else {}
            if st.get("stopped_for_day"):
                status = "STOPPED DAY"
            elif st.get("pending_exit_intent"):
                status = "EXITING"
            elif st.get("tracked_position"):
                status = "OPEN"
            elif st.get("pending_entry_intent"):
                status = "ENTERING"
            else:
                status = "FLAT"
            flow_parts.append(f"{strategy} {status}")
        self.strategy_flow.set("Strategies: " + " • ".join(flow_parts))
        deadline = state.get("killswitch_deadline")
        armed = bool(state.get("killswitch_armed"))
        self.killswitch.set(f"Killswitch: {'ARMED (next ' + str(deadline) + ')' if armed else 'not armed'}")

        stats = state.get("journal_stats") or {}
        today = stats.get("today") or {}
        all_time = stats.get("all_time") or {}
        by_strategy = stats.get("by_strategy") if isinstance(stats.get("by_strategy"), dict) else {}
        main_today = (by_strategy.get("MAIN") or {}).get("today") or {}
        scalp_today = (by_strategy.get("SCALPER") or {}).get("today") or {}
        self.stats_text.set(
            f"Today: {today.get('total_trades', 0)} trades  •  WR {today.get('win_rate_pct', 0)}%  •  P&L ${today.get('total_pnl_dollars', 0)}\n"
            f"MAIN {main_today.get('total_trades', 0)} / ${main_today.get('total_pnl_dollars', 0)}  •  SCALPER {scalp_today.get('total_trades', 0)} / ${scalp_today.get('total_pnl_dollars', 0)}\n"
            f"All-time: {all_time.get('total_trades', 0)} trades  •  WR {all_time.get('win_rate_pct', 0)}%  •  PF {all_time.get('profit_factor')}  •  P&L ${all_time.get('total_pnl_dollars', 0)}"
        )
        quote_ms = state.get("last_quote_batch_ms")
        avg_ms = state.get("quote_latency_avg_ms")
        p95_ms = state.get("quote_latency_p95_ms")
        freshness = state.get("last_quote_age_seconds")
        parts = []
        if quote_ms is not None:
            parts.append(f"last batch {quote_ms} ms")
        if avg_ms is not None:
            parts.append(f"avg {avg_ms} ms")
        if p95_ms is not None:
            parts.append(f"p95 {p95_ms} ms")
        if freshness is not None:
            parts.append(f"quote age {freshness}s")
        self.latency_text.set("Latency: " + (" • ".join(parts) if parts else "—"))

    def copy_webhook(self, silent: bool = False) -> None:
        if self.controller.copy_webhook(self):
            if not silent:
                self.banner.configure(text="Full TradingView webhook URL copied to clipboard.", fg=self.GREEN, bg=self.PANEL2)
        elif not silent:
            messagebox.showinfo("BOT", "Webhook URL is not ready yet.")

    @staticmethod
    def open_tradingview() -> None:
        webbrowser.open("https://www.tradingview.com/chart/?symbol=NASDAQ%3AQQQ", new=2)

    @staticmethod
    def open_journal() -> None:
        path = STATE_DIR / "trade_journal"
        path.mkdir(parents=True, exist_ok=True)
        if os.name == "nt":
            os.startfile(path)  # type: ignore[attr-defined]
        else:
            webbrowser.open(path.as_uri())

    def show_diagnostics(self) -> None:
        state = self._last_state or http_json("/status", timeout=1.0) or {}
        lines = [
            f"BOT process: {'running' if self._running else 'stopped'}",
            f"Robinhood MCP: {'connected' if state.get('mcp_connected') else 'not connected'}",
            f"Automatic live submit: {'ON' if state.get('auto_submit_live_orders') else 'OFF'}",
            f"Agentic account: ****{state.get('agentic_account_last4') or '----'}",
            f"Buying power: {state.get('buying_power')}",
            f"0DTE ready: {state.get('zero_dte_ready')}",
            f"Cloudflare mode: {self.controller.tunnel_mode}",
            f"Cloudflare URL: {self.controller.public_url or 'not ready'}",
            f"Webhook URL: {mask_url(self.controller.webhook_url)}",
            f"Last TradingView webhook: {state.get('last_webhook_received_at') or 'none yet'}",
            f"Pending strategy intents: {len(state.get('pending_entry_intents') or {}) + len(state.get('pending_exit_intents') or {})}",
            f"Open strategy positions: {state.get('open_positions_count') or 0}",
            f"New entries open: {state.get('session_entry_open')}",
            f"Entry cutoff ET: {state.get('session_entry_cutoff_et')}",
            f"Session phase: {state.get('session_phase')}",
            f"Force-flat fallback ET: {state.get('session_force_flat_et')}",
            f"After-hours idle ET: {state.get('session_auto_shutdown_et')}",
            f"Last end-of-day action: {short_text(state.get('last_end_of_day_action'), 140)}",
            f"MAIN: {short_text((state.get('strategies') or {}).get('MAIN'), 120)}",
            f"SCALPER: {short_text((state.get('strategies') or {}).get('SCALPER'), 120)}",
            f"Last error: {state.get('last_error') or 'none'}",
        ]
        messagebox.showinfo("BOT diagnostics", "\n".join(lines))

    def _shutdown_block_reason(self) -> str | None:
        state = http_json("/status", timeout=1.0) or self._last_state or {}
        open_count = int(state.get("open_positions_count") or 0)
        regs = state.get("registered_orders") if isinstance(state.get("registered_orders"), dict) else {}
        submitted = []
        for bucket in (state.get("pending_entry_intents"), state.get("pending_exit_intents")):
            if isinstance(bucket, dict):
                submitted.extend(k for k, v in bucket.items() if isinstance(v, dict) and v.get("final_live_submit_performed"))
        if open_count or regs or submitted:
            return (
                f"BOT is still managing live exposure/order state (positions={open_count}, live orders={len(regs)}). "
                "Close/finish the Robinhood position first; the dashboard will not orphan it."
            )
        return None

    def stop_bot(self) -> None:
        if self._closing:
            return
        reason = self._shutdown_block_reason()
        if reason:
            self.banner.configure(text="STOP BLOCKED — live exposure is still being managed", fg="#111111", bg=self.YELLOW)
            messagebox.showwarning("BOT is still managing exposure", reason)
            return
        self.banner.configure(text="Stopping BOT…", fg=self.YELLOW, bg=self.PANEL2)
        self.controller.stop_async()

    def on_close(self) -> None:
        if self._closing:
            return
        reason = self._shutdown_block_reason()
        if reason:
            self.banner.configure(text="CLOSE BLOCKED — live exposure is still being managed", fg="#111111", bg=self.YELLOW)
            messagebox.showwarning("Keep BOT open", reason)
            return
        self._closing = True
        self.banner.configure(text="Stopping BOT before closing…", fg=self.YELLOW, bg=self.PANEL2)
        self.controller.stop_async()
        deadline = time.monotonic() + 8.0

        def wait_for_children() -> None:
            if self.controller.processes_stopped() or time.monotonic() >= deadline:
                self.destroy()
                return
            self.after(100, wait_for_children)

        self.after(100, wait_for_children)


def main() -> None:
    os.chdir(ROOT)
    app = BotApp()
    app.mainloop()


if __name__ == "__main__":
    main()
