from __future__ import annotations

import csv
import json
import os
import tempfile
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any


class TradeJournal:
    """Persistent journal for confirmed live fills only.

    v0.7 supports more than one simultaneous logical strategy position. Open
    trades are therefore persisted in ``open_trades.json`` keyed by strategy.
    ``open_trade.json`` remains a compatibility mirror when exactly one logical
    trade is open, which keeps older tooling/tests readable during migration.
    """

    CSV_FIELDS = [
        "trade_id",
        "strategy",
        "date",
        "opened_at",
        "closed_at",
        "symbol",
        "direction",
        "setup",
        "tier",
        "option_side",
        "expiration",
        "strike",
        "qty",
        "entry_option_price",
        "exit_option_price",
        "exit_price_source",
        "capital_deployed",
        "proceeds",
        "pnl_dollars",
        "pnl_percent",
        "result",
        "hold_minutes",
        "underlying_entry",
        "underlying_exit",
        "target",
        "stop",
        "exit_trigger",
        "exit_reason",
        "entry_delta",
        "entry_gamma",
        "entry_theta",
        "entry_iv",
        "entry_spread_pct",
        "selected_score",
        "projected_target_profit_dollars",
        "projected_stop_profit_dollars",
        "signal_to_decision_ms",
        "signal_to_contract_selected_ms",
        "exit_reaction_ms",
        "entry_order_id",
        "exit_order_id",
        "fill_source",
    ]

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        self.daily_dir = self.root / "daily"
        self.daily_dir.mkdir(parents=True, exist_ok=True)
        self.open_path = self.root / "open_trade.json"  # legacy mirror
        self.open_trades_path = self.root / "open_trades.json"
        self.jsonl_path = self.root / "trades.jsonl"
        self.csv_path = self.root / "trades.csv"
        self.stats_path = self.root / "stats.json"
        self._stats_cache: dict[str, Any] = {}
        self._migrate_legacy_open_trade()
        self.refresh_stats()

    @staticmethod
    def _dump(path: Path, payload: Any) -> None:
        """Write JSON safely on Windows without letting transient file locks kill BOT.

        Windows can briefly deny ``os.replace`` when Defender, an editor, or another
        process has the destination open. Use a unique same-directory temp file and
        retry the atomic replace for a short bounded window.
        """
        path.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(payload, indent=2, default=str)
        fd, tmp_name = tempfile.mkstemp(
            prefix=f".{path.name}.", suffix=".tmp", dir=str(path.parent)
        )
        tmp = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                f.write(data)
                f.flush()
                try:
                    os.fsync(f.fileno())
                except OSError:
                    pass

            delay = 0.01
            last_exc: OSError | None = None
            for _ in range(10):
                try:
                    os.replace(tmp, path)
                    return
                except PermissionError as exc:
                    last_exc = exc
                    time.sleep(delay)
                    delay = min(delay * 2.0, 0.20)
                except OSError as exc:
                    last_exc = exc
                    time.sleep(delay)
                    delay = min(delay * 2.0, 0.20)
            if last_exc is not None:
                raise last_exc
        finally:
            try:
                tmp.unlink(missing_ok=True)
            except Exception:
                pass

    @staticmethod
    def _dt(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            # Broker timestamps are commonly offset-aware while older/local
            # journal timestamps were written without an offset. Normalize both
            # to UTC-aware datetimes before arithmetic so a confirmed EXIT can
            # never fail merely because the timestamp formats differ. Historical
            # naive values are interpreted in the machine's local timezone, which
            # matches how this bot originally produced them with datetime.now().
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                local_tz = datetime.now().astimezone().tzinfo
                dt = dt.replace(tzinfo=local_tz)
            return dt.astimezone(timezone.utc)
        except Exception:
            return None

    def _migrate_legacy_open_trade(self) -> None:
        if self.open_trades_path.exists() or not self.open_path.exists():
            return
        try:
            trade = json.loads(self.open_path.read_text(encoding="utf-8"))
        except Exception:
            return
        if not isinstance(trade, dict) or not trade:
            return
        strategy = str(trade.get("strategy") or "MAIN").upper()
        trade["strategy"] = strategy
        self._dump(self.open_trades_path, {strategy: trade})

    def load_open_trades(self) -> dict[str, dict[str, Any]]:
        if not self.open_trades_path.exists():
            return {}
        try:
            value = json.loads(self.open_trades_path.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                return {}
            out: dict[str, dict[str, Any]] = {}
            for key, record in value.items():
                if isinstance(record, dict):
                    strategy = str(key).upper()
                    record = dict(record)
                    record.setdefault("strategy", strategy)
                    out[strategy] = record
            return out
        except Exception:
            return {}

    def _write_open_trades(self, trades: dict[str, dict[str, Any]]) -> None:
        clean = {str(k).upper(): dict(v) for k, v in trades.items() if isinstance(v, dict)}
        if clean:
            self._dump(self.open_trades_path, clean)
        else:
            try:
                self.open_trades_path.unlink(missing_ok=True)
            except Exception:
                pass

        # Compatibility mirror. If two strategies are open at once there is no
        # truthful single open_trade.json, so remove it rather than overwrite one.
        if len(clean) == 1:
            self._dump(self.open_path, next(iter(clean.values())))
        else:
            try:
                self.open_path.unlink(missing_ok=True)
            except Exception:
                pass

    def load_open_trade(self, strategy: str | None = None) -> dict[str, Any] | None:
        trades = self.load_open_trades()
        if strategy:
            return trades.get(str(strategy).upper())
        if "MAIN" in trades:
            return trades["MAIN"]
        if len(trades) == 1:
            return next(iter(trades.values()))
        return None

    def start_trade(self, record: dict[str, Any]) -> dict[str, Any]:
        payload = dict(record)
        strategy = str(payload.get("strategy") or "MAIN").upper()
        payload["strategy"] = strategy
        payload.setdefault("trade_id", f"trade-{strategy.lower()}-{int(datetime.now().timestamp() * 1000)}")
        payload.setdefault("opened_at", datetime.now().isoformat(timespec="milliseconds"))
        payload.setdefault("date", str(payload["opened_at"])[:10])
        payload["status"] = "OPEN"

        trades = self.load_open_trades()
        if strategy in trades:
            raise RuntimeError(f"Journal already has an open {strategy} trade")
        trades[strategy] = payload
        self._write_open_trades(trades)
        self.refresh_stats()
        return payload

    def _read_trades(self) -> list[dict[str, Any]]:
        if not self.jsonl_path.exists():
            return []
        out: list[dict[str, Any]] = []
        for line in self.jsonl_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
                if isinstance(row, dict):
                    row.setdefault("strategy", "MAIN")
                    out.append(row)
            except Exception:
                continue
        return out

    def _write_csv(self, trades: list[dict[str, Any]]) -> None:
        with self.csv_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.CSV_FIELDS, extrasaction="ignore")
            writer.writeheader()
            for trade in trades:
                writer.writerow({k: trade.get(k) for k in self.CSV_FIELDS})

    @staticmethod
    def _summary(trades: list[dict[str, Any]]) -> dict[str, Any]:
        numeric = [t for t in trades if isinstance(t.get("pnl_dollars"), (int, float))]
        winners = [t for t in numeric if float(t["pnl_dollars"]) > 0]
        losers = [t for t in numeric if float(t["pnl_dollars"]) < 0]
        breakeven = [t for t in numeric if abs(float(t["pnl_dollars"])) < 1e-9]
        gross_profit = sum(float(t["pnl_dollars"]) for t in winners)
        gross_loss = sum(float(t["pnl_dollars"]) for t in losers)
        total_pnl = sum(float(t["pnl_dollars"]) for t in numeric)
        returns = [float(t["pnl_percent"]) for t in numeric if isinstance(t.get("pnl_percent"), (int, float))]
        holds = [float(t["hold_minutes"]) for t in trades if isinstance(t.get("hold_minutes"), (int, float))]
        capital = sum(float(t.get("capital_deployed") or 0.0) for t in trades)

        best = max(numeric, key=lambda t: float(t["pnl_dollars"]), default=None)
        worst = min(numeric, key=lambda t: float(t["pnl_dollars"]), default=None)

        return {
            "total_trades": len(trades),
            "trades_with_pnl": len(numeric),
            "wins": len(winners),
            "losses": len(losers),
            "breakeven": len(breakeven),
            "win_rate_pct": round((len(winners) / len(numeric) * 100.0), 2) if numeric else 0.0,
            "total_pnl_dollars": round(total_pnl, 2),
            "gross_profit_dollars": round(gross_profit, 2),
            "gross_loss_dollars": round(gross_loss, 2),
            "profit_factor": round(gross_profit / abs(gross_loss), 3) if gross_loss < 0 else None,
            "average_pnl_dollars": round(total_pnl / len(numeric), 2) if numeric else 0.0,
            "average_return_pct": round(sum(returns) / len(returns), 2) if returns else 0.0,
            "average_hold_minutes": round(sum(holds) / len(holds), 2) if holds else 0.0,
            "capital_deployed_dollars": round(capital, 2),
            "best_trade": ({"trade_id": best.get("trade_id"), "pnl_dollars": best.get("pnl_dollars")} if best else None),
            "worst_trade": ({"trade_id": worst.get("trade_id"), "pnl_dollars": worst.get("pnl_dollars")} if worst else None),
        }

    def refresh_stats(self) -> dict[str, Any]:
        trades = self._read_trades()
        today = date.today().isoformat()
        today_trades = [t for t in trades if str(t.get("date", "")) == today]
        strategies = sorted({str(t.get("strategy") or "MAIN").upper() for t in trades} | set(self.load_open_trades()))
        by_strategy = {}
        for strategy in strategies:
            rows = [t for t in trades if str(t.get("strategy") or "MAIN").upper() == strategy]
            today_rows = [t for t in rows if str(t.get("date", "")) == today]
            by_strategy[strategy] = {
                "today": self._summary(today_rows),
                "all_time": self._summary(rows),
                "open_trade": self.load_open_trade(strategy),
            }
        stats = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "current_date": today,
            "open_trade": self.load_open_trade(),
            "open_trades": self.load_open_trades(),
            "today": self._summary(today_trades),
            "all_time": self._summary(trades),
            "by_strategy": by_strategy,
        }
        self._dump(self.stats_path, stats)
        self._stats_cache = stats
        return stats

    def _write_daily_snapshot(self, day: str, trades: list[dict[str, Any]]) -> None:
        day_trades = [t for t in trades if str(t.get("date", "")) == day]
        payload = {
            "date": day,
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "summary": self._summary(day_trades),
            "by_strategy": {
                s: self._summary([t for t in day_trades if str(t.get("strategy") or "MAIN").upper() == s])
                for s in sorted({str(t.get("strategy") or "MAIN").upper() for t in day_trades})
            },
            "trades": day_trades,
        }
        self._dump(self.daily_dir / f"{day}.json", payload)

    def close_trade(
        self,
        exit_data: dict[str, Any],
        *,
        strategy: str | None = None,
        trade_id: str | None = None,
    ) -> dict[str, Any] | None:
        open_trades = self.load_open_trades()
        open_trade: dict[str, Any] | None = None
        chosen_strategy: str | None = None

        if trade_id:
            for s, row in open_trades.items():
                if row.get("trade_id") == trade_id:
                    chosen_strategy, open_trade = s, row
                    break
        elif strategy:
            chosen_strategy = str(strategy).upper()
            open_trade = open_trades.get(chosen_strategy)
        else:
            if "MAIN" in open_trades:
                chosen_strategy, open_trade = "MAIN", open_trades["MAIN"]
            elif len(open_trades) == 1:
                chosen_strategy, open_trade = next(iter(open_trades.items()))

        if not open_trade or not chosen_strategy:
            return None

        trade = dict(open_trade)
        trade.update(exit_data)
        trade["strategy"] = chosen_strategy
        trade["status"] = "CLOSED"
        trade.setdefault("closed_at", datetime.now().isoformat(timespec="milliseconds"))
        trade.setdefault("date", str(trade.get("opened_at", trade["closed_at"]))[:10])

        opened = self._dt(str(trade.get("opened_at") or ""))
        closed = self._dt(str(trade.get("closed_at") or ""))
        if opened and closed:
            trade["hold_minutes"] = round(max(0.0, (closed - opened).total_seconds()) / 60.0, 3)

        qty = int(trade.get("qty") or 0)
        entry = trade.get("entry_option_price")
        exit_price = trade.get("exit_option_price")
        capital = trade.get("capital_deployed")
        if capital is None and isinstance(entry, (int, float)):
            capital = float(entry) * 100.0 * qty
        trade["capital_deployed"] = round(float(capital or 0.0), 2)

        if isinstance(exit_price, (int, float)) and qty > 0:
            proceeds = float(exit_price) * 100.0 * qty
            pnl = proceeds - float(trade["capital_deployed"])
            trade["proceeds"] = round(proceeds, 2)
            trade["pnl_dollars"] = round(pnl, 2)
            trade["pnl_percent"] = round((pnl / float(trade["capital_deployed"]) * 100.0), 2) if float(trade["capital_deployed"]) > 0 else None
            trade["result"] = "WIN" if pnl > 1e-9 else ("LOSS" if pnl < -1e-9 else "BREAKEVEN")
        else:
            trade["proceeds"] = None
            trade["pnl_dollars"] = None
            trade["pnl_percent"] = None
            trade["result"] = "UNKNOWN"

        existing = self._read_trades()
        tid = trade.get("trade_id")
        if not any(t.get("trade_id") == tid for t in existing):
            with self.jsonl_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(trade, default=str) + "\n")
            existing.append(trade)

        open_trades.pop(chosen_strategy, None)
        self._write_open_trades(open_trades)
        self._write_csv(existing)
        self._write_daily_snapshot(str(trade["date"]), existing)
        self.refresh_stats()
        return trade

    def stats(self) -> dict[str, Any]:
        # Stats only change when the journal changes; open/close operations already
        # refresh them. Returning the cache avoids rewriting stats.json on every
        # worker state sync (which v0.7.0 could do dozens of times per second).
        if not self._stats_cache:
            return self.refresh_stats()
        return json.loads(json.dumps(self._stats_cache, default=str))

    def recent_trades(self, limit: int = 50) -> list[dict[str, Any]]:
        trades = self._read_trades()
        return trades[-max(1, int(limit)) :]
