from __future__ import annotations

import re

from app.models import TradeSignal


def _num(text: str, key: str):
    m = re.search(rf"\b{re.escape(key)}\s+([+-]?\d+(?:\.\d+)?)", text)
    return float(m.group(1)) if m else None



def _origin_timestamp(text: str):
    m = re.search(r"\bTV_TS\s+(\d{10,16})\b", text, flags=re.IGNORECASE)
    if not m:
        return None
    try:
        raw = int(m.group(1))
        seconds = raw / 1000.0 if raw >= 10_000_000_000 else float(raw)
        from datetime import datetime, timezone
        return datetime.fromtimestamp(seconds, tz=timezone.utc).replace(tzinfo=None)
    except Exception:
        return None


def _signal_id(text: str):
    m = re.search(r"\bSIG_ID\s+([A-Za-z0-9_.:-]{1,96})", text, flags=re.IGNORECASE)
    return m.group(1) if m else None


def _extract_source(raw: str) -> tuple[str, str]:
    """Return (strategy_source, message_without_source_prefix).

    v0.7 accepts a few human-readable forms so TradingView alerts stay easy to
    inspect. Legacy untagged alerts are deliberately treated as MAIN so the
    existing selective strategy alert does not need to be rebuilt.
    """
    s = raw.strip()
    patterns = (
        r"^SOURCE\s+([A-Z0-9_-]+)\s*\|\s*(.+)$",
        r"^SOURCE\s*=\s*([A-Z0-9_-]+)\s*\|\s*(.+)$",
        r"^\[([A-Z0-9_-]+)\]\s*(.+)$",
        r"^(MAIN|SCALPER)\s*\|\s*(.+)$",
    )
    for pattern in patterns:
        m = re.match(pattern, s, flags=re.IGNORECASE | re.DOTALL)
        if m:
            source = str(m.group(1)).strip().upper()
            body = str(m.group(2)).strip()
            return source, body
    return "MAIN", s


def is_non_actionable_tradingview_alert(raw: str) -> bool:
    """Return True for setup-state messages that must never place orders.

    Older/manual TradingView alerts can accidentally target an ARM/setup marker
    or a legacy STOP TRADING FOR TODAY message. v0.7.4 treats those as harmless
    informational chatter instead of allowing them to change live execution
    state. Real automation should send only filled ENTRY/EXIT order messages.
    """
    _source, body = _extract_source(raw)
    u = re.sub(r"\s+", " ", body.strip().upper())
    return (
        u.startswith("ARM CALL QQQ")
        or u.startswith("ARM PUT QQQ")
        or u.startswith("ARM LONG QQQ")
        or u.startswith("ARM SHORT QQQ")
        or u.startswith("SETUP LONG QQQ")
        or u.startswith("SETUP SHORT QQQ")
        or u.startswith("STOP TRADING QQQ")
        or "NO ENTRY YET" in u
    )


def parse_tradingview_alert(raw: str) -> TradeSignal:
    source, s = _extract_source(raw)

    # Main bot entry format uses ENTRY/TARGET/SL. The scalper uses
    # TRIGGER/TARGET +/-distance/FAILURE_SL, so its trigger is accepted as the
    # underlying entry reference while target may remain None (the executor does
    # not need TradingView's underlying target to submit the option order).
    # Simple live protocol:
    #   CALL QQQ | ...  -> long underlying thesis / buy call
    #   PUT QQQ  | ...  -> short underlying thesis / buy put
    # Legacy LONG/SHORT forms remain accepted for backward compatibility.
    entry_match = re.match(r"^(CALL|PUT|LONG|SHORT)\s+QQQ\s*\|", s, flags=re.IGNORECASE)
    if entry_match:
        parts = [p.strip() for p in s.split("|")]
        verb = entry_match.group(1).upper()
        direction = "LONG" if verb in {"CALL", "LONG"} else "SHORT"
        setup = parts[1] if len(parts) > 1 else None
        tier = parts[-1] if parts and ("TIER" in parts[-1].upper()) else None
        entry = _num(s, "ENTRY")
        if entry is None:
            entry = _num(s, "TRIGGER")
        stop = _num(s, "SL")
        if stop is None:
            stop = _num(s, "FAILURE_SL")
        target = _num(s, "TARGET")
        # The selective MAIN strategy sends an absolute TARGET. The reaction
        # scalper sends TARGET +0.01 / -0.02 as a signed distance from TRIGGER.
        if entry is not None and target is not None and re.search(r"\bTARGET\s+[+-]", s):
            target = entry + target
        return TradeSignal(
            source=source,
            event="ENTRY",
            direction=direction,
            setup=setup,
            entry=entry,
            target=target,
            stop=stop,
            lead=_num(s, "LEAD"),
            opportunity=_num(s, "OPP"),
            tier=tier,
            raw=raw.strip(),
            origin_at=_origin_timestamp(s),
            signal_id=_signal_id(s),
        )

    # Both strategies can emit either:
    #   EXIT QQQ
    #   EXIT QQQ | REASON | PRICE ...
    #   WIN / EXIT QQQ | REASON | PRICE ...
    # or the older main-strategy shape:
    #   WIN QQQ | REASON | EXIT ...
    #
    # IMPORTANT: v0.7.4 MAIN's order-fill alert was exactly ``EXIT QQQ``
    # after the SOURCE prefix was stripped. Treat the bare form as a valid EXIT.
    is_live_exit = bool(re.match(r"^EXIT\s+QQQ(?:\s*\||\s*$)", s, flags=re.IGNORECASE)) or bool(
        re.match(r"^(WIN|LOSS|FLAT)\s*/\s*EXIT\s+QQQ(?:\s*\||\s*$)", s, flags=re.IGNORECASE)
    )
    is_closed_trade_exit = bool(re.match(r"^(WIN|LOSS|FLAT)\s+QQQ\s*\|", s)) and bool(
        re.search(r"\|\s*EXIT\s+-?\d+(?:\.\d+)?", s)
    )
    if is_live_exit or is_closed_trade_exit:
        parts = [p.strip() for p in s.split("|")]
        reason_match = re.search(r"\bREASON\s+([A-Za-z0-9_.:-]{1,96})", s, flags=re.IGNORECASE)
        reason = reason_match.group(1) if reason_match else (parts[1] if len(parts) > 1 else "EXIT")
        exit_price = _num(s, "PRICE")
        if exit_price is None:
            exit_price = _num(s, "EXIT")
        return TradeSignal(
            source=source,
            event="EXIT",
            exit_reason=reason,
            exit_price=exit_price,
            raw=raw.strip(),
            origin_at=_origin_timestamp(s),
            signal_id=_signal_id(s),
        )

    if s.startswith("STOP TRADING QQQ"):
        return TradeSignal(source=source, event="STOP_DAY", raw=raw.strip(), origin_at=_origin_timestamp(s), signal_id=_signal_id(s))

    raise ValueError(f"Unrecognized TradingView alert: {raw.strip()}")
