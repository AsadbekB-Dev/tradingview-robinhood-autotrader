from datetime import datetime, timezone
from pathlib import Path

from app.signal_parser import parse_tradingview_alert, is_non_actionable_tradingview_alert
from app.risk import RiskGate


def _ts_ms():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


def _risk():
    return RiskGate({
        "require_fresh_signal_seconds": 25,
        "require_fresh_origin_signal_seconds": 25,
        "max_origin_clock_skew_seconds": 10,
        "require_origin_timestamp": True,
        "max_trade_budget_dollars": 200,
    })


def test_option2_call_wire_contract():
    t = _ts_ms()
    raw = (
        f"SOURCE MAIN | CALL QQQ | LEG PRIMARY | ENTRY 600.25 | TARGET 601.25 | "
        f"SL 599.50 | TV_TS {t} | SIG_ID OPT2V191-CALL-PRIMARY-{t} | TV_ORDER_ID L | TV_FILL 600.30"
    )
    s = parse_tradingview_alert(raw)
    assert s.event == "ENTRY" and s.direction == "LONG" and s.source == "MAIN"
    assert (s.entry, s.target, s.stop) == (600.25, 601.25, 599.50)
    assert _risk().validate_entry_signal(s)[0]
    assert _risk().signal_is_fresh(s)[0]


def test_option2_put_wire_contract():
    t = _ts_ms()
    raw = (
        f"SOURCE MAIN | PUT QQQ | LEG PRIMARY | ENTRY 600.25 | TARGET 599.25 | "
        f"SL 601.00 | TV_TS {t} | SIG_ID OPT2V191-PUT-PRIMARY-{t} | TV_ORDER_ID S | TV_FILL 600.20"
    )
    s = parse_tradingview_alert(raw)
    assert s.event == "ENTRY" and s.direction == "SHORT"
    assert _risk().validate_entry_signal(s)[0]
    assert _risk().signal_is_fresh(s)[0]


def test_option2_reentry_is_still_just_call_or_put():
    t = _ts_ms()
    raw = (
        f"SOURCE MAIN | CALL QQQ | LEG REENTRY | ENTRY 601.00 | TARGET 602.00 | "
        f"SL 600.50 | TV_TS {t} | SIG_ID OPT2V191-CALL-REENTRY-{t} | TV_ORDER_ID L2 | TV_FILL 601.05"
    )
    s = parse_tradingview_alert(raw)
    assert s.event == "ENTRY" and s.direction == "LONG" and s.setup == "LEG REENTRY"
    assert _risk().validate_entry_signal(s)[0]


def test_option2_every_close_command_is_exit_and_reason_is_metadata():
    t = _ts_ms()
    for reason in (
        "STOP", "REENTRY_STOP", "REENTRY_TARGET", "FLOW_STOP", "LOCK_FLOOR",
        "TIME_BLOCK", "LIFE_FLOOR_BREACH", "FLOW_FAIL", "LIFE_BANK",
        "REENTRY_EARLY", "REENTRY_PROTECT", "EARLY", "REENTRY_EXHAUST",
        "LIFE_BANK_EXHAUST", "SELL_EXHAUST",
    ):
        raw = (
            f"SOURCE MAIN | EXIT QQQ | LEG PRIMARY | REASON {reason} | TV_TS {t} | "
            f"SIG_ID OPT2V191-EXIT-PRIMARY-{t}"
        )
        s = parse_tradingview_alert(raw)
        assert s.event == "EXIT"
        assert s.exit_reason == reason
        assert s.source == "MAIN"


def test_option2_arm_call_put_are_get_ready_only():
    t = _ts_ms()
    assert is_non_actionable_tradingview_alert(
        f"SOURCE MAIN | ARM CALL QQQ | LEG PRIMARY | TRIGGER 600.25 | TV_TS {t}"
    )
    assert is_non_actionable_tradingview_alert(
        f"SOURCE MAIN | ARM PUT QQQ | LEG REENTRY | TRIGGER 599.75 | TV_TS {t}"
    )


def test_legacy_long_short_and_arm_formats_still_fail_safe():
    t = _ts_ms()
    s = parse_tradingview_alert(
        f"SOURCE MAIN | LONG QQQ | LEGACY | ENTRY 600 | TARGET 601 | SL 599 | TV_TS {t}"
    )
    assert s.event == "ENTRY" and s.direction == "LONG"
    assert is_non_actionable_tradingview_alert(f"SOURCE MAIN | ARM LONG QQQ | X | TV_TS {t}")


def test_pine_actionable_vocabulary_is_only_call_put_exit():
    pine = Path(__file__).resolve().parents[1] / "tradingview" / "Option2_QQQ_Evolving_Auction_Interpreter_v1_9_1_SIMPLE_WEBHOOK.pine"
    text = pine.read_text(encoding="utf-8")
    # No legacy actionable order messages may remain in alert payloads.
    forbidden = [
        'alert_message="ENTER CALL"', 'alert_message="ENTER PUT"',
        'alert_message="ENTER AGAIN CALL"', 'alert_message="ENTER AGAIN PUT"',
        'alert_message="SELL"', 'alert_message="SELL AGAIN"',
        'alert_message="EXIT TIME BLOCK"', 'alert_loss="SELL"',
        'alert_profit="SELL AGAIN"', 'alert_loss="SELL AGAIN"',
    ]
    for token in forbidden:
        assert token not in text
    assert '" | " + optionAction + " QQQ"' in text
    assert '" | EXIT QQQ"' in text
    assert '" | ARM " + optionAction + " QQQ"' in text
