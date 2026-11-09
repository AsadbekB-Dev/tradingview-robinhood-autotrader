from __future__ import annotations

import tempfile
import time
from datetime import datetime, timezone, timedelta, date

import pytest

from app.models import TradeSignal, OptionContract
from app.risk import RiskGate
from app.signal_parser import parse_tradingview_alert
from app.contract_selector import ContractSelector
from app.robinhood.persistent_worker import PersistentRobinhoodWorker, SignalJob


class FakeClient:
    def __init__(self):
        self.calls = []
        self.fail_cancel_once = False
        self.cancel_attempts = 0

    def start(self, timeout=1):
        return None

    def stop(self):
        return None

    def tool_names(self):
        return []

    def tool_schema(self, name):
        return None

    def call_tool(self, name, arguments=None, timeout=1):
        self.calls.append((name, arguments or {}))
        if name == 'cancel_option_order':
            self.cancel_attempts += 1
            if self.fail_cancel_once and self.cancel_attempts == 1:
                raise RuntimeError('transient network failure')
            return {'data': {'accepted': True}}
        raise RuntimeError(f'unexpected tool call {name}')


def base_cfg(**worker_overrides):
    d = tempfile.mkdtemp()
    worker = {
        'state_dir': d,
        'signal_dedupe_seconds': 20,
        'signal_queue_maxsize': 8,
        'jobs_per_loop': 1,
        'cancel_retry_seconds': 0.25,
        'broker_position_reconcile_seconds': 5,
    }
    worker.update(worker_overrides)
    return {
        'server': {},
        'execution': {
            'auto_submit_live_orders': False,
            'max_trade_budget_dollars': 200,
            'require_fresh_signal_seconds': 25,
            'require_fresh_origin_signal_seconds': 25,
            'max_origin_clock_skew_seconds': 10,
            'require_origin_timestamp': True,
        },
        'selector': {},
        'persistent_worker': worker,
        'robinhood_mcp': {},
        'journal': {'directory': d + '/journal'},
    }


def ts_ms(dt=None):
    dt = dt or datetime.now(timezone.utc)
    return int(dt.timestamp() * 1000)


def test_current_pine_entry_and_exit_formats_parse():
    t = ts_ms()
    long_raw = f'SOURCE MAIN | LONG QQQ | TEST | ENTRY 500.00 | TARGET 501.00 | SL 499.50 | TV_TS {t}'
    sig = parse_tradingview_alert(long_raw)
    assert sig.event == 'ENTRY' and sig.direction == 'LONG'
    assert sig.entry == 500 and sig.target == 501 and sig.stop == 499.5

    exit_sig = parse_tradingview_alert(f'SOURCE MAIN | EXIT QQQ | STOP | TV_TS {t}')
    assert exit_sig.event == 'EXIT'

    bare_exit = parse_tradingview_alert('SOURCE MAIN | EXIT QQQ')
    assert bare_exit.event == 'EXIT'


def test_entry_geometry_fails_closed():
    risk = RiskGate(base_cfg()['execution'])
    bad_long = TradeSignal(event='ENTRY', direction='LONG', entry=500, target=499, stop=501, raw='x', origin_at=datetime.now(timezone.utc).replace(tzinfo=None))
    ok, why = risk.validate_entry_signal(bad_long)
    assert not ok and 'geometry' in why

    bad_short = TradeSignal(event='ENTRY', direction='SHORT', entry=500, target=501, stop=499, raw='x', origin_at=datetime.now(timezone.utc).replace(tzinfo=None))
    ok, why = risk.validate_entry_signal(bad_short)
    assert not ok and 'geometry' in why


def test_selector_no_longer_gets_malformed_geometry_through_risk_gate():
    cfg = base_cfg()['execution']
    risk = RiskGate(cfg)
    s = TradeSignal(event='ENTRY', direction='LONG', entry=500, target=499, stop=501, raw='x', origin_at=datetime.now(timezone.utc).replace(tzinfo=None))
    opt = OptionContract(symbol='QQQ', instrument_id='x', expiration=date.today(), strike=500, side='CALL', bid=1, ask=1.05, delta=.5, gamma=.02, theta=-.1)
    ranked = ContractSelector({'min_dte':0,'max_dte':0,'hard_max_spread_pct':.5}).rank_positions(s,[opt],200,today=date.today(),current_underlying_price=500)
    assert ranked, 'selector itself remains a scorer; risk layer is what must reject malformed strategy geometry'
    approved, why = risk.approve_budgeted_entry(s, ranked[0], open_positions=0)
    assert not approved and 'geometry' in why


def test_missing_origin_timestamp_is_rejected_for_entry():
    risk = RiskGate(base_cfg()['execution'])
    s = TradeSignal(event='ENTRY', direction='LONG', entry=500, target=501, stop=499, raw='x')
    ok, why = risk.signal_is_fresh(s)
    assert not ok and 'TV_TS' in why


def test_stale_and_future_origin_rejected():
    risk = RiskGate(base_cfg()['execution'])
    old = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(seconds=30)
    s = TradeSignal(event='ENTRY', direction='LONG', entry=500, target=501, stop=499, raw='x', origin_at=old)
    assert risk.signal_is_fresh(s)[0] is False

    future = datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=15)
    s = TradeSignal(event='ENTRY', direction='LONG', entry=500, target=501, stop=499, raw='x', origin_at=future)
    assert risk.signal_is_fresh(s)[0] is False


def test_cancel_network_failure_is_retryable_not_latched_forever():
    client = FakeClient(); client.fail_cancel_once = True
    w = PersistentRobinhoodWorker(base_cfg(), tool_client=client)
    w.account_number = 'acct'
    w.pending_entry_intent = {'kind': 'ENTRY'}
    w.registered_order = {'strategy':'MAIN','kind':'ENTRY','order_id':'abc','registered_at':'2000-01-01T00:00:00+00:00'}

    assert w._request_cancel_registered_order(reason='timeout') is False
    assert client.cancel_attempts == 1
    assert w._request_cancel_registered_order(reason='timeout') is False  # cooldown
    assert client.cancel_attempts == 1
    w.registered_order['cancel_requested_at'] = (datetime.now(timezone.utc)-timedelta(seconds=1)).isoformat()
    assert w._request_cancel_registered_order(reason='timeout') is True
    assert client.cancel_attempts == 2
    assert w.registered_order['cancel_accepted'] is True


def test_cannot_clear_live_submitted_intent():
    w = PersistentRobinhoodWorker(base_cfg(), tool_client=FakeClient())
    w.pending_entry_intent = {'kind':'ENTRY','final_live_submit_performed':True}
    with pytest.raises(RuntimeError):
        w.clear_pending_intent(kind='ENTRY')

    w.pending_entry_intent = {'kind':'ENTRY','final_live_submit_performed':False}
    w.registered_order = {'kind':'ENTRY','order_id':'abc'}
    with pytest.raises(RuntimeError):
        w.clear_pending_intent(kind='ENTRY')


def test_urgent_exit_jobs_preempt_normal_entry_jobs():
    w = PersistentRobinhoodWorker(base_cfg(), tool_client=FakeClient())
    seen = []
    w._handle_signal = lambda raw, received_at=None: seen.append(raw) or {'ok':True}
    assert w.enqueue_signal('SOURCE MAIN | LONG QQQ | X')
    assert w.enqueue_signal('SOURCE MAIN | EXIT QQQ')
    w._process_jobs(max_jobs=1)
    assert seen == ['SOURCE MAIN | EXIT QQQ']
    w._process_jobs(max_jobs=1)
    assert seen[-1] == 'SOURCE MAIN | LONG QQQ | X'


def test_queue_is_bounded_and_rejects_instead_of_silent_drop():
    w = PersistentRobinhoodWorker(base_cfg(signal_queue_maxsize=8), tool_client=FakeClient())
    for i in range(8):
        assert w.enqueue_signal(f'SOURCE MAIN | LONG QQQ | {i}')
    assert w.enqueue_signal('SOURCE MAIN | LONG QQQ | overflow') is False


def test_shutdown_exposure_summary_includes_unmanaged_broker_positions():
    w = PersistentRobinhoodWorker(base_cfg(), tool_client=FakeClient())
    w._set_state(unmanaged_live_positions=[{'option_id':'x','quantity':1}])
    reasons = w._live_exposure_summary()
    assert any('unmanaged' in x for x in reasons)


def test_order_fill_fallback_uses_explicit_average_price_only():
    qty, price, ts = PersistentRobinhoodWorker._weighted_execution_fill({
        'processed_quantity':'2', 'average_price':'1.23', 'updated_at':'2026-01-01T00:00:00Z', 'legs':[]
    })
    assert qty == 2 and price == 1.23
    qty, price, _ = PersistentRobinhoodWorker._weighted_execution_fill({
        'processed_quantity':'2', 'price':'1.50', 'legs':[]
    })
    assert qty == 2 and price is None


def test_journal_handles_mixed_naive_and_aware_timestamps():
    from app.trade_journal import TradeJournal
    root = tempfile.mkdtemp()
    j = TradeJournal(root)
    j.start_trade({
        'strategy': 'MAIN', 'opened_at': '2026-09-19T10:00:00.000',
        'symbol': 'QQQ', 'qty': 1, 'entry_option_price': 1.00,
    })
    closed = j.close_trade({
        'closed_at': '2026-09-19T17:05:00+00:00',
        'exit_option_price': 1.10,
    }, strategy='MAIN')
    assert closed is not None
    assert isinstance(closed.get('hold_minutes'), float)
    assert j.stats()['all_time']['total_trades'] == 1
