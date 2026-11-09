# Asads AUTOMATION v0.7.8 HARDENED — adversarial audit

## Scope

This pass treated the automation as hostile-to-itself: malformed/replayed TradingView messages, queue floods, transient cancel failures, partial/ambiguous broker fills, stale local state, local file-write failures, duplicate EXITs, shutdown with unknown broker exposure, and mixed broker/local timestamp formats were all considered failure cases.

This is **not** a claim that live trading software can be made failure-proof. The purpose of this release is to fail closed on new risk and remain biased toward reducing already-open risk when local bookkeeping is damaged.

## Material failures found in v0.7.7 and hardened

1. **Invalid strategy geometry could reach contract ranking.** A LONG with target below entry / stop above entry (or reversed SHORT) was not structurally rejected. v0.7.8 validates finite positive prices and requires LONG `target > entry > stop`, SHORT `target < entry < stop` before any broker-entry work.
2. **Untimestamped ENTRY alerts could bypass origin freshness.** The package now requires `TV_TS` for ENTRY by default and still rejects stale/future-skewed origins.
3. **A failed cancel could become permanently latched as if cancellation had already been requested successfully.** Failed/negative cancels are now retryable after a cooldown; only broker acceptance is treated as accepted.
4. **Webhook bursts could starve broker polling/watchdogs.** Normal jobs are bounded and only a small number are processed per loop; EXIT/STOP jobs have a separate urgent queue and are serviced first. Queue overflow returns HTTP 503 rather than a false success.
5. **Legacy local mutation endpoints could alter execution bookkeeping.** They are disabled by default. Clearing an intent is also refused once a live submit/registered order exists.
6. **Shutdown accounting omitted unmanaged broker positions.** Unknown/mismatched live option exposure now participates in the shutdown exposure gate.
7. **Broker position reconciliation was too event-driven.** The worker now refreshes broker positions periodically in addition to startup/entry checks.
8. **Some broker fill shapes could report processed quantity without leg-execution rows.** Explicit broker average-fill fields are accepted as a fallback; the working limit price is deliberately never fabricated into a fill.
9. **Local persistence failures could block a risk-reducing EXIT or leave an accepted broker order unpollable.** ENTRY still fails closed if its pre-submit state cannot be persisted. EXIT persistence is best-effort, and after a broker order ID is returned the registered order is established in memory before fallible disk writes.
10. **Trade-journal writes could interrupt post-fill management.** Confirmed broker state now wins over reporting state; journal failures are isolated and surfaced for reconciliation instead of undoing/forgetting broker-flat state.
11. **Mixed timezone-aware broker timestamps and timezone-naive local timestamps could crash trade close calculations.** Journal datetime parsing now normalizes both before hold-time arithmetic.

## Verification performed

- Python compile pass for `app`, `tools`, and tests.
- 12 adversarial regression tests pass.
- Parser regression covers the current MAIN entry/exit message forms and the old bare `SOURCE MAIN | EXIT QQQ` form.
- Tests cover malformed LONG/SHORT geometry, missing/stale/future `TV_TS`, retryable cancel failure, refusal to clear submitted state, EXIT queue priority, bounded queue overflow, unmanaged-exposure shutdown blocking, explicit-average-price fill fallback, and mixed naive/aware journal timestamps.
- A fake-broker end-to-end exercise was run through ENTRY selection/submission/fill -> tracked position -> EXIT -> duplicate EXIT suppression -> confirmed exit fill -> flat.
- A partial-exit exercise was run through partial fill -> terminal cancellation -> replacement of only the remaining quantity -> final fill -> flat. That exercise exposed the mixed-timezone journal bug fixed in this release.

## Residual risks / things that still cannot be guaranteed

- Broker/MCP calls are still serialized and can block the worker for their timeout window. A hung external service can therefore delay management despite queue prioritization. This is the largest remaining architectural risk if the bot is meant to run unattended with real money.
- Cloudflare Quick Tunnel hostnames can change after restart. A stable named tunnel is safer operationally for a permanent TradingView webhook.
- Robinhood/API semantics can change. The bot must continue to fail closed when a response is missing/ambiguous rather than guessing.
- EXIT repricing is finite, by design, to avoid uncontrolled duplicate-order loops. A repeatedly failing broker/cancel path can still require manual intervention.
- Power loss, OS crash, ISP outage, broker outage, and exchange halts cannot be solved purely in application code. Startup reconciliation helps recovery but cannot guarantee an exit during an external outage.

## Before live money

Run the hardened package in a no-submit/shadow mode first, then conduct controlled tiny-size live tests of: normal entry/exit, duplicate EXIT, partial fill, rejected order, cancel failure, tunnel loss while a position is open, process restart with an open position, and force-flat timing. Do not scale size until those exact operational tests are observed on the current Robinhood/MCP behavior.
