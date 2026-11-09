# Asads AUTOMATION v0.7.8 HARDENED

Windows automation package for QQQ 0DTE execution through Robinhood Agentic Trading MCP.

Included TradingView strategies:
- `tradingview/Asads_MAIN_v2_17_AUTOMATION.pine`
- `tradingview/Asads_SCALPER_v1_13_1_AUTOMATION.pine`

## One-time setup

1. Extract the folder somewhere permanent.
2. Run `SETUP_ONCE.cmd` once.
3. Complete the Robinhood Agentic authorization for the account that belongs to that PC/user.
4. After setup, launch with `BOT.cmd` or the desktop shortcut `Asads AUTOMATION`.

Normal startup does **not** reinstall Python, pip packages, or cloudflared. Use `REPAIR_SETUP.cmd` only when setup is damaged.

## TradingView alerts

Create a fresh alert after loading each automation Pine script.

For both MAIN and SCALPER:
- Condition: **Order fills only**
- Message: `{{strategy.order.alert_message}}`
- Webhook: use the current webhook shown by the BOT.

Do not use `alert() function calls only` and do not use an old alert created from a previous Pine version.

## Important operating rules

- The BOT owns live Robinhood position/order state; TradingView provides strategy intent.
- ENTRY alerts must pass freshness, current-Q​​QQ target/stop validity, buying-power, contract, spread, quote-age, and session checks.
- EXIT alerts are risk-reducing and are not blocked by manual-reconcile mode.
- Stale exits are cancel-confirmed before a replacement is submitted. Partial fills only replace the remaining quantity.
- The BOT has an independent QQQ stop watchdog and time/session failsafes for BOT-owned positions.
- A Cloudflare tunnel failure does not intentionally stop live Robinhood position management.
- User shutdown is refused while BOT-owned live exposure/order state remains.

## Session defaults

The package uses conservative QQQ 0DTE defaults:
- New-entry hard cutoff: 15:15 ET
- Sellout-derived entry cushion: 20 minutes
- Fallback force-flat: 15:40 ET
- Sellout-derived force-flat cushion: 5 minutes
- After-hours mode begins at 16:00 ET and stays alive while flat.

Robinhood can change trading/sellout handling. Contract `sellout_datetime` is used when available and malformed timestamps fail closed for new entries.

## Sizing

Default strategy budget: `$200` each, capped by live buying power and the global committed-capital limit. Contract quantity is always integer and is also capped by `selector.max_contracts_per_trade`.

Edit `config.yaml` deliberately; do not scale position size just because a short live sample was profitable.

## Learning foundation

`learning.enabled: true` is **shadow-only**. It records decisions, selector snapshots, entry fills, and exit fills to local JSONL files. It cannot approve/reject live trades or rewrite strategy rules.

News integration is intentionally disabled by default. See `RESEARCH.md` for the proposed learning/news architecture.

## Files to keep private

Never share the `state` folder. It can contain Robinhood OAuth/session material, webhook secrets, account-linked order state, and local journals.


## v0.7.8 hardening changes

- ENTRY alerts now fail closed unless LONG geometry is `target > entry > stop` or SHORT geometry is `target < entry < stop`.
- Packaged config requires a TradingView `TV_TS` on every ENTRY so legacy/replayed untimestamped entries cannot silently open risk.
- Risk-reducing EXIT/STOP alerts use a priority queue and the worker processes only a bounded number of webhook jobs per loop so position/order watchdogs cannot be starved by an alert burst.
- Webhook queues are bounded. A full queue returns HTTP 503 instead of falsely acknowledging a signal that cannot be retained.
- Failed/negative live cancel requests are retried after a bounded delay instead of becoming permanently stuck after the first network error.
- Broker option positions are reconciled periodically, not only on startup/new entry. Unmanaged/mismatched exposure remains visible to safety checks, blocks shutdown, and is rechecked before new risk is admitted.
- Shutdown is blocked when Robinhood reports unmanaged/mismatched option exposure.
- Legacy local endpoints that can mutate live execution state are disabled by default.
- Local clear-intent refuses to forget a submitted or registered live broker order.

## Option2 v1.9.1 automation-ready Pine

`tradingview/Option2_QQQ_Evolving_Auction_Interpreter_v1_9_1_AUTOMATION_READY.pine` is the uploaded Option2 v1.9.1 strategy with trading logic left intact and order-fill alerts standardized for this worker. Create the TradingView strategy alert as **Order fills only** and leave the compiler-provided message unchanged. See `OPTION2_ALERT_INTEGRATION_AUDIT.md`.

## v0.7.9 simple Option2 webhook vocabulary
Option2 now emits only `ARM CALL`, `ARM PUT`, `CALL`, `PUT`, and `EXIT` meanings. ARM is informational only. All actual close paths normalize to `EXIT`; detailed causes remain in `REASON` metadata. See `SIMPLE_WEBHOOK_PROTOCOL.md`.
