# Option2 v1.9.1 alert integration audit

## Scope

The trading logic was intentionally left unchanged. Only TradingView alert plumbing was modified so executed strategy orders can be consumed by the existing hardened Python webhook parser.

## Material incompatibilities found in the uploaded Pine

The uploaded strategy used order-fill messages such as `ENTER CALL`, `ENTER PUT`, `ENTER AGAIN CALL`, `ENTER AGAIN PUT`, `SELL`, `SELL AGAIN`, and `EXIT TIME BLOCK`. The hardened Python parser does not treat those bare strings as actionable ENTRY/EXIT messages. Connecting that Pine directly to the webhook would therefore reject or ignore critical fills.

The script also contains informational `alert()` calls such as `ARM CALL` and `ARM PUT`. If the TradingView alert were accidentally configured for both order fills and `alert()` calls, those messages did not match the worker's harmless setup-only patterns.

## Changes

- Added `//@strategy_alert_message {{strategy.order.alert_message}} | TV_ORDER_ID {{strategy.order.id}} | TV_FILL {{strategy.order.price}}`.
- Every primary LONG/SHORT order fill now sends a parser-compatible message with absolute underlying ENTRY, TARGET, SL, `TV_TS`, and `SIG_ID`.
- Every re-entry order fill uses the same parser-compatible ENTRY contract with `OPT2V191_REENTRY` as its setup tag.
- Every stop, target, early exit, flow failure, profit-bank, protection, exhaustion, and time-block fill sends `SOURCE MAIN | EXIT QQQ | ...`.
- Informational ARM messages were changed to `ARM LONG QQQ` / `ARM SHORT QQQ` format so the worker ignores them safely if the alert is misconfigured to include `alert()` calls.
- Trading rules, conditions, stops, targets, strategy IDs, comments, and execution logic were not intentionally changed.

## Verification

- All 46 actionable order-alert parameters in the modified Pine are standardized; no raw legacy `ENTER CALL`, `SELL`, `SELL AGAIN`, or `EXIT TIME BLOCK` order alert messages remain.
- Every `strategy.entry`, `strategy.exit`, `strategy.close`, and `strategy.close_all` call has an actionable order-fill alert payload.
- Synthetic primary LONG, primary SHORT, re-entry, EXIT, and ARM messages pass the actual v0.7.8 parser/risk contract tests.
- Existing hardening tests plus the new Option2 alert-contract tests pass locally.

## TradingView setup

Use **Order fills only** for the strategy alert. The compiler annotation pre-populates the alert message. Leave the generated message unchanged. Set the webhook URL to the BOT's current HTTPS webhook URL. After changing Pine code or inputs, delete and recreate the TradingView alert because TradingView runs a saved snapshot of the script/inputs.
