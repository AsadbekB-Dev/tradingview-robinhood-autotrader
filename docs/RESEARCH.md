# v0.7.8 Hardened Audit + Research Notes

## What changed in this release

- MAIN and SCALPER use order-fill automation only.
- MAIN name cleaned to `Asads MAIN v2.17 • AUTOMATION`.
- SCALPER name cleaned to `Asads SCALPER v1.13.1 • AUTOMATION`.
- Large historical comment blocks were removed from the automation Pine copies without changing the intended trading formulas.
- TradingView execution timestamps (`TV_TS`) are attached to order-fill messages and checked again before broker submission.
- Exact MAIN/SCALPER source tags are required for new automation copies.
- Exit handling is idempotent and replacement-safe: no blind duplicate sell after an uncertain prior sell.
- Partial exit fills update owned quantity before any replacement.
- Natural-price execution is favored: ask for buys, bid for sells.
- Option ranking uses the *remaining* underlying move from current QQQ, not the stale original signal distance.
- Option ranking charges bid/ask friction and uses per-contract quote freshness.
- Exact ATM strikes are no longer accidentally omitted; the strike window can expand once if a small account cannot find an eligible contract nearby.
- Initial Cloudflare failure and later tunnel loss are treated as inbound-connectivity problems, not reasons to intentionally kill a ready Robinhood position manager.
- QQQ 0DTE timing was corrected to a 20-minute entry cushion and 5-minute force-flat cushion around Robinhood-provided sellout time, with 15:15/15:40 ET hard fallbacks.

## Research conclusions

### TradingView

Order-fill alerts are the correct automation surface because TradingView's broker emulator controls the actual strategy fill event and `strategy.order.alert_message` is evaluated when that order executes. Keep alerts on **Order fills only**.

Both strategies use `calc_on_every_tick = true`. TradingView documents that realtime tick execution can differ from historical bars because historical bars do not retain the full realtime tick stream. This means historical results can repaint or differ from the alerts that were actually emitted live. Treat live forward logs as the source of truth for execution validation.

TradingView webhooks time out after about three seconds. The BOT therefore acknowledges the webhook immediately and performs Robinhood work asynchronously.

References:
- https://www.tradingview.com/pine-script-docs/concepts/alerts/
- https://www.tradingview.com/pine-script-docs/concepts/strategies/
- https://www.tradingview.com/support/solutions/43000529348-how-to-configure-webhook-alerts/

### Robinhood execution

Robinhood describes the ask as the buyer's natural options price and the bid as the seller's natural price; moving away from the natural price trades execution speed for possible price improvement. For 0DTE automation, this release prioritizes fill certainty rather than working midpoint orders.

Robinhood currently states that same-day-expiring stock/ETF option positions cannot be newly opened after 15:30 ET and that it may begin closing at-risk expiring positions at 15:30 ET, or 15:45 ET for late-close options. The BOT remains deliberately earlier than those boundaries.

References:
- https://robinhood.com/us/en/learn/articles/what-is-a-bid-ask-spread/
- https://robinhood.com/us/en/support/articles/options-trading-hours/
- https://robinhood.com/us/en/support/articles/trading-with-your-agent/

### Cloudflare

Quick Tunnels are useful for testing but Cloudflare documents them as development/testing only and gives them random hostnames. The BOT now survives tunnel loss, but a named/remotely-managed tunnel with a stable hostname is still the better production endpoint.

Reference:
- https://developers.cloudflare.com/cloudflare-one/networks/connectors/cloudflare-tunnel/do-more-with-tunnels/trycloudflare/

## Adaptive learning design

Do **not** let a model rewrite live Pine or mutate risk rules from a few recent trades. Use a champion/challenger workflow:

1. **Collect**: strategy signal, market regime, option chain, selector ranking, actual fill, exit, latency, slippage, and P&L.
2. **Shadow model**: predict `TAKE/SKIP`, directional success, expected move, and expected option value while having zero influence on execution.
3. **Chronological validation**: train only on earlier data and evaluate on later untouched periods. Avoid random train/test splits that leak neighboring market regimes.
4. **Champion/challenger**: a new model can be promoted only after it beats the current model on out-of-sample expectancy, drawdown, calibration, and stability.
5. **Live gate later**: only after enough forward samples should a learner be allowed to veto the weakest setups. Risk limits and exit safety remain deterministic.

The first useful model is likely a tabular calibrated classifier/regressor (for example gradient boosting + probability calibration), not a giant neural network. With small live datasets, lower-complexity models are easier to audit and less likely to memorize noise.

## News integration

News should begin as **context**, not as `positive headline = CALL` logic. Suggested features:
- event timestamp / age
- symbols mentioned (QQQ + major constituents)
- category: Fed/macro, earnings, regulation, geopolitics, company-specific
- sentiment and sentiment magnitude
- novelty / repeated-story count
- market reaction since release
- whether a scheduled macro/earnings event is imminent

A feed such as Alpaca's realtime news WebSocket exposes headline, summary/content, symbols, and timestamps. A financial-domain sentiment model such as FinBERT can create a consistent sentiment feature. Neither should be allowed to trigger live orders by itself until shadow tests prove incremental value.

References:
- https://docs.alpaca.markets/us/docs/streaming-real-time-news
- https://arxiv.org/abs/2006.08097

## Next data upgrade

The current shadow logger learns from real strategy orders and option-selection alternatives. The highest-value future telemetry improvement is a **separate candidate stream** that records valid/near-valid MAIN and SCALPER setups that never become orders. That would let us study good moves the frozen strategies missed without contaminating the live action-only webhook path.
