# TradingView → Robinhood Autotrader

A Windows bot that takes **TradingView strategy alerts** (webhooks) and turns them into **QQQ 0DTE options orders** through **Robinhood's official Agentic Trading MCP connection**, with risk checks at every step.

Python, about **6,500 lines**, with tests. Version `0.7.9-hardened-simple-protocol`.

> Engineering portfolio project. It is not financial advice, and I don't claim it's profitable. Options are high-risk, so trade real money only if you understand exactly what the code does.

---

## How it works

```
TradingView strategy ──(alert webhook)──► Cloudflare tunnel ──► Bot (local Windows)
                                                                     │
                       signal parser → freshness/session checks → contract selector
                                                                     │
                                         risk engine (buying power, spread, quote age)
                                                                     │
                                       Robinhood Agentic Trading MCP ──► order
```

- **TradingView decides *what*** (entry/exit intent from the Pine strategies in `tradingview/`).
- **The bot owns the live state.** It tracks real positions and orders and refuses to act on stale or unsafe signals.

## Safety and hardening (the interesting part)

- **Entry checks:** alert freshness, valid target/stop for the current QQQ price, buying power, spread width, quote age, contract validity and trading session.
- **Exits always get through:** risk-reducing exits are never blocked, even in manual-reconcile mode.
- **No double orders:** a stale exit is cancel-confirmed before a replacement goes in, and partial fills only replace the remaining quantity.
- **Independent stop watchdog:** the bot watches QQQ itself and can flatten bot-owned positions even if TradingView goes quiet.
- **Session failsafes:** a hard cutoff for new entries, a force-flat time, and "sellout" timestamps that fail closed if malformed.
- **Shutdown protection:** the bot refuses to quit while it still owns live positions or orders.
- **Sizing caps:** a fixed per-strategy budget, a global committed-capital limit, and integer contracts with a max per trade.
- **Learning is shadow-only:** decisions and fills are logged to JSONL for research, and the logs can never approve trades or change rules.

Full details are in [`docs/HARDENING_AUDIT.md`](docs/HARDENING_AUDIT.md) and the alert format in [`docs/SIMPLE_WEBHOOK_PROTOCOL.md`](docs/SIMPLE_WEBHOOK_PROTOCOL.md).

## Project layout

| Path | What |
|---|---|
| `app/signal_parser.py` | Parses and validates TradingView alert messages |
| `app/contract_selector.py` | Picks the 0DTE contract (strike, spread, quote checks) |
| `app/risk.py` | Buying power, capital limits and sizing |
| `app/windowing.py` | Session windows, cutoffs and force-flat logic |
| `app/robinhood/` | MCP client and the persistent worker that manages live orders |
| `app/trade_journal.py` | Shadow-mode decision and fill logging |
| `tools/` | Desktop app, setup scripts, Cloudflare tunnel setup |
| `tradingview/` | The Pine Script strategies that send the alerts |
| `tests/` | Hardening and alert-contract tests |

## Setup (Windows)

1. Run `SETUP_ONCE.cmd`. It installs Python packages and downloads `cloudflared` for the webhook tunnel.
2. Authorize Robinhood Agentic Trading for your own account when prompted.
3. Set `webhook_secret` in `config.yaml` (it ships as `change-me`).
4. Launch with `BOT.cmd`, then create the TradingView alerts as described in [`docs/OPERATING_GUIDE.md`](docs/OPERATING_GUIDE.md).

Secrets, OAuth tokens, logs and the tunnel URL live in `state/`, which is git-ignored and never committed.

## Tests

```
pip install -r requirements.txt pytest
pytest tests/
```

---

*© Asad Kurbonov. Shared for portfolio purposes. Please ask before reusing.*
