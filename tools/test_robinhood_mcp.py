from __future__ import annotations

import logging
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import load_config
from app.robinhood.mcp_client import RobinhoodMCPClient

ET = ZoneInfo("America/New_York")


def data(payload):
    d = payload.get("data") if isinstance(payload, dict) else None
    return d if isinstance(d, dict) else (payload or {})


def main() -> None:
    logging.getLogger("httpx2").setLevel(logging.WARNING)
    logging.getLogger("mcp.client.streamable_http").setLevel(logging.ERROR)
    cfg = load_config()
    state_dir = cfg.get("persistent_worker", {}).get("state_dir", "state")
    mcp_cfg = cfg.get("robinhood_mcp", {})
    client = RobinhoodMCPClient(
        url=mcp_cfg.get("url", "https://agent.robinhood.com/mcp/trading"),
        state_dir=state_dir,
        client_name=mcp_cfg.get("client_name", "Asads QQQ Trader"),
    )
    try:
        print("Connecting to Robinhood Trading MCP...")
        client.start(timeout=float(mcp_cfg.get("startup_timeout_seconds", 300)))
        print(f"Connected. Tools: {len(client.tool_names())}")

        accounts = data(client.call_tool("get_accounts", {})).get("accounts") or []
        agentic = [a for a in accounts if a.get("agentic_allowed") and a.get("state") == "active"]
        if not agentic:
            raise RuntimeError("No active Agentic account returned")
        acct = agentic[0]
        number = str(acct["account_number"])
        portfolio = data(client.call_tool("get_portfolio", {"account_number": number}))
        bp = (portfolio.get("buying_power") or {}).get("buying_power")
        print(f"Agentic account: ****{number[-4:]} | options={acct.get('option_level')} | buying power=${bp}")

        chains = data(client.call_tool("get_option_chains", {"underlying_symbol": "QQQ"})).get("chains") or []
        today = datetime.now(ET).date().isoformat()
        expirations = chains[0].get("expiration_dates", []) if chains else []
        print(f"QQQ 0DTE available today ({today}): {today in expirations}")

        started = time.perf_counter()
        quote = client.call_tool("get_equity_quotes", {"symbols": ["QQQ"]})
        ms = (time.perf_counter() - started) * 1000
        q = (data(quote).get("results") or [{}])[0].get("quote", {})
        print(f"QQQ last: {q.get('last_trade_price')} | equity quote call: {ms:.1f} ms")
        print("READ-ONLY MCP CHECK PASSED. No order tool was called.")
    finally:
        client.stop()


if __name__ == "__main__":
    main()
