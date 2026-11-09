# Option2 simple webhook protocol

TradingView emits only these command meanings for live automation:

- `ARM CALL` — informational/get-ready only. Never places an order.
- `ARM PUT` — informational/get-ready only. Never places an order.
- `CALL` — actionable call entry.
- `PUT` — actionable put entry.
- `EXIT` — actionable close. Stop, target, flow failure, time block, early exit, protection, exhaustion, re-entry exit, etc. are all `EXIT`; the cause is metadata in `REASON`.

`LEG PRIMARY` / `LEG REENTRY` and `REASON ...` are logging/ownership metadata. They do not create extra execution verbs.

The Pine strategy's market reasoning, entry/exit conditions, stop/target prices, quantities, session logic, and order types were not changed for this protocol update. Only alert payload construction and alert text were changed.
