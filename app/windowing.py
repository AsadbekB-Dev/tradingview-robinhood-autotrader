from __future__ import annotations
from typing import Any


def strike_window(
    rows: list[dict[str, Any]],
    share_price: float | None,
    each_side: int = 4,
) -> list[dict[str, Any]]:
    """Return a compact nearest-strike window around spot.

    Exact-ATM strikes are always eligible. Older versions used strictly ``<``
    and ``>`` comparisons and could accidentally omit the ATM contract whenever
    there were already enough strikes on both sides.
    """
    if not rows:
        return []
    target_count = max(int(each_side) * 2, 1)
    if share_price is None:
        return sorted(rows, key=lambda r: float(r["strike"]))[:target_count]

    # Nearest-by-distance is both simpler and correct at an exact strike. Tie
    # break by strike for deterministic output. This keeps the same total quote
    # count as before while guaranteeing ATM inclusion.
    chosen = sorted(
        rows,
        key=lambda r: (abs(float(r["strike"]) - float(share_price)), float(r["strike"])),
    )[:target_count]
    return sorted(chosen, key=lambda r: float(r["strike"]))
