"""Binance Spot combined-stream messages: URL building, parsing and Parquet schemas.

Only public market-data streams are used. No API key, no account endpoints.

Timestamps are UTC epoch milliseconds (int64). Prices and quantities are float64,
matching the rest of the repo. ``recv_time_ms`` is our local receive time; the
``depth5`` partial-book stream carries no exchange timestamp, so it is the only
clock those snapshots have.
"""

from __future__ import annotations

import json
from urllib.parse import urlsplit

import pyarrow as pa

AGG_TRADE = "aggTrade"
DEPTH5 = "depth5"
STREAM_KINDS = (AGG_TRADE, DEPTH5)
DEPTH_LEVELS = 5

DEFAULT_BASE_URL = "wss://stream.binance.com:9443"
ALLOWED_HOSTS = frozenset({"stream.binance.com", "data-stream.binance.vision"})

TRADE_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("event_time_ms", pa.int64(), nullable=False),
        pa.field("trade_time_ms", pa.int64(), nullable=False),
        pa.field("agg_trade_id", pa.int64(), nullable=False),
        pa.field("price", pa.float64(), nullable=False),
        pa.field("quantity", pa.float64(), nullable=False),
        pa.field("first_trade_id", pa.int64(), nullable=False),
        pa.field("last_trade_id", pa.int64(), nullable=False),
        pa.field("buyer_is_maker", pa.bool_(), nullable=False),
        pa.field("recv_time_ms", pa.int64(), nullable=False),
    ]
)

DEPTH_SCHEMA = pa.schema(
    [
        pa.field("symbol", pa.string(), nullable=False),
        pa.field("last_update_id", pa.int64(), nullable=False),
        pa.field("recv_time_ms", pa.int64(), nullable=False),
        *[
            pa.field(f"{side}_{what}_{lvl}", pa.float64(), nullable=False)
            for side in ("bid", "ask")
            for lvl in range(1, DEPTH_LEVELS + 1)
            for what in ("px", "qty")
        ],
    ]
)

GAP_SCHEMA = pa.schema(
    [
        pa.field("start_ms", pa.int64(), nullable=False),
        pa.field("end_ms", pa.int64(), nullable=False),
        pa.field("duration_ms", pa.int64(), nullable=False),
        pa.field("reason", pa.string(), nullable=False),
        pa.field("scope", pa.string(), nullable=False),
    ]
)

SCHEMAS = {AGG_TRADE: TRADE_SCHEMA, DEPTH5: DEPTH_SCHEMA}
# Column that must strictly increase per symbol; used for de-duplication and the audit.
SEQUENCE_COLUMN = {AGG_TRADE: "agg_trade_id", DEPTH5: "last_update_id"}


def normalize_symbols(symbols: list[str] | tuple[str, ...]) -> tuple[str, ...]:
    out = tuple(dict.fromkeys(s.strip().upper() for s in symbols if s.strip()))
    if not out:
        raise ValueError("at least one symbol is required")
    for s in out:
        if not s.isalnum():
            raise ValueError(f"invalid symbol: {s!r}")
    return out


def stream_names(symbols: list[str] | tuple[str, ...]) -> list[str]:
    names: list[str] = []
    for s in normalize_symbols(symbols):
        names += [f"{s.lower()}@aggTrade", f"{s.lower()}@depth5@100ms"]
    return names


def build_url(
    symbols: list[str] | tuple[str, ...],
    base_url: str = DEFAULT_BASE_URL,
    allowed_hosts: frozenset[str] | set[str] = ALLOWED_HOSTS,
) -> str:
    """Combined-stream URL. Refuses any host outside the allowlist."""
    parts = urlsplit(base_url)
    if parts.scheme not in ("ws", "wss"):
        raise ValueError(f"base_url must be ws:// or wss://, got {base_url!r}")
    if parts.hostname not in allowed_hosts:
        raise ValueError(f"host {parts.hostname!r} is not in the allowlist {sorted(allowed_hosts)}")
    return f"{base_url.rstrip('/')}/stream?streams={'/'.join(stream_names(symbols))}"


def _levels(side: str, levels: list) -> dict[str, float]:
    if len(levels) != DEPTH_LEVELS:
        raise ValueError(f"expected {DEPTH_LEVELS} {side} levels, got {len(levels)}")
    out: dict[str, float] = {}
    for i, (px, qty) in enumerate(levels, start=1):
        out[f"{side}_px_{i}"] = float(px)
        out[f"{side}_qty_{i}"] = float(qty)
    return out


def parse_message(raw: str | bytes, recv_time_ms: int) -> tuple[str, dict] | None:
    """Parse one combined-stream frame into ``(kind, row)``.

    Returns ``None`` for frames that carry no market data (subscription acks).
    Raises ``ValueError`` for anything malformed, so the caller can count and keep it.
    """
    try:
        msg = json.loads(raw)
    except (ValueError, TypeError) as e:
        raise ValueError(f"not JSON: {e}") from e
    if not isinstance(msg, dict):
        raise ValueError("frame is not a JSON object")
    stream, data = msg.get("stream"), msg.get("data")
    if stream is None and "result" in msg:
        return None
    if not isinstance(stream, str) or not isinstance(data, dict):
        raise ValueError("frame has no stream/data")

    symbol, _, rest = stream.partition("@")
    symbol = symbol.upper()
    try:
        if rest == "aggTrade":
            return AGG_TRADE, {
                "symbol": symbol,
                "event_time_ms": int(data["E"]),
                "trade_time_ms": int(data["T"]),
                "agg_trade_id": int(data["a"]),
                "price": float(data["p"]),
                "quantity": float(data["q"]),
                "first_trade_id": int(data["f"]),
                "last_trade_id": int(data["l"]),
                "buyer_is_maker": bool(data["m"]),
                "recv_time_ms": int(recv_time_ms),
            }
        if rest.startswith("depth5"):
            return DEPTH5, {
                "symbol": symbol,
                "last_update_id": int(data["lastUpdateId"]),
                "recv_time_ms": int(recv_time_ms),
                **_levels("bid", data["bids"]),
                **_levels("ask", data["asks"]),
            }
    except (KeyError, TypeError, ValueError) as e:
        raise ValueError(f"bad {rest} payload: {e!r}") from e
    raise ValueError(f"unexpected stream {stream!r}")
