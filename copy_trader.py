"""
Polymarket Copy Trader — Position Monitor & Copy Execution

Polls followed traders for new positions and mirrors their trades.
Includes kill switch, circuit breakers, and liquidity checks.
"""
import math
import threading
import time
import requests
from datetime import datetime, timezone
from typing import Optional

import psycopg2

from polymarket_client import PolymarketClient
from config import (
    MAX_DAILY_TRADES,
    MAX_SPREAD_PCT,
    MAX_TRADE_SIZE_USD,
    DAILY_LOSS_LIMIT_USD,
    ALERT_WEBHOOK_URL,
    PARTIAL_SIZE_CHANGE_THRESHOLD,
    TRADE_SIZE_FLOOR_PCT,
    TRADE_SIZE_CEILING_PCT,
    TRADE_SIZE_MIDPOINT,
    TRADE_SIZE_MAX_DISTANCE,
    TICK_SIZE,
    LOT_SIZE,
    MIN_ORDER_SHARES,
    MIN_NOTIONAL_USD,
    REALTIME_ALERT_MAX_POSITIONS,
    ACTIVE_TRADER_WINDOW_MINUTES,
)
from db import (
    get_followed_traders,
    log_copy_trade,
    log_pending_copy_trade,
    update_pending_to_open,
    update_pending_to_failed,
    is_trading_enabled,
    get_config,
    set_config,
    log_alert,
    get_followed_open_positions,
    upsert_followed_position,
    mark_position_closed,
    has_baseline_snapshot,
    get_copy_trade_for_token,
    update_copy_trade_status,
    update_copy_trade_exit,
    has_pending_copy_trade,
    get_orphaned_copy_trades,
    get_daily_realized_pnl,
    get_conn,
    release_conn,
    set_needs_order_for_new_entries,
    get_positions_needing_orders,
    get_all_wallets_needing_orders,
    clear_needs_order,
    increment_needs_order_attempts,
    get_trades_needing_sell,
    clear_needs_sell,
    set_needs_sell,
)


MAX_NEEDS_ORDER_ATTEMPTS = 3  # Clear needs_order after this many failed BUY attempts


# ============================================================
# Order sizing helpers
# ============================================================

def round_to_tick(value: float, tick: float = LOT_SIZE) -> float:
    """
    Round *down* to the nearest multiple of `tick` so we never over-size an
    order past the caller's intent. Uses math.floor to avoid floating-point
    noise creeping past a lot/tick boundary.

    Examples (tick=0.01):
        round_to_tick(1.0037) -> 1.00
        round_to_tick(5.999)  -> 5.99
        round_to_tick(0.005)  -> 0.00

    Raises ValueError on non-positive tick so a misconfigured caller can't
    silently bypass rounding and send an off-tick order to the CLOB.
    """
    if tick <= 0:
        raise ValueError(f"round_to_tick requires tick > 0, got {tick}")
    if value <= 0:
        return 0.0
    return round(math.floor(value / tick) * tick, 8)


def calculate_trade_size(account_balance: float, entry_price: float) -> float:
    """
    U-shaped sizing: high probability (90¢+) and high EV (60-70¢) get more capital.
    Dead zone around 80¢ gets minimum allocation.
    Floor and ceiling scale with account balance.
    """
    if entry_price <= 0 or entry_price >= 1 or account_balance <= 0:
        return 0.0

    floor = account_balance * TRADE_SIZE_FLOOR_PCT
    ceiling = account_balance * TRADE_SIZE_CEILING_PCT

    distance = abs(entry_price - TRADE_SIZE_MIDPOINT)
    size_range = ceiling - floor

    size = floor + (distance * size_range / TRADE_SIZE_MAX_DISTANCE)
    size = min(size, ceiling)
    size = min(size, MAX_TRADE_SIZE_USD)  # Absolute hard cap
    size = max(size, floor)
    size = round_to_tick(size, LOT_SIZE)

    shares = size / entry_price
    if shares < MIN_ORDER_SHARES or size < MIN_NOTIONAL_USD:
        return 0.0

    return size


def make_idempotency_key(source_wallet: str, asset_id: str) -> str:
    """
    Build a stable idempotency key for a (source trader, token) pair.

    This deliberately does NOT include a timestamp — two copy orders for the
    same (trader, token) while one is still OPEN should collapse into one,
    regardless of when the second one was detected.
    """
    return f"{source_wallet}:{asset_id}"


def is_order_filled(order_result) -> bool:
    """
    Determine whether a CLOB order response represents a successful placement.

    Real CLOB response shape:
        {"success": True, "status": "live"|"matched"|"filled", "orderID": "0x...",
         "errorMsg": "", "takingAmount": "", "makingAmount": ""}

    - success must be True
    - status "live" = limit order on the book (accepted, not yet matched)
    - status "matched"/"filled" = immediately filled
    - Any other status or success=False → not filled
    """
    if not order_result:
        return False
    if not isinstance(order_result, dict):
        print(f"[ORDER] is_order_filled: unexpected response type {type(order_result).__name__}, treating as unfilled")
        return False

    if order_result.get("success") is not True:
        return False
    if order_result.get("errorMsg"):
        return False

    status = str(order_result.get("status", "")).lower()
    if status in ("live", "matched", "filled"):
        return True

    print(f"[ORDER] is_order_filled: unexpected status '{status}' in response: {order_result}")
    return False


def extract_fill_shares(order_result: dict) -> float:
    """
    Extract actual filled share quantity from CLOB order response.

    Real CLOB response shape:
        {"success": True, "status": "live"|"matched"|"filled",
         "orderID": "0x...", "errorMsg": "",
         "takingAmount": "", "makingAmount": ""}

    - "live" status = limit order on the book, no shares filled yet → return 0
    - "matched" status = check takingAmount and makingAmount for fill data
    """
    if not order_result or not isinstance(order_result, dict):
        return 0.0

    status = str(order_result.get("status", "")).lower()

    # "live" means the order is sitting on the book — nothing filled yet
    if status == "live":
        return 0.0

    # For "matched"/"filled", check takingAmount and makingAmount
    if status in ("matched", "filled"):
        for field in ["takingAmount", "makingAmount"]:
            val = order_result.get(field)
            if val:
                try:
                    f = float(val)
                    if f > 0:
                        return f
                except (ValueError, TypeError):
                    continue

    # Fallback: try legacy field names
    for field in ["filledSize", "filled_size", "matchedSize", "matched_size",
                   "sizeMatched", "size", "executedQty"]:
        val = order_result.get(field)
        if val is not None:
            try:
                f = float(val)
                if f > 0:
                    return f
            except (ValueError, TypeError):
                continue

    print(f"[ORDER] WARNING: Could not extract fill shares from response: {order_result}")
    return 0.0


def extract_fill_price(order_result: dict) -> float:
    """
    Extract average fill price from CLOB order response.

    Real CLOB response shape:
        {"success": True, "status": "live"|"matched"|"filled",
         "orderID": "0x...", "errorMsg": "",
         "takingAmount": "", "makingAmount": ""}

    The CLOB response has no explicit fill price field.
    - "live" = limit order on book, no fill yet -> return 0 (caller uses order price)
    - "matched"/"filled" = check for price fields from py-clob-client wrapper
    """
    if not order_result or not isinstance(order_result, dict):
        return 0.0

    status = str(order_result.get("status", "")).lower()
    if status == "live":
        return 0.0

    for field in ["avgPrice", "avg_price", "price", "averagePrice"]:
        val = order_result.get(field)
        if val is not None:
            try:
                f = float(val)
                if f > 0:
                    return f
            except (ValueError, TypeError):
                continue

    return 0.0


client = PolymarketClient()


# ============================================================
# Active-trader tracking (thread-safe, adaptive polling)
# ============================================================

_active_traders_lock = threading.Lock()
_active_traders = {}   # {proxy_wallet: datetime_of_last_activity}
_last_polled_at = {}   # {proxy_wallet: datetime_of_last_poll}


def add_active_trader(wallet: str):
    """Mark a trader as recently active (thread-safe)."""
    with _active_traders_lock:
        _active_traders[wallet] = datetime.now(timezone.utc)


def get_active_traders_snapshot() -> dict:
    """Return a snapshot copy of active traders (thread-safe)."""
    with _active_traders_lock:
        return dict(_active_traders)


def update_last_polled(wallet: str):
    """Record when we last polled this wallet (thread-safe)."""
    with _active_traders_lock:
        _last_polled_at[wallet] = datetime.now(timezone.utc)


def was_recently_polled(wallet: str, seconds: int = 25) -> bool:
    """Check if wallet was polled within the last N seconds (thread-safe)."""
    with _active_traders_lock:
        last = _last_polled_at.get(wallet)
        if not last:
            return False
        return (datetime.now(timezone.utc) - last).total_seconds() < seconds


def prune_stale_active_traders(window_minutes: int):
    """Remove traders who haven't had activity within the window (thread-safe)."""
    with _active_traders_lock:
        now = datetime.now(timezone.utc)
        stale = [w for w, t in _active_traders.items()
                 if (now - t).total_seconds() > window_minutes * 60]
        for w in stale:
            del _active_traders[w]


# ============================================================
# Order execution (via Madrid proxy)
# ============================================================

def get_token_market_params(condition_id: str) -> tuple:
    """Fetch tick_size and neg_risk for a market from the CLOB API."""
    if not condition_id:
        return "0.01", False
    try:
        info = client.get_market_info(condition_id)
        if info:
            tick_size = str(info.get("minimum_tick_size", "0.01"))
            neg_risk = bool(info.get("neg_risk", False))
            return tick_size, neg_risk
    except Exception as e:
        print(f"[ORDER] Failed to fetch market params for {condition_id[:30]}: {e}")
    return "0.01", False


def place_copy_order(token_id: str, side: str, size: float, price: float,
                     tick_size: str = "0.01", neg_risk: bool = False) -> dict:
    """Place an order via the Madrid order proxy."""
    import requests
    from config import ORDER_PROXY_URL, ORDER_PROXY_AUTH_TOKEN

    try:
        resp = requests.post(
            f"{ORDER_PROXY_URL}/execute-order",
            json={
                "token_id": token_id,
                "side": side,
                "size": size,
                "price": price,
                "tick_size": tick_size,
                "neg_risk": neg_risk,
            },
            headers={"Authorization": f"Bearer {ORDER_PROXY_AUTH_TOKEN}"},
            timeout=30,
        )
        data = resp.json()
        if data.get("success"):
            return data.get("order_result", {})
        else:
            print(f"[ORDER] Proxy error: {data.get('error')}")
            return None
    except Exception as e:
        print(f"[ORDER] Proxy request failed: {e}")
        return None


def place_exit_order(token_id: str, shares: float, price: float,
                     title: str = "", condition_id: str = "") -> Optional[dict]:
    """Sell/exit a position by selling all shares via proxy."""
    exit_tick, exit_neg = get_token_market_params(condition_id)
    return place_copy_order(token_id=token_id, side="SELL", size=shares, price=price,
                            tick_size=exit_tick, neg_risk=exit_neg)


def get_order_price(token_id: str, side: str = "buy") -> float:
    """Fetch the current price for a token from the Madrid proxy."""
    import requests
    from config import ORDER_PROXY_URL, ORDER_PROXY_AUTH_TOKEN
    try:
        resp = requests.get(
            f"{ORDER_PROXY_URL}/price",
            params={"token_id": token_id, "side": side},
            headers={"Authorization": f"Bearer {ORDER_PROXY_AUTH_TOKEN}"},
            timeout=15,
        )
        data = resp.json()
        return float(data.get("price", 0))
    except Exception as e:
        print(f"[PRICE] Proxy request failed: {e}")
        return 0.0


# ============================================================
# Position delta detection
# ============================================================

def detect_new_positions(wallet: str, username: str = "") -> tuple:
    """
    Compare current positions against followed_positions table.
    Returns (new_entries, exits, api_position_count, partial_entries, partial_exits):
      - new_entries: positions the trader just entered (not pre-existing)
      - exits: positions the trader just fully exited
      - api_position_count: total number of open positions in the API response
      - partial_entries: positions the trader added to by >PARTIAL_SIZE_CHANGE_THRESHOLD
      - partial_exits: positions the trader reduced by >PARTIAL_SIZE_CHANGE_THRESHOLD

    New entries are saved with needs_order=FALSE. The caller is responsible for
    setting needs_order=TRUE via set_needs_order_for_new_entries() after checking
    whether the trader is high-volume (>REALTIME_ALERT_MAX_POSITIONS).
    """
    # Get current positions from API. size_threshold=0 so dust positions
    # stay visible and don't trigger false exits when sizes drift near the cut.
    current = client.get_positions(wallet, size_threshold=0)
    if current is None:
        return [], [], 0, [], []

    current_assets = {pos.get("asset", ""): pos for pos in current if pos.get("asset")}

    # Check if this is the first time we're seeing this trader
    has_baseline = has_baseline_snapshot(wallet)

    if not has_baseline:
        # First time — save all current positions as pre-existing baseline
        for pos in current:
            asset_id = pos.get("asset", "")
            if not asset_id:
                continue
            upsert_followed_position({
                "proxy_wallet": wallet,
                "asset_id": asset_id,
                "condition_id": pos.get("conditionId", ""),
                "title": pos.get("title", ""),
                "slug": pos.get("slug", ""),
                "outcome": pos.get("outcome", ""),
                "outcome_index": pos.get("outcomeIndex", 0),
                "size": pos.get("size", 0),
                "avg_price": pos.get("avgPrice", 0),
                "entry_price": pos.get("curPrice", 0),
                "current_price": pos.get("curPrice", 0),
                "current_value": pos.get("currentValue", 0),
                "pre_existing": True,
            })
        print(f"[POLL] {username or wallet[:10]}: API={len(current)} positions, DB=0 tracked, baseline saved, new=0, exits=0")
        return [], [], len(current), [], []

    # Get what we're currently tracking
    tracked = get_followed_open_positions(wallet)
    tracked_assets = {p["asset_id"]: p for p in tracked}
    print(f"[POLL] {username or wallet[:10]}: API={len(current_assets)} positions, DB={len(tracked_assets)} tracked")

    new_entries = []
    partial_entries = []
    partial_exits = []

    for asset_id, pos in current_assets.items():
        if asset_id not in tracked_assets:
            # New position — save and flag as entry
            upsert_followed_position({
                "proxy_wallet": wallet,
                "asset_id": asset_id,
                "condition_id": pos.get("conditionId", ""),
                "title": pos.get("title", ""),
                "slug": pos.get("slug", ""),
                "outcome": pos.get("outcome", ""),
                "outcome_index": pos.get("outcomeIndex", 0),
                "size": pos.get("size", 0),
                "avg_price": pos.get("avgPrice", 0),
                "entry_price": pos.get("curPrice", 0),
                "current_price": pos.get("curPrice", 0),
                "current_value": pos.get("currentValue", 0),
                "pre_existing": False,
            })
            new_entries.append(pos)
        else:
            # Existing position — check for size changes before upsert so we
            # can compare against the prior poll's recorded size.
            tracked_row = tracked_assets[asset_id]
            current_size = float(pos.get("size", 0) or 0)
            try:
                previous_size = float(tracked_row.get("size_at_last_poll", 0) or 0)
            except (TypeError, ValueError):
                previous_size = 0.0

            if previous_size > 0:
                size_change_pct = (current_size - previous_size) / previous_size
                if size_change_pct <= -PARTIAL_SIZE_CHANGE_THRESHOLD:
                    partial_exits.append({
                        "asset_id": asset_id,
                        "condition_id": pos.get("conditionId", ""),
                        "title": pos.get("title", ""),
                        "outcome": pos.get("outcome", ""),
                        "old_size": previous_size,
                        "new_size": current_size,
                        "change_pct": size_change_pct,
                        "pre_existing": bool(tracked_row.get("pre_existing", False)),
                    })
                elif size_change_pct >= PARTIAL_SIZE_CHANGE_THRESHOLD:
                    partial_entries.append({
                        "asset_id": asset_id,
                        "condition_id": pos.get("conditionId", ""),
                        "title": pos.get("title", ""),
                        "outcome": pos.get("outcome", ""),
                        "old_size": previous_size,
                        "new_size": current_size,
                        "change_pct": size_change_pct,
                        "pre_existing": bool(tracked_row.get("pre_existing", False)),
                    })

            upsert_followed_position({
                "proxy_wallet": wallet,
                "asset_id": asset_id,
                "condition_id": pos.get("conditionId", ""),
                "title": pos.get("title", ""),
                "slug": pos.get("slug", ""),
                "outcome": pos.get("outcome", ""),
                "outcome_index": pos.get("outcomeIndex", 0),
                "size": pos.get("size", 0),
                "avg_price": pos.get("avgPrice", 0),
                "entry_price": tracked_row.get("entry_price", 0),
                "current_price": pos.get("curPrice", 0),
                "current_value": pos.get("currentValue", 0),
                "pre_existing": tracked_row.get("pre_existing", False),
            })

    # Detect full EXITS (tracked but not in current API response)
    exits = []
    for asset_id, tracked_pos in tracked_assets.items():
        if asset_id not in current_assets:
            mark_position_closed(wallet, asset_id)
            exits.append(tracked_pos)

    print(
        f"[POLL] {username or wallet[:10]}: "
        f"new={len(new_entries)}, exits={len(exits)}, "
        f"partial_entries={len(partial_entries)}, partial_exits={len(partial_exits)}"
    )
    return new_entries, exits, len(current_assets), partial_entries, partial_exits


# ============================================================
# Trade execution checks
# ============================================================

def check_spread(token_id: str) -> tuple:
    """
    Check bid-ask spread. Returns (acceptable, spread_pct).
    """
    spread_data = client.get_spread(token_id)
    if not spread_data:
        return False, 1.0

    spread = float(spread_data.get("spread", 1.0))
    mid = float(spread_data.get("mid", 0.5))

    if mid <= 0:
        return False, 1.0

    spread_pct = spread / mid
    return spread_pct <= MAX_SPREAD_PCT, spread_pct


def get_daily_trade_count() -> int:
    """Count how many copy trades we've made today."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM copy_trades
                WHERE copied_at::date = CURRENT_DATE
            """)
            return cur.fetchone()[0]
    finally:
        release_conn(conn)


# ============================================================
# Alerting (n8n webhook)
# ============================================================

def send_alert(alert_type: str, payload: dict):
    """Send alert via n8n webhook and log it."""
    log_alert(alert_type, payload)

    if not ALERT_WEBHOOK_URL:
        print(f"[ALERT] No webhook configured: {alert_type}")
        return

    webhook_data = {
        "alert_type": alert_type,
        "payload": payload,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }

    if alert_type == "BATCH_ENTRY":
        webhook_data["subject"] = f"{payload.get('source_username', 'unknown')}: {payload.get('count', 0)} new position(s)"
        webhook_data["body"] = (
            f"New Positions Detected\n"
            f"Trader: {payload.get('source_username', 'unknown')}\n"
            f"Count: {payload.get('count', 0)}\n"
            f"{'='*40}\n"
            f"{payload.get('entries', '')}"
        )
    elif alert_type == "BATCH_EXIT":
        webhook_data["subject"] = f"{payload.get('source_username', 'unknown')}: {payload.get('count', 0)} exit(s)"
        webhook_data["body"] = (
            f"Position Exits Detected\n"
            f"Trader: {payload.get('source_username', 'unknown')}\n"
            f"Count: {payload.get('count', 0)}\n"
            f"{'='*40}\n"
            f"{payload.get('exits', '')}"
        )
    elif alert_type == "PARTIAL_EXIT":
        webhook_data["subject"] = f"{payload.get('source_username', 'unknown')}: position size reduced"
        webhook_data["body"] = (
            f"Partial Exit Detected\n"
            f"Trader: {payload.get('source_username', 'unknown')}\n"
            f"{payload.get('details', '')}"
        )
    elif alert_type == "PARTIAL_ENTRY":
        webhook_data["subject"] = f"{payload.get('source_username', 'unknown')}: position size increased"
        webhook_data["body"] = (
            f"Position Add-On Detected\n"
            f"Trader: {payload.get('source_username', 'unknown')}\n"
            f"{payload.get('details', '')}"
        )
    elif alert_type == "DAILY_SUMMARY":
        webhook_data["subject"] = "Polymarket Copy Trader - Daily Summary"
        webhook_data["body"] = (
            f"Daily Summary\n"
            f"Trades today: {payload.get('trades_today', 0)}\n"
            f"Open positions: {payload.get('open_positions', 0)}\n"
            f"Daily PnL: ${payload.get('daily_pnl', 0):.2f}\n"
            f"Followed traders: {payload.get('followed_count', 0)}"
        )
    elif alert_type == "CIRCUIT_BREAKER":
        webhook_data["subject"] = "ALERT: Circuit Breaker Triggered"
        webhook_data["body"] = f"Reason: {payload.get('reason', 'unknown')}"
    else:
        webhook_data["subject"] = f"Polymarket Alert: {alert_type}"
        webhook_data["body"] = str(payload)

    try:
        resp = requests.post(
            ALERT_WEBHOOK_URL,
            json=webhook_data,
            timeout=10,
        )
        if resp.status_code < 300:
            print(f"[ALERT] Webhook sent: {webhook_data['subject']}")
        else:
            print(f"[ALERT] Webhook failed ({resp.status_code}): {webhook_data['subject']}")
    except Exception as e:
        print(f"[ALERT] Webhook error: {e}")


# ============================================================
# Main polling loop — three-job architecture
#
# Normal poller  (every 2 min):  detection + DB writes + alerts. NO orders.
# Active poller  (every 30 sec): fast-poll active traders, places BUY/SELL.
# Reconciliation (every 5 min):  catches orphaned positions, places SELL.
# ============================================================

def poll_followed_traders(dry_run: bool = True, mode: str = "normal"):
    """
    Single poll cycle: check followed traders for position changes.

    mode="normal"  — poll ALL followed traders. Detection and alerts only.
                     Never places orders. Adds active traders to the fast-poll set.
    mode="active"  — poll only recently-active traders. Places BUY and SELL orders.
    """
    followed = get_followed_traders()
    if not followed:
        print("[POLL] No followed traders. Run trader_ranker.py first.")
        return

    if mode == "active":
        prune_stale_active_traders(ACTIVE_TRADER_WINDOW_MINUTES)

        # Rehydrate: ensure wallets with pending DB flags are in the active set,
        # even after a process restart or if they were pruned from _active_traders.
        for pending_wallet in get_all_wallets_needing_orders():
            add_active_trader(pending_wallet)

        active_snapshot = get_active_traders_snapshot()
        if not active_snapshot:
            return  # no active traders — skip entirely

        active_wallets = set(active_snapshot.keys())
        followed = [t for t in followed if t["proxy_wallet"] in active_wallets]
        if not followed:
            return

        # Skip traders polled within the last 25 seconds
        followed = [t for t in followed if not was_recently_polled(t["proxy_wallet"], 25)]
        if not followed:
            return

        print(f"[POLL-ACTIVE] Checking {len(followed)} active traders...")
    else:
        print(f"[POLL-NORMAL] Checking {len(followed)} followed traders...")

    # Active mode: fetch balance and daily trade count for order execution
    live_trading = False
    account_balance = 0.0
    daily_trades = 0
    max_daily = MAX_DAILY_TRADES

    if mode == "active":
        live_trading = is_trading_enabled() and not dry_run

        account_balance = client.get_own_balance()
        if account_balance > 0:
            print(f"[POLL-ACTIVE] Account balance: ${account_balance:.2f}")
        else:
            print(f"[POLL-ACTIVE] Balance $0 — no orders this cycle")

        try:
            daily_trades = get_daily_trade_count()
        except Exception as e:
            print(f"[POLL-ACTIVE] Could not fetch daily trade count: {e}")
            daily_trades = 0
        try:
            max_daily = int(get_config("max_daily_trades") or MAX_DAILY_TRADES)
        except (TypeError, ValueError):
            max_daily = MAX_DAILY_TRADES

        # B-02 fix: check daily realized loss limit before placing any orders
        if live_trading:
            try:
                daily_pnl = get_daily_realized_pnl()
                if daily_pnl < -DAILY_LOSS_LIMIT_USD:
                    set_config("kill_switch", "OFF")
                    send_alert("CIRCUIT_BREAKER", {
                        "reason": f"Daily loss limit exceeded: ${daily_pnl:.2f} (limit: -${DAILY_LOSS_LIMIT_USD:.2f})",
                    })
                    print(f"[SAFETY] Daily loss limit exceeded (${daily_pnl:.2f}), kill switch auto-disabled")
                    live_trading = False
            except Exception as e:
                print(f"[POLL-ACTIVE] Could not check daily PnL: {e}")

    for trader in followed:
        wallet = trader["proxy_wallet"]
        username = trader.get("username", wallet[:10])

        (
            new_entries,
            exits,
            api_position_count,
            partial_entries,
            partial_exits,
        ) = detect_new_positions(wallet, username)

        # Record poll timestamp (thread-safe)
        update_last_polled(wallet)

        # Mark trader as active if any changes detected (thread-safe)
        if new_entries or exits:
            add_active_trader(wallet)

        # High-volume traders: track in DB but suppress alerts and orders
        high_volume = api_position_count > REALTIME_ALERT_MAX_POSITIONS
        if high_volume:
            total_changes = (
                len(new_entries) + len(exits)
                + len(partial_entries) + len(partial_exits)
            )
            if total_changes > 0:
                print(f"[POLL] {username}: {total_changes} changes tracked silently (high-volume)")
            continue

        # Normal poller: set needs_order flag for new entries so the active
        # poller can pick them up. This is a no-op if new_entries is empty.
        if mode == "normal" and new_entries:
            new_asset_ids = [p.get("asset", "") for p in new_entries if p.get("asset")]
            set_needs_order_for_new_entries(wallet, new_asset_ids)

        # Normal poller: set needs_sell flag on our OPEN copy_trades when
        # the source trader exits, so the active poller can place SELL orders.
        if mode == "normal" and exits:
            for ex_pos in exits:
                ex_token = ex_pos.get("asset_id", "")
                if not ex_token:
                    continue
                if ex_pos.get("pre_existing", False):
                    continue
                our_trade = get_copy_trade_for_token(wallet, ex_token)
                if our_trade and our_trade.get("status") == "OPEN":
                    set_needs_sell(our_trade["id"])
                    print(f"[POLL-NORMAL] {username}: flagged needs_sell on copy_trade {our_trade['id']} for '{ex_pos.get('title', '?')}'")

        # ===== NEW ENTRIES =====
        if new_entries:
            label = "POLL-ACTIVE" if mode == "active" else "POLL-NORMAL"
            print(f"[{label}] {username}: {len(new_entries)} new entry(s) detected!")

            entry_lines = []
            for pos in new_entries:
                title = pos.get("title", "unknown")
                outcome = pos.get("outcome", "?")
                token_id = pos.get("asset", "")

                try:
                    entry_price = float(pos.get("curPrice") or 0.50)
                except (ValueError, TypeError):
                    entry_price = 0.50

                size_usd = calculate_trade_size(account_balance, entry_price)

                status = "DETECTED"

                # Only the active poller places BUY orders
                if mode == "active" and live_trading and token_id and size_usd > 0:
                    idem_key = make_idempotency_key(wallet, token_id)

                    if has_pending_copy_trade(idem_key):
                        print(f"[POLL-ACTIVE] {username}: already have open copy_trade for {token_id[:16]}..., skipping")
                        status = "DUPLICATE"
                    elif not is_trading_enabled():
                        status = "KILL SWITCH"
                    elif daily_trades >= max_daily:
                        send_alert("CIRCUIT_BREAKER", {
                            "reason": f"Daily trade limit reached ({daily_trades}/{max_daily})",
                        })
                        status = "CIRCUIT BREAKER"
                    elif account_balance < MIN_NOTIONAL_USD:
                        print(f"[POLL-ACTIVE] Balance exhausted (${account_balance:.2f}), stopping orders")
                        status = "NO BALANCE"
                    else:
                        spread_ok, spread_pct = check_spread(token_id)
                        if not spread_ok:
                            print(f"[POLL-ACTIVE] {username}: spread too wide ({spread_pct:.1%}) on '{title}', skipping")
                            status = f"SKIP (spread {spread_pct:.1%})"
                        else:
                            order_price = get_order_price(token_id, side="buy")
                            if order_price <= 0:
                                print(f"[POLL-ACTIVE] {username}: could not fetch price for '{title}', skipping")
                                status = "NO PRICE"
                            else:
                                buy_size = round_to_tick(size_usd / order_price, LOT_SIZE)
                                print(f"[POLL-ACTIVE] U-size: ${size_usd:.2f} (balance=${account_balance:.0f}, price={order_price:.2f}, shares={buy_size:.2f})")
                                mkt_tick, mkt_neg = get_token_market_params(pos.get("conditionId", ""))

                                # B-04 fix: write PENDING record BEFORE placing the order.
                                # If we crash after the order but before the DB update,
                                # the PENDING record survives for reconciliation.
                                pending_id = None
                                try:
                                    pending_id = log_pending_copy_trade({
                                        "source_wallet": wallet,
                                        "source_username": username,
                                        "condition_id": pos.get("conditionId", ""),
                                        "token_id": token_id,
                                        "market_title": title,
                                        "market_slug": pos.get("slug", ""),
                                        "outcome": outcome,
                                        "side": "BUY",
                                        "size_usd": size_usd,
                                        "idempotency_key": idem_key,
                                    })
                                except psycopg2.errors.UniqueViolation:
                                    print(
                                        f"[POLL-ACTIVE] {username}: DUPLICATE — "
                                        f"PENDING/OPEN record already exists for {idem_key}"
                                    )
                                    send_alert("DUPLICATE_ORDER", {
                                        "source_username": username,
                                        "title": title,
                                        "idempotency_key": idem_key,
                                    })
                                    status = "DUPLICATE"

                                if pending_id:
                                    order_result = place_copy_order(
                                        token_id=token_id,
                                        side="BUY",
                                        size=buy_size,
                                        price=order_price,
                                        tick_size=mkt_tick,
                                        neg_risk=mkt_neg,
                                    )
                                    if is_order_filled(order_result):
                                        actual_shares = extract_fill_shares(order_result)
                                        actual_price = extract_fill_price(order_result)
                                        if actual_shares <= 0:
                                            print(f"[ORDER] WARNING: Using estimated shares (extraction failed)")
                                            actual_shares = size_usd / order_price if order_price > 0 else 0
                                        if actual_price <= 0:
                                            actual_price = order_price
                                        order_id = str(order_result.get("orderID", "") or order_result.get("orderId", ""))
                                        update_pending_to_open(pending_id, order_id, actual_price, actual_shares)
                                        daily_trades += 1
                                        account_balance -= size_usd
                                        status = "EXECUTED"
                                    else:
                                        update_pending_to_failed(pending_id)
                                        print(f"[POLL-ACTIVE] {username}: order not filled on '{title}' (FOK killed)")
                                        status = "NOT FILLED"

                entry_lines.append(
                    f"  - {outcome} @ {entry_price} — {title} [Size: ${size_usd:.2f}] [{status}]"
                )

            send_alert("BATCH_ENTRY", {
                "source_username": username,
                "count": len(entry_lines),
                "entries": "\n".join(entry_lines),
                "live_trading": live_trading,
            })

        # ===== FULL EXITS =====
        if exits:
            label = "POLL-ACTIVE" if mode == "active" else "POLL-NORMAL"
            print(f"[{label}] {username}: {len(exits)} exit(s) detected!")

            exit_lines = []
            for pos in exits:
                title = pos.get("title", "unknown")
                outcome = pos.get("outcome", "?")
                pre_existing = pos.get("pre_existing", False)
                token_id = pos.get("asset_id", "")

                exit_status = "DETECTED"

                # Only the active poller places SELL orders
                if mode == "active" and live_trading and not pre_existing and token_id:
                    our_trade = get_copy_trade_for_token(wallet, token_id)
                    if our_trade and our_trade.get("status") == "OPEN":
                        try:
                            raw_shares = float(our_trade.get("shares", 0) or 0)
                        except (TypeError, ValueError):
                            raw_shares = 0.0

                        shares_to_sell = round_to_tick(raw_shares, LOT_SIZE)

                        if shares_to_sell <= 0:
                            update_copy_trade_exit(our_trade["id"], "CLOSED", None)
                            exit_status = "DUST CLOSED"
                            print(f"[POLL-ACTIVE] {username}: residual {raw_shares} shares below lot, marking CLOSED")
                        elif not is_trading_enabled():
                            exit_status = "KILL SWITCH"
                        else:
                            sell_price = get_order_price(token_id, side="sell")
                            if sell_price <= 0:
                                print(f"[POLL-ACTIVE] {username}: could not fetch sell price for '{title}', skipping")
                                exit_status = "NO PRICE"
                            else:
                                sell_result = place_exit_order(
                                    token_id=token_id,
                                    shares=shares_to_sell,
                                    price=sell_price,
                                    title=title,
                                    condition_id=pos.get("condition_id", ""),
                                )
                                if is_order_filled(sell_result):
                                    exit_price = extract_fill_price(sell_result)
                                    update_copy_trade_exit(our_trade["id"], "CLOSED", exit_price if exit_price > 0 else None)
                                    account_balance += (exit_price * shares_to_sell) if exit_price > 0 else 0
                                    exit_status = f"SOLD {shares_to_sell:.2f}"
                                    if exit_price > 0:
                                        entry_p = float(our_trade.get("entry_price", 0) or 0)
                                        pnl = (exit_price - entry_p) * shares_to_sell if entry_p > 0 else 0
                                        print(f"[POLL-ACTIVE] {username}: sold {shares_to_sell:.2f} of '{title}' PnL=${pnl:+.2f}")
                                    else:
                                        print(f"[POLL-ACTIVE] {username}: sold {shares_to_sell:.2f} of '{title}'")
                                else:
                                    # C4 fix: don't mark closed — reconciliation will retry
                                    print(f"[POLL-ACTIVE] {username}: SELL not filled on '{title}' (FOK killed) — reconciliation will retry")
                                    exit_status = "SELL FAILED (will retry)"
                    else:
                        exit_status = "NO COPY TRADE"

                exit_lines.append(
                    f"  - {outcome} — {title} (pre-existing: {pre_existing}) [{exit_status}]"
                )

            send_alert("BATCH_EXIT", {
                "source_username": username,
                "count": len(exits),
                "exits": "\n".join(exit_lines),
            })

        # ===== PARTIAL EXITS (alert only, no auto-sell) =====
        if partial_exits:
            for pe in partial_exits:
                details = (
                    f"{pe['outcome']} — {pe['title']}\n"
                    f"Size: {pe['old_size']:.2f} -> {pe['new_size']:.2f} "
                    f"({pe['change_pct']*100:+.1f}%)"
                )
                send_alert("PARTIAL_EXIT", {
                    "source_username": username,
                    "details": details,
                    **pe,
                })

        # ===== PARTIAL ENTRIES (alert only) =====
        if partial_entries:
            for pe in partial_entries:
                details = (
                    f"{pe['outcome']} — {pe['title']}\n"
                    f"Size: {pe['old_size']:.2f} -> {pe['new_size']:.2f} "
                    f"({pe['change_pct']*100:+.1f}%)"
                )
                send_alert("PARTIAL_ENTRY", {
                    "source_username": username,
                    "details": details,
                    **pe,
                })

    # ==================================================================
    # Flag-driven order processing (active poller only)
    # Runs AFTER the per-trader detection loop so we process flags set
    # by the normal poller (or by this cycle's own detection).
    # Runs globally — not limited to the per-trader loop above — so
    # that flags survive even if the trader was pruned from _active_traders.
    # ==================================================================
    if mode == "active":
        print(f"[DIAG] needs_order gate: mode=active live_trading={live_trading} dry_run={dry_run} kill_switch_enabled={is_trading_enabled()}")
    if mode == "active" and live_trading:
        # ----- Process needs_order flags (BUY orders) -----
        all_followed = {t["proxy_wallet"]: t for t in get_followed_traders()}
        pending_wallets = get_all_wallets_needing_orders()
        print(f"[DIAG] Found {len(pending_wallets)} wallets with needs_order flags: {pending_wallets}")
        for pending_wallet in pending_wallets:
            trader_info = all_followed.get(pending_wallet)
            if not trader_info:
                print(f"[DIAG] Skipping {pending_wallet}: not in followed traders")
                continue
            pending_username = trader_info.get("username", pending_wallet[:10])
            pending_positions = get_positions_needing_orders(pending_wallet)
            print(f"[DIAG] Processing needs_order flags for {pending_username} ({pending_wallet}): {len(pending_positions)} positions")

            for fp in pending_positions:
                fp_id = fp["id"]
                token_id = fp.get("asset_id", "")
                title = fp.get("title", "unknown")
                outcome = fp.get("outcome", "?")
                condition_id = fp.get("condition_id", "")
                attempts = int(fp.get("needs_order_attempts", 0) or 0)

                print(f"[DIAG] fp_id={fp_id} title='{title}' outcome={outcome} token_id={token_id[:20] if token_id else 'EMPTY'} attempts={attempts}")

                if not token_id:
                    print(f"[DIAG] Skipping fp_id={fp_id}: no token_id, clearing flag")
                    clear_needs_order(fp_id)
                    continue

                # Give up after MAX_NEEDS_ORDER_ATTEMPTS failures
                if attempts >= MAX_NEEDS_ORDER_ATTEMPTS:
                    print(f"[DIAG] Skipping fp_id={fp_id}: max attempts ({attempts}) exceeded")
                    clear_needs_order(fp_id)
                    print(f"[POLL-ACTIVE] {pending_username}: giving up on '{title}' after {attempts} failed attempts")
                    send_alert("ORDER_FAILED_PERMANENT", {
                        "source_username": pending_username,
                        "title": title,
                        "outcome": outcome,
                        "attempts": attempts,
                    })
                    continue

                # Pre-flight checks
                idem_key = make_idempotency_key(pending_wallet, token_id)
                if has_pending_copy_trade(idem_key):
                    print(f"[DIAG] Skipping fp_id={fp_id}: has PENDING/OPEN copy_trade for idem_key={idem_key}")
                    clear_needs_order(fp_id)
                    print(f"[POLL-ACTIVE] {pending_username}: already have OPEN/PENDING copy_trade for '{title}', clearing flag")
                    continue

                print(f"[DIAG] Checking kill switch: enabled={is_trading_enabled()}")
                if not is_trading_enabled():
                    print(f"[POLL-ACTIVE] Kill switch OFF, skipping needs_order processing")
                    break

                print(f"[DIAG] Checking daily_trades: {daily_trades}/{max_daily}")
                if daily_trades >= max_daily:
                    print(f"[DIAG] Skipping: daily trade limit hit")
                    send_alert("CIRCUIT_BREAKER", {
                        "reason": f"Daily trade limit reached ({daily_trades}/{max_daily})",
                    })
                    break

                print(f"[DIAG] Checking balance: ${account_balance:.2f} vs MIN_NOTIONAL_USD ${MIN_NOTIONAL_USD}")
                if account_balance < MIN_NOTIONAL_USD:
                    print(f"[POLL-ACTIVE] Balance exhausted (${account_balance:.2f}), stopping needs_order processing")
                    break

                # Fetch price and check spread
                spread_ok, spread_pct = check_spread(token_id)
                print(f"[DIAG] Spread check: ok={spread_ok} pct={spread_pct:.4f}")
                if not spread_ok:
                    increment_needs_order_attempts(fp_id)
                    print(f"[POLL-ACTIVE] {pending_username}: spread too wide ({spread_pct:.1%}) on '{title}', will retry (attempt {attempts+1})")
                    continue

                order_price = get_order_price(token_id, side="buy")
                print(f"[DIAG] Order price: {order_price}")
                if order_price <= 0:
                    increment_needs_order_attempts(fp_id)
                    print(f"[POLL-ACTIVE] {pending_username}: no price for '{title}', will retry (attempt {attempts+1})")
                    continue

                size_usd = calculate_trade_size(account_balance, order_price)
                print(f"[DIAG] size_usd={size_usd:.2f}")
                if size_usd <= 0:
                    clear_needs_order(fp_id)
                    print(f"[POLL-ACTIVE] {pending_username}: calculated size $0 for '{title}', clearing flag")
                    continue

                buy_size = round_to_tick(size_usd / order_price, LOT_SIZE)
                mkt_tick, mkt_neg = get_token_market_params(condition_id)

                print(f"[POLL-ACTIVE] {pending_username}: placing BUY for '{title}' — ${size_usd:.2f} ({buy_size:.2f} shares @ {order_price:.2f})")

                # Write PENDING record BEFORE placing order (crash safety)
                pending_id = None
                try:
                    pending_id = log_pending_copy_trade({
                        "source_wallet": pending_wallet,
                        "source_username": pending_username,
                        "condition_id": condition_id,
                        "token_id": token_id,
                        "market_title": title,
                        "market_slug": fp.get("slug", ""),
                        "outcome": outcome,
                        "side": "BUY",
                        "size_usd": size_usd,
                        "idempotency_key": idem_key,
                    })
                except psycopg2.errors.UniqueViolation:
                    clear_needs_order(fp_id)
                    print(f"[POLL-ACTIVE] {pending_username}: DUPLICATE — PENDING/OPEN record already exists for {idem_key}")
                    continue

                if pending_id:
                    print(f"[DIAG] About to call place_copy_order for fp_id={fp_id} token={token_id[:20]} size={buy_size} price={order_price} tick={mkt_tick} neg_risk={mkt_neg}")
                    try:
                        order_result = place_copy_order(
                            token_id=token_id,
                            side="BUY",
                            size=buy_size,
                            price=order_price,
                            tick_size=mkt_tick,
                            neg_risk=mkt_neg,
                        )
                        print(f"[DIAG] place_copy_order returned: {order_result}")
                    except Exception as e:
                        print(f"[DIAG] place_copy_order raised exception: {type(e).__name__}: {e}")
                        import traceback
                        traceback.print_exc()
                        update_pending_to_failed(pending_id)
                        increment_needs_order_attempts(fp_id)
                        continue
                    if is_order_filled(order_result):
                        actual_shares = extract_fill_shares(order_result)
                        actual_price = extract_fill_price(order_result)
                        if actual_shares <= 0:
                            actual_shares = size_usd / order_price if order_price > 0 else 0
                        if actual_price <= 0:
                            actual_price = order_price
                        order_id = str(order_result.get("orderID", "") or order_result.get("orderId", ""))
                        update_pending_to_open(pending_id, order_id, actual_price, actual_shares)
                        daily_trades += 1
                        account_balance -= size_usd
                        clear_needs_order(fp_id)
                        print(f"[POLL-ACTIVE] {pending_username}: EXECUTED BUY on '{title}' — {actual_shares:.2f} shares @ {actual_price:.4f}")
                    else:
                        update_pending_to_failed(pending_id)
                        increment_needs_order_attempts(fp_id)
                        print(f"[POLL-ACTIVE] {pending_username}: BUY not filled on '{title}' (attempt {attempts+1}/{MAX_NEEDS_ORDER_ATTEMPTS})")

        # ----- Process needs_sell flags (SELL orders) -----
        pending_sells = get_trades_needing_sell()
        for trade in pending_sells:
            trade_id = trade["id"]
            token_id = trade.get("token_id", "")
            title = trade.get("market_title", "unknown")
            sell_username = trade.get("source_username", "?")

            if not token_id:
                clear_needs_sell(trade_id)
                continue

            if not is_trading_enabled():
                print(f"[POLL-ACTIVE] Kill switch OFF, skipping needs_sell processing")
                break

            try:
                raw_shares = float(trade.get("shares", 0) or 0)
            except (TypeError, ValueError):
                raw_shares = 0.0

            shares_to_sell = round_to_tick(raw_shares, LOT_SIZE)

            if shares_to_sell <= 0:
                update_copy_trade_exit(trade_id, "CLOSED", None)
                clear_needs_sell(trade_id)
                print(f"[POLL-ACTIVE] {sell_username}: dust position on '{title}', marking CLOSED")
                continue

            sell_price = get_order_price(token_id, side="sell")
            if sell_price <= 0:
                # Don't clear — reconciliation will retry
                print(f"[POLL-ACTIVE] {sell_username}: no sell price for '{title}', will retry")
                continue

            sell_tick, sell_neg = get_token_market_params(trade.get("condition_id", ""))
            sell_result = place_copy_order(
                token_id=token_id,
                side="SELL",
                size=shares_to_sell,
                price=sell_price,
                tick_size=sell_tick,
                neg_risk=sell_neg,
            )

            if is_order_filled(sell_result):
                exit_price = extract_fill_price(sell_result)
                update_copy_trade_exit(trade_id, "CLOSED", exit_price if exit_price > 0 else None)
                clear_needs_sell(trade_id)
                if exit_price > 0:
                    entry_p = float(trade.get("entry_price", 0) or 0)
                    pnl = (exit_price - entry_p) * shares_to_sell if entry_p > 0 else 0
                    print(f"[POLL-ACTIVE] {sell_username}: SOLD {shares_to_sell:.2f} of '{title}' PnL=${pnl:+.2f}")
                else:
                    print(f"[POLL-ACTIVE] {sell_username}: SOLD {shares_to_sell:.2f} of '{title}'")
            else:
                # Don't clear — reconciliation will retry on next cycle
                print(f"[POLL-ACTIVE] {sell_username}: SELL not filled on '{title}', will retry")

    label = "POLL-ACTIVE" if mode == "active" else "POLL-NORMAL"
    print(f"[{label}] Cycle complete at {datetime.now(timezone.utc).isoformat()}")


# ============================================================
# Reconciliation job (C4 fix — catches orphaned positions)
# ============================================================

def run_reconciliation(dry_run: bool = True):
    """
    Cross-reference open copy_trades against followed_positions.
    If the source trader exited but we still hold, place SELL.
    Runs every 5 minutes. Failed sells are retried on the next cycle.
    """
    orphans = get_orphaned_copy_trades()

    if not orphans:
        return

    print(f"[RECONCILE] Found {len(orphans)} orphaned position(s)")

    if dry_run or not is_trading_enabled():
        for orphan in orphans:
            print(f"[RECONCILE] Would sell: {orphan['market_title']} — {orphan['shares']} shares (dry run)")
        return

    for orphan in orphans:
        token_id = orphan["token_id"]
        trade_id = orphan["id"]

        try:
            raw_shares = float(orphan.get("shares", 0) or 0)
        except (TypeError, ValueError):
            raw_shares = 0.0

        shares_to_sell = round_to_tick(raw_shares, LOT_SIZE)

        if shares_to_sell <= 0:
            print(f"[RECONCILE] No shares to sell for trade {trade_id}, marking closed")
            update_copy_trade_exit(trade_id, "CLOSED", None)
            continue

        # Re-check kill switch before each sell
        if not is_trading_enabled():
            print(f"[RECONCILE] Kill switch OFF, stopping reconciliation")
            return

        print(f"[RECONCILE] Selling {shares_to_sell:.2f} shares of '{orphan['market_title']}'")

        sell_price = get_order_price(token_id, side="sell")
        if sell_price <= 0:
            print(f"[RECONCILE] Could not fetch sell price for trade {trade_id} — will retry next cycle")
            continue

        sell_tick, sell_neg = get_token_market_params(orphan.get("condition_id", ""))
        sell_result = place_copy_order(
            token_id=token_id,
            side="SELL",
            size=shares_to_sell,
            price=sell_price,
            tick_size=sell_tick,
            neg_risk=sell_neg,
        )

        if is_order_filled(sell_result):
            exit_price = extract_fill_price(sell_result)
            update_copy_trade_exit(trade_id, "CLOSED", exit_price if exit_price > 0 else None)
            if exit_price > 0:
                entry_p = float(orphan.get("entry_price", 0) or 0)
                pnl = (exit_price - entry_p) * shares_to_sell if entry_p > 0 else 0
                print(f"[RECONCILE] Sold — exit price: {exit_price:.4f}, PnL: ${pnl:+.2f}")
            else:
                print(f"[RECONCILE] Sold successfully (exit price unknown)")
        else:
            # Leave OPEN — next reconciliation cycle retries
            print(f"[RECONCILE] Sell failed for trade {trade_id} — will retry next cycle")

    print(f"[RECONCILE] Cycle complete at {datetime.now(timezone.utc).isoformat()}")


# ============================================================
# CLI entry point
# ============================================================

if __name__ == "__main__":
    import sys
    from db import init_db

    print("[COPY TRADER] Initializing...")
    init_db()

    dry_run = "--live" not in sys.argv

    if dry_run:
        print("[COPY TRADER] Running in DRY RUN mode (use --live for real trades)")
    else:
        print("[COPY TRADER] LIVE MODE - real orders will be placed!")
        confirm = input("Type 'CONFIRM' to proceed: ")
        if confirm != "CONFIRM":
            print("Aborted.")
            sys.exit(0)

    if "--reconcile" in sys.argv:
        run_reconciliation(dry_run=dry_run)
    else:
        poll_followed_traders(dry_run=dry_run, mode="normal")
