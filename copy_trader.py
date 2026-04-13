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
# py_clob_client imports are lazy-loaded inside get_clob_client() and place_copy_order()
# to prevent startup crashes if the package has import issues.
from config import (
    MAX_DAILY_TRADES,
    MAX_SPREAD_PCT,
    MAX_TRADE_SIZE_USD,
    ALERT_WEBHOOK_URL,
    PARTIAL_SIZE_CHANGE_THRESHOLD,
    TRADE_SIZE_FLOOR_PCT,
    TRADE_SIZE_CEILING_PCT,
    TRADE_SIZE_MIDPOINT,
    TRADE_SIZE_MAX_DISTANCE,
    TICK_SIZE,
    LOT_SIZE,
    MIN_ORDER_SIZE,
    POLYMARKET_CLOB_HOST,
    POLYMARKET_CHAIN_ID,
    POLYMARKET_SIGNATURE_TYPE,
    POLY_API_KEY,
    POLY_API_SECRET,
    POLY_API_PASSPHRASE,
    POLY_PRIVATE_KEY,
    POLY_WALLET_ADDRESS,
    REALTIME_ALERT_MAX_POSITIONS,
    ACTIVE_TRADER_WINDOW_MINUTES,
)
from db import (
    get_followed_traders,
    log_copy_trade,
    is_trading_enabled,
    get_config,
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
    get_conn,
    release_conn,
)


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

    if size < MIN_ORDER_SIZE:
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
    Determine whether a FOK market-order response actually represents a fill.

    Defaults to False. A response is only treated as filled when we can see
    an explicit positive fill amount. The presence of an orderID is NOT
    sufficient — py_clob_client returns an orderID on any accepted request,
    including FOK orders that matched zero shares.

    Unknown response shapes are logged once per call so the field list can
    be extended after the first live run reveals the real schema.
    """
    if not order_result:
        return False
    if not isinstance(order_result, dict):
        print(f"[ORDER] is_order_filled: unexpected response type {type(order_result).__name__}, treating as unfilled")
        return False

    status = str(order_result.get("status", "")).upper()
    if status in {"KILLED", "CANCELLED", "CANCELED", "REJECTED", "FAILED", "UNMATCHED", "EXPIRED"}:
        return False
    if order_result.get("success") is False:
        return False
    if order_result.get("errorMsg"):
        return False

    def _as_float(val) -> float:
        try:
            return float(val) if val is not None else 0.0
        except (TypeError, ValueError):
            return 0.0

    # Positive fill indicator required. Add new field names here as we see
    # them in real responses during the shadow-trading period.
    filled_candidates = [
        order_result.get("filledSize"),
        order_result.get("filled_size"),
        order_result.get("matchedSize"),
        order_result.get("matched_size"),
        order_result.get("sizeMatched"),
    ]
    for candidate in filled_candidates:
        if _as_float(candidate) > 0:
            return True

    # Known "matched" statuses with no explicit size field but a clean shape
    if status in {"MATCHED", "FILLED"}:
        return True

    print(f"[ORDER] is_order_filled: no positive fill indicator in response, treating as unfilled: {order_result}")
    return False


def extract_fill_shares(order_result: dict) -> float:
    """
    Extract actual filled share quantity from CLOB order response.

    UPDATE THESE FIELD NAMES after running test_live_order() with a real
    $1 order and inspecting the response.
    """
    if not order_result or not isinstance(order_result, dict):
        return 0.0

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

    UPDATE THESE FIELD NAMES after running test_live_order().
    """
    if not order_result or not isinstance(order_result, dict):
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
# CLOB client (order execution)
# ============================================================

_clob_client = None


def get_clob_client():
    """Get an authenticated CLOB client. Initializes on first call."""
    from py_clob_client.client import ClobClient
    from py_clob_client.clob_types import ApiCreds
    global _clob_client
    if _clob_client is not None:
        return _clob_client

    if not POLY_PRIVATE_KEY or not POLY_WALLET_ADDRESS:
        print("[CLOB] No private key or wallet address configured. Cannot place orders.")
        return None

    if not POLY_API_KEY or not POLY_API_SECRET or not POLY_API_PASSPHRASE:
        print("[CLOB] No API credentials configured. Cannot place orders.")
        return None

    try:
        creds = ApiCreds(
            api_key=POLY_API_KEY,
            api_secret=POLY_API_SECRET,
            api_passphrase=POLY_API_PASSPHRASE,
        )
        clob = ClobClient(
            host=POLYMARKET_CLOB_HOST,
            key=POLY_PRIVATE_KEY,
            chain_id=POLYMARKET_CHAIN_ID,
            signature_type=POLYMARKET_SIGNATURE_TYPE,
            funder=POLY_WALLET_ADDRESS,
            creds=creds,
        )
        print("[CLOB] Authenticated successfully")
        _clob_client = clob
        return clob
    except Exception as e:
        print(f"[CLOB] Authentication failed: {type(e).__name__}")
        return None


def place_copy_order(
    token_id: str,
    side: str,
    amount: float,
    title: str = "",
) -> Optional[dict]:
    """
    Place a FOK market order on Polymarket.

    For BUY orders, `amount` is USDC to spend.
    For SELL orders, `amount` is shares (contracts) to sell.
    Returns the raw order response dict (success OR failure). Callers must
    validate the response via `is_order_filled()` before treating it as a
    real execution.

    The amount is always rounded DOWN to LOT_SIZE before being sent so the
    CLOB API doesn't reject us for off-tick size.
    """
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY, SELL
    clob = get_clob_client()
    if clob is None:
        print(f"[ORDER] Cannot place order — CLOB client not initialized")
        return None

    rounded_amount = round_to_tick(amount, LOT_SIZE)
    unit = "USDC" if side == "BUY" else "shares"

    if rounded_amount <= 0:
        print(f"[ORDER] Rounded amount is {rounded_amount} {unit}, skipping '{title}'")
        return None

    try:
        order_side = BUY if side == "BUY" else SELL
        mo = MarketOrderArgs(
            token_id=token_id,
            amount=rounded_amount,
            side=order_side,
        )
        signed_order = clob.create_market_order(mo)
        resp = clob.post_order(signed_order, OrderType.FOK)
        print(f"[ORDER] {side} {rounded_amount:.4f} {unit} on '{title}' — Response: {resp}")
        return resp
    except Exception as e:
        print(f"[ORDER] Failed to place {side} {rounded_amount:.4f} {unit} on '{title}': {type(e).__name__}")
        return None


def place_exit_order(token_id: str, shares: float, title: str = "") -> Optional[dict]:
    """Sell/exit a position by selling all shares."""
    return place_copy_order(token_id=token_id, side="SELL", amount=shares, title=title)


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
                    elif account_balance < MIN_ORDER_SIZE:
                        print(f"[POLL-ACTIVE] Balance exhausted (${account_balance:.2f}), stopping orders")
                        status = "NO BALANCE"
                    else:
                        spread_ok, spread_pct = check_spread(token_id)
                        if not spread_ok:
                            print(f"[POLL-ACTIVE] {username}: spread too wide ({spread_pct:.1%}) on '{title}', skipping")
                            status = f"SKIP (spread {spread_pct:.1%})"
                        else:
                            print(f"[POLL-ACTIVE] U-size: ${size_usd:.2f} (balance=${account_balance:.0f}, price={entry_price:.2f})")
                            order_result = place_copy_order(
                                token_id=token_id,
                                side="BUY",
                                amount=size_usd,
                                title=title,
                            )
                            if is_order_filled(order_result):
                                # Extract real fill data (C1 fix)
                                actual_shares = extract_fill_shares(order_result)
                                actual_price = extract_fill_price(order_result)
                                if actual_shares <= 0:
                                    print(f"[ORDER] WARNING: Using estimated shares (extraction failed)")
                                    actual_shares = size_usd / entry_price if entry_price > 0 else 0
                                if actual_price <= 0:
                                    actual_price = entry_price

                                try:
                                    log_copy_trade({
                                        "source_wallet": wallet,
                                        "source_username": username,
                                        "condition_id": pos.get("conditionId", ""),
                                        "token_id": token_id,
                                        "market_title": title,
                                        "market_slug": pos.get("slug", ""),
                                        "outcome": outcome,
                                        "side": "BUY",
                                        "entry_price": actual_price,
                                        "size_usd": size_usd,
                                        "shares": actual_shares,
                                        "order_id": str(order_result.get("orderID", "") or order_result.get("orderId", "")),
                                        "idempotency_key": idem_key,
                                    })
                                    daily_trades += 1
                                    account_balance -= size_usd  # Running balance (H2 fix)
                                    status = "EXECUTED"
                                except psycopg2.errors.UniqueViolation:
                                    print(
                                        f"[POLL-ACTIVE] {username}: DUPLICATE ORDER WARNING - "
                                        f"UniqueViolation on idem_key {idem_key}. "
                                        f"Order placed on-chain but another poll beat us. "
                                        f"Manual reconciliation required for '{title}'."
                                    )
                                    send_alert("DUPLICATE_ORDER", {
                                        "source_username": username,
                                        "title": title,
                                        "idempotency_key": idem_key,
                                        "order_id": str(order_result.get("orderID", "") or order_result.get("orderId", "")),
                                    })
                                    status = "DUPLICATE"
                            else:
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
                            sell_result = place_exit_order(
                                token_id=token_id,
                                shares=shares_to_sell,
                                title=title,
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

        sell_result = place_copy_order(
            token_id=token_id,
            side="SELL",
            amount=shares_to_sell,
            title=orphan.get("market_title", ""),
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
# Test order function (H4 — discover real CLOB response schema)
# ============================================================

def test_live_order():
    """
    Place a $1 test order and print the FULL response.
    Run ONCE manually to discover field names before going live.

    Usage:
      python -c "from copy_trader import test_live_order; test_live_order()"
    """
    from py_clob_client.clob_types import MarketOrderArgs, OrderType
    from py_clob_client.order_builder.constants import BUY

    clob = get_clob_client()
    if not clob:
        print("ERROR: Could not initialize CLOB client")
        return None

    # PASTE A REAL TOKEN ID HERE before running.
    # Find one at: https://gamma-api.polymarket.com/markets?closed=false&limit=5
    # Use a clobTokenIds value from a liquid market.
    TEST_TOKEN_ID = "PASTE_A_REAL_TOKEN_ID_HERE"

    if "PASTE" in TEST_TOKEN_ID:
        print("ERROR: Replace TEST_TOKEN_ID with a real token ID first!")
        print("  Find one at: https://gamma-api.polymarket.com/markets?closed=false&limit=5")
        return None

    print("=" * 60)
    print("TEST: Placing $1 FOK BUY order")
    print(f"Token: {TEST_TOKEN_ID[:40]}...")
    print("=" * 60)

    try:
        mo = MarketOrderArgs(
            token_id=TEST_TOKEN_ID,
            amount=1.0,
            side=BUY,
        )
        signed_order = clob.create_market_order(mo)
        result = clob.post_order(signed_order, OrderType.FOK)

        print(f"\nResponse type: {type(result)}")
        print(f"Raw response: {result}")

        if isinstance(result, dict):
            print("\nField breakdown:")
            for key, value in result.items():
                print(f"  {key}: {value} (type: {type(value).__name__})")

        print("\n" + "=" * 60)
        print("UPDATE THESE FUNCTIONS with the real field names above:")
        print("  - extract_fill_shares()")
        print("  - extract_fill_price()")
        print("  - is_order_filled()")
        print("=" * 60)

        return result
    except Exception as e:
        print(f"\nOrder FAILED: {e}")
        import traceback
        traceback.print_exc()
        return None


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

    if "--test-order" in sys.argv:
        test_live_order()
    elif "--reconcile" in sys.argv:
        run_reconciliation(dry_run=dry_run)
    else:
        poll_followed_traders(dry_run=dry_run, mode="normal")
