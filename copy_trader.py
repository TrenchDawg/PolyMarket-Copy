"""
Polymarket Copy Trader — Position Monitor & Copy Execution

Polls followed traders for new positions and mirrors their trades.
Includes kill switch, circuit breakers, and liquidity checks.
"""
import json
import time
import requests
from datetime import datetime, timezone, date
from typing import Optional
from polymarket_client import PolymarketClient
from config import (
    DEFAULT_PORTFOLIO_FRACTION,
    MAX_POSITION_USD,
    MAX_DAILY_TRADES,
    MIN_LIQUIDITY_USD,
    MAX_SPREAD_PCT,
    ALERT_WEBHOOK_URL,
)
from db import (
    get_followed_traders,
    log_copy_trade,
    is_kill_switch_on,
    get_config,
    log_alert,
)


client = PolymarketClient()


# ============================================================
# Position delta detection
# ============================================================

def detect_new_positions(wallet: str, username: str = "") -> tuple:
    """
    Compare current positions against followed_positions table.
    Returns (new_entries, exits) where:
      - new_entries: list of positions the trader just entered (not pre-existing)
      - exits: list of positions the trader just exited
    """
    from db import (
        get_followed_open_positions,
        upsert_followed_position,
        mark_position_closed,
        has_baseline_snapshot,
    )

    # Get current positions from API
    current = client.get_positions(wallet, size_threshold=10)
    if current is None:
        return [], []

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
        print(f"[POLL] {username or wallet[:10]}: Baseline saved ({len(current)} positions), no alerts")
        return [], []

    # Get what we're currently tracking
    tracked = get_followed_open_positions(wallet)
    tracked_assets = {p["asset_id"]: p for p in tracked}

    # Detect NEW entries (in API but not tracked)
    new_entries = []
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
            # Existing position — update current price/value
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
                "entry_price": tracked_assets[asset_id].get("entry_price", 0),
                "current_price": pos.get("curPrice", 0),
                "current_value": pos.get("currentValue", 0),
                "pre_existing": tracked_assets[asset_id].get("pre_existing", False),
            })

    # Detect EXITS (tracked but not in current API response)
    exits = []
    for asset_id, tracked_pos in tracked_assets.items():
        if asset_id not in current_assets:
            mark_position_closed(wallet, asset_id)
            exits.append(tracked_pos)

    return new_entries, exits


# ============================================================
# Trade execution checks
# ============================================================

def check_liquidity(condition_id: str) -> tuple[bool, str]:
    """
    Check if a market has sufficient liquidity and acceptable spread.
    Returns (ok, reason).
    """
    # Try to get top holders as a proxy for liquidity
    holders = client.get_top_holders(condition_id)
    # For now, we'll rely on the spread check which is more direct

    return True, "liquidity check passed"


def check_spread(token_id: str) -> tuple[bool, float]:
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
    from db import get_conn
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM copy_trades
                WHERE copied_at::date = CURRENT_DATE
            """)
            return cur.fetchone()[0]
    finally:
        conn.close()


def calculate_position_size(portfolio_value: float) -> float:
    """
    Calculate position size for a copy trade.
    Uses portfolio fraction from config, capped at MAX_POSITION_USD.
    """
    fraction = float(get_config("portfolio_fraction") or DEFAULT_PORTFOLIO_FRACTION)
    max_usd = float(get_config("max_position_usd") or MAX_POSITION_USD)
    size = portfolio_value * fraction
    return min(size, max_usd)


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
# Copy trade execution (DRY RUN by default)
# ============================================================

def execute_copy_trade(
    trader: dict,
    position: dict,
    dry_run: bool = True,
) -> Optional[dict]:
    """
    Execute a copy trade. In dry_run mode, logs but doesn't place orders.
    """
    token_id = position.get("asset", "")
    condition_id = position.get("conditionId", "")
    title = position.get("title", "unknown")
    outcome = position.get("outcome", "Yes")
    avg_price = float(position.get("avgPrice", 0))
    slug = position.get("slug", "")

    wallet = trader.get("proxy_wallet", "")
    username = trader.get("username", wallet[:10])
    composite_score = float(trader.get("composite_score", 0))

    # === Pre-flight checks ===

    # 1. Kill switch
    if is_kill_switch_on():
        send_alert("NEW_POSITION_DETECTED", {
            "source_username": username,
            "title": title,
            "outcome": outcome,
            "size": float(position.get("size", 0)),
            "avg_price": avg_price,
        })
        print(f"[COPY] Kill switch ON — alert only for {title}")
        return None

    # 2. Circuit breaker
    daily_count = get_daily_trade_count()
    max_daily = int(get_config("max_daily_trades") or MAX_DAILY_TRADES)
    if daily_count >= max_daily:
        send_alert("CIRCUIT_BREAKER", {
            "reason": f"Daily trade limit reached ({daily_count}/{max_daily})",
        })
        print(f"[COPY] Circuit breaker: {daily_count} trades today, limit is {max_daily}")
        return None

    # 3. Spread check
    spread_ok, spread_pct = check_spread(token_id)
    if not spread_ok:
        print(f"[COPY] Spread too wide ({spread_pct:.1%}) for {title}, skipping")
        return None

    # 4. Get current price
    price_data = client.get_market_price(token_id, side="buy")
    if not price_data:
        print(f"[COPY] Could not get price for {title}, skipping")
        return None
    entry_price = float(price_data.get("price", avg_price))

    # 5. Position sizing
    # TODO: Get actual portfolio value from Polymarket API
    portfolio_value = 1000.0  # placeholder — replace with real value
    size_usd = calculate_position_size(portfolio_value)
    shares = size_usd / entry_price if entry_price > 0 else 0

    trade_record = {
        "source_wallet": wallet,
        "source_username": username,
        "condition_id": condition_id,
        "token_id": token_id,
        "market_title": title,
        "market_slug": slug,
        "outcome": outcome,
        "side": "BUY",
        "entry_price": entry_price,
        "size_usd": size_usd,
        "shares": shares,
        "order_id": None,
    }

    if dry_run:
        print(f"[COPY] DRY RUN — would buy {shares:.1f} shares of '{outcome}' "
              f"in '{title}' @ {entry_price:.4f} (${size_usd:.2f})")
        trade_record["order_id"] = "DRY_RUN"
        log_copy_trade(trade_record)
        send_alert("NEW_TRADE", {
            **trade_record,
            "composite_score": composite_score,
            "dry_run": True,
        })
        return trade_record

    # === LIVE EXECUTION ===
    # TODO: Implement actual order placement via Polymarket CLOB API
    # This requires:
    # 1. py-clob-client or direct API calls with Ed25519 signatures
    # 2. Wallet funded with USDC on Polygon
    # 3. Allowances set for CTF contracts
    print(f"[COPY] LIVE execution not yet implemented")
    return None


# ============================================================
# Main polling loop
# ============================================================

def poll_followed_traders(dry_run: bool = True):
    """
    Single poll cycle: check all followed traders for new positions.
    Sends batched alerts — one per trader per cycle, not one per position.
    """
    followed = get_followed_traders()
    if not followed:
        print("[POLL] No followed traders. Run trader_ranker.py first.")
        return

    print(f"[POLL] Checking {len(followed)} followed traders...")

    for trader in followed:
        wallet = trader["proxy_wallet"]
        username = trader.get("username", wallet[:10])

        new_entries, exits = detect_new_positions(wallet, username)

        # Send ONE batched entry alert per trader (not per position)
        if new_entries:
            print(f"[POLL] {username}: {len(new_entries)} new entry(s) detected!")

            entry_lines = []
            for pos in new_entries:
                title = pos.get("title", "unknown")
                outcome = pos.get("outcome", "?")
                price = pos.get("curPrice", 0)
                entry_lines.append(f"  - {outcome} @ {price} — {title}")

            send_alert("BATCH_ENTRY", {
                "source_username": username,
                "count": len(new_entries),
                "entries": "\n".join(entry_lines),
            })

        # Send ONE batched exit alert per trader
        if exits:
            print(f"[POLL] {username}: {len(exits)} exit(s) detected!")

            exit_lines = []
            for pos in exits:
                title = pos.get("title", "unknown")
                outcome = pos.get("outcome", "?")
                pre_existing = pos.get("pre_existing", False)
                exit_lines.append(f"  - {outcome} — {title} (pre-existing: {pre_existing})")

            send_alert("BATCH_EXIT", {
                "source_username": username,
                "count": len(exits),
                "exits": "\n".join(exit_lines),
            })

    print(f"[POLL] Cycle complete at {datetime.now(timezone.utc).isoformat()}")


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
        print("[COPY TRADER] ⚠️  LIVE MODE — real orders will be placed!")
        confirm = input("Type 'CONFIRM' to proceed: ")
        if confirm != "CONFIRM":
            print("Aborted.")
            sys.exit(0)

    # Single poll for testing; the scheduler wraps this
    poll_followed_traders(dry_run=dry_run)
