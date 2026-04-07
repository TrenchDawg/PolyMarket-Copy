"""
Polymarket Copy Trader — Database helpers
"""
import json
import psycopg2
import psycopg2.extras
from datetime import datetime, timezone
from config import DATABASE_URL


def get_conn():
    """Get a PostgreSQL connection."""
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Run schema.sql to initialize tables."""
    import os
    schema_path = os.path.join(os.path.dirname(__file__), "schema.sql")
    with open(schema_path, "r") as f:
        sql = f.read()
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.commit()
        print("[DB] Schema initialized successfully")
    finally:
        conn.close()


# ============================================================
# Trader operations
# ============================================================

def upsert_trader(trader: dict):
    """Insert or update a trader record."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO traders (
                    proxy_wallet, username, profile_image, x_username,
                    verified_badge, total_pnl, total_volume,
                    total_positions, active_positions,
                    roi_pct, win_rate, consistency_score,
                    recency_score, diversification, composite_score,
                    last_scored_at, updated_at
                ) VALUES (
                    %(proxy_wallet)s, %(username)s, %(profile_image)s, %(x_username)s,
                    %(verified_badge)s, %(total_pnl)s, %(total_volume)s,
                    %(total_positions)s, %(active_positions)s,
                    %(roi_pct)s, %(win_rate)s, %(consistency_score)s,
                    %(recency_score)s, %(diversification)s, %(composite_score)s,
                    %(last_scored_at)s, NOW()
                )
                ON CONFLICT (proxy_wallet) DO UPDATE SET
                    username = EXCLUDED.username,
                    profile_image = EXCLUDED.profile_image,
                    x_username = EXCLUDED.x_username,
                    verified_badge = EXCLUDED.verified_badge,
                    total_pnl = EXCLUDED.total_pnl,
                    total_volume = EXCLUDED.total_volume,
                    total_positions = EXCLUDED.total_positions,
                    active_positions = EXCLUDED.active_positions,
                    roi_pct = EXCLUDED.roi_pct,
                    win_rate = EXCLUDED.win_rate,
                    consistency_score = EXCLUDED.consistency_score,
                    recency_score = EXCLUDED.recency_score,
                    diversification = EXCLUDED.diversification,
                    composite_score = EXCLUDED.composite_score,
                    last_scored_at = EXCLUDED.last_scored_at,
                    updated_at = NOW()
            """, trader)
        conn.commit()
    finally:
        conn.close()


def get_followed_traders() -> list:
    """Get all traders we're actively following."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM traders
                WHERE is_followed = TRUE
                ORDER BY composite_score DESC
            """)
            return cur.fetchall()
    finally:
        conn.close()


def get_top_traders(limit: int = 10) -> list:
    """Get top-scored traders."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM traders
                WHERE composite_score IS NOT NULL
                ORDER BY composite_score DESC
                LIMIT %s
            """, (limit,))
            return cur.fetchall()
    finally:
        conn.close()


def update_follow_list(top_n: int):
    """Set is_followed=TRUE for top N traders, FALSE for rest."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Unfollow all
            cur.execute("UPDATE traders SET is_followed = FALSE")
            # Follow top N
            cur.execute("""
                UPDATE traders SET is_followed = TRUE
                WHERE proxy_wallet IN (
                    SELECT proxy_wallet FROM traders
                    WHERE composite_score IS NOT NULL
                    ORDER BY composite_score DESC
                    LIMIT %s
                )
            """, (top_n,))
        conn.commit()
    finally:
        conn.close()


# ============================================================
# Position snapshot operations
# ============================================================

def save_position_snapshot(wallet: str, positions: list):
    """Save a batch of positions for a trader."""
    conn = get_conn()
    now = datetime.now(timezone.utc)
    try:
        with conn.cursor() as cur:
            for pos in positions:
                cur.execute("""
                    INSERT INTO trader_positions (
                        proxy_wallet, condition_id, asset_id, title,
                        outcome, size, avg_price, current_price,
                        market_slug, snapshot_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (
                    wallet,
                    pos.get("conditionId", ""),
                    pos.get("asset", ""),
                    pos.get("title", ""),
                    pos.get("outcome", ""),
                    pos.get("size", 0),
                    pos.get("avgPrice", 0),
                    pos.get("curPrice", 0),
                    pos.get("slug", ""),
                    now
                ))
        conn.commit()
    finally:
        conn.close()


def get_latest_positions(wallet: str) -> list:
    """Get the most recent position snapshot for a trader."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get the latest snapshot timestamp
            cur.execute("""
                SELECT DISTINCT snapshot_at FROM trader_positions
                WHERE proxy_wallet = %s
                ORDER BY snapshot_at DESC
                LIMIT 1
            """, (wallet,))
            row = cur.fetchone()
            if not row:
                return []

            cur.execute("""
                SELECT * FROM trader_positions
                WHERE proxy_wallet = %s AND snapshot_at = %s
            """, (wallet, row["snapshot_at"]))
            return cur.fetchall()
    finally:
        conn.close()


# ============================================================
# Copy trade operations
# ============================================================

def log_copy_trade(trade: dict):
    """Log a copy trade we executed."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO copy_trades (
                    source_wallet, source_username, condition_id,
                    token_id, market_title, market_slug, outcome,
                    side, entry_price, size_usd, shares, order_id
                ) VALUES (
                    %(source_wallet)s, %(source_username)s, %(condition_id)s,
                    %(token_id)s, %(market_title)s, %(market_slug)s, %(outcome)s,
                    %(side)s, %(entry_price)s, %(size_usd)s, %(shares)s, %(order_id)s
                )
            """, trade)
        conn.commit()
    finally:
        conn.close()


# ============================================================
# System config operations
# ============================================================

def get_config(key: str) -> str:
    """Get a system config value."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM system_config WHERE key = %s", (key,))
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


def set_config(key: str, value: str):
    """Set a system config value."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO system_config (key, value, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
            """, (key, value))
        conn.commit()
    finally:
        conn.close()


def is_kill_switch_on() -> bool:
    """Check if the kill switch is engaged (trading disabled)."""
    val = get_config("kill_switch")
    return val != "ON"  # OFF or missing = kill switch engaged = no trading


# ============================================================
# Followed positions operations
# ============================================================

def get_followed_open_positions(wallet: str) -> list:
    """Get all OPEN positions we're tracking for a trader."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM followed_positions
                WHERE proxy_wallet = %s AND status = 'OPEN'
            """, (wallet,))
            return cur.fetchall()
    finally:
        conn.close()


def upsert_followed_position(position: dict):
    """Insert or update a followed position."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO followed_positions (
                    proxy_wallet, asset_id, condition_id, title, slug,
                    outcome, outcome_index, size, avg_price, entry_price,
                    current_price, current_value, pre_existing, status, last_updated
                ) VALUES (
                    %(proxy_wallet)s, %(asset_id)s, %(condition_id)s, %(title)s, %(slug)s,
                    %(outcome)s, %(outcome_index)s, %(size)s, %(avg_price)s, %(entry_price)s,
                    %(current_price)s, %(current_value)s, %(pre_existing)s, 'OPEN', NOW()
                )
                ON CONFLICT (proxy_wallet, asset_id) DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    current_value = EXCLUDED.current_value,
                    size = EXCLUDED.size,
                    last_updated = NOW()
            """, position)
        conn.commit()
    finally:
        conn.close()


def mark_position_closed(wallet: str, asset_id: str):
    """Mark a position as closed (trader exited or market resolved)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE followed_positions
                SET status = 'CLOSED', closed_at = NOW()
                WHERE proxy_wallet = %s AND asset_id = %s AND status = 'OPEN'
            """, (wallet, asset_id))
        conn.commit()
    finally:
        conn.close()


def get_closed_followed_position(wallet: str, asset_id: str) -> dict:
    """Get a specific position (for exit alert details)."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM followed_positions
                WHERE proxy_wallet = %s AND asset_id = %s
            """, (wallet, asset_id))
            return cur.fetchone()
    finally:
        conn.close()


def has_baseline_snapshot(wallet: str) -> bool:
    """Check if we've ever recorded positions for this trader."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM followed_positions WHERE proxy_wallet = %s
                )
            """, (wallet,))
            return cur.fetchone()[0]
    finally:
        conn.close()


def get_trader_allocation(wallet: str) -> float:
    """Get a trader's portfolio allocation percentage (0.0 to 1.0)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT allocation_pct FROM traders WHERE proxy_wallet = %s
            """, (wallet,))
            row = cur.fetchone()
            return float(row[0]) if row and row[0] else 0.0
    finally:
        conn.close()


def log_alert(alert_type: str, payload: dict):
    """Log an alert that was sent."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO alerts_log (alert_type, payload)
                VALUES (%s, %s)
            """, (alert_type, json.dumps(payload)))
        conn.commit()
    finally:
        conn.close()


def should_send_alert(wallet: str, cooldown_minutes: int) -> bool:
    """Return True if enough time has passed since the last alert for this trader."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_alert_at FROM traders WHERE proxy_wallet = %s",
                (wallet,)
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or not row[0]:
        return True

    elapsed = datetime.now(timezone.utc) - row[0]
    return elapsed.total_seconds() >= (cooldown_minutes * 60)


def update_last_alert(wallet: str):
    """Record that we just sent an alert for this trader."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE traders SET last_alert_at = NOW() WHERE proxy_wallet = %s",
                (wallet,)
            )
        conn.commit()
    finally:
        conn.close()


def seconds_until_alert(wallet: str, cooldown_minutes: int) -> int:
    """Return seconds remaining in the cooldown window (0 if no cooldown active)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT last_alert_at FROM traders WHERE proxy_wallet = %s",
                (wallet,)
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row or not row[0]:
        return 0

    elapsed = (datetime.now(timezone.utc) - row[0]).total_seconds()
    remaining = (cooldown_minutes * 60) - elapsed
    return max(0, int(remaining))
