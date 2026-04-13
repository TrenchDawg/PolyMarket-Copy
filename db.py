"""
Polymarket Copy Trader — Database helpers
"""
import json
import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool
from datetime import datetime, timezone
from typing import Optional
from config import DATABASE_URL


# ============================================================
# Connection pool
# ============================================================

_pool: Optional[ThreadedConnectionPool] = None


def _get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None or _pool.closed:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=10,
            dsn=DATABASE_URL,
        )
    return _pool


def get_conn():
    """Borrow a PostgreSQL connection from the pool."""
    return _get_pool().getconn()


def release_conn(conn):
    """
    Return a connection to the pool, but NEVER return a dirty or dead one.

    Scenarios handled:
      1. conn already closed (server dropped, code bug) -> discard via putconn(close=True)
      2. conn in aborted transaction state (after IntegrityError etc.) -> rollback first
      3. conn has an uncommitted healthy transaction -> rollback to avoid leaking state
      4. pool itself is closed -> best-effort hard-close the connection

    Without this, a single psycopg2 error poisons the pool: the next borrower
    gets a connection in "current transaction is aborted, commands ignored"
    state and every subsequent query fails until restart.
    """
    if conn is None:
        return

    pool = None
    try:
        pool = _get_pool()
    except Exception:
        pool = None

    # If the socket is already closed, ask the pool to throw it away entirely.
    if getattr(conn, "closed", 0):
        if pool is not None:
            try:
                pool.putconn(conn, close=True)
            except Exception:
                pass
        return

    # Roll back any pending/aborted transaction before handing the conn back.
    try:
        conn.rollback()
    except Exception:
        # Connection is unusable — force-close it through the pool.
        if pool is not None:
            try:
                pool.putconn(conn, close=True)
                return
            except Exception:
                pass
        try:
            conn.close()
        except Exception:
            pass
        return

    if pool is None:
        try:
            conn.close()
        except Exception:
            pass
        return

    try:
        pool.putconn(conn)
    except Exception:
        try:
            conn.close()
        except Exception:
            pass


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
        release_conn(conn)


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
        release_conn(conn)


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
        release_conn(conn)


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
        release_conn(conn)


def update_follow_list(scored_wallets: list):
    """
    Follow every trader that passed all scoring filters in the current run.
    No top-N cutoff: if a trader survived every filter, they're worth following.

    CRITICAL: Traders with open copy_trades are NEVER unfollowed, even if they
    drop off the scored list. Otherwise the poll loop would stop monitoring them
    and we'd never detect the exit signal for positions we already hold.

    CRITICAL: If scored_wallets is empty (scoring API outage, total failure),
    we refuse to touch the follow list at all. Otherwise a single bad scoring
    run would unfollow every trader with no open positions and leave the bot
    silent until the next successful run.
    """
    if not scored_wallets:
        print("[SCORER] update_follow_list called with empty scored_wallets - refusing to unfollow everyone. Check scoring pipeline for failures.")
        return

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            # Log any trader we're retaining purely because of an open position.
            # Using = ANY(%s) is empty-list-safe unlike NOT IN ().
            cur.execute("""
                SELECT t.username, t.proxy_wallet, COUNT(ct.id) AS open_trades
                FROM traders t
                JOIN copy_trades ct
                     ON t.proxy_wallet = ct.source_wallet AND ct.status = 'OPEN'
                WHERE t.is_followed = TRUE
                  AND NOT (t.proxy_wallet = ANY(%s))
                GROUP BY t.username, t.proxy_wallet
            """, (scored_wallets,))
            retained = cur.fetchall()
            for username, wallet, count in retained:
                label = username or wallet[:10]
                print(f"[SCORER] Retaining {label} - {count} open copy trade(s) still held")

            # Unfollow everyone EXCEPT traders with open copy_trades. Using
            # NOT EXISTS is NULL-safe (unlike NOT IN which returns NULL if any
            # subquery row is NULL) and usually optimizes identically.
            cur.execute("""
                UPDATE traders t
                SET is_followed = FALSE
                WHERE t.is_followed = TRUE
                  AND NOT EXISTS (
                      SELECT 1 FROM copy_trades ct
                      WHERE ct.source_wallet = t.proxy_wallet
                        AND ct.status = 'OPEN'
                  )
            """)

            # Follow every wallet from the current scoring run
            cur.execute("""
                UPDATE traders SET is_followed = TRUE
                WHERE proxy_wallet = ANY(%s)
                  AND composite_score IS NOT NULL
            """, (scored_wallets,))
        conn.commit()
    finally:
        release_conn(conn)


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
        release_conn(conn)


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
        release_conn(conn)


# ============================================================
# Copy trade operations
# ============================================================

def log_copy_trade(trade: dict):
    """
    Log a copy trade we executed.

    Expects `trade` to include an `idempotency_key` (may be None for legacy
    writes). The UNIQUE constraint on copy_trades.idempotency_key means a
    second INSERT with the same key will raise psycopg2.IntegrityError — the
    caller should already have checked `has_pending_copy_trade` upstream, but
    this is the belt-and-suspenders layer.
    """
    trade = dict(trade)  # shallow copy so we don't mutate the caller's dict
    trade.setdefault("idempotency_key", None)
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO copy_trades (
                    source_wallet, source_username, condition_id,
                    token_id, market_title, market_slug, outcome,
                    side, entry_price, size_usd, shares, order_id,
                    idempotency_key
                ) VALUES (
                    %(source_wallet)s, %(source_username)s, %(condition_id)s,
                    %(token_id)s, %(market_title)s, %(market_slug)s, %(outcome)s,
                    %(side)s, %(entry_price)s, %(size_usd)s, %(shares)s, %(order_id)s,
                    %(idempotency_key)s
                )
            """, trade)
        conn.commit()
    finally:
        release_conn(conn)


def has_pending_copy_trade(idempotency_key: str) -> bool:
    """
    Return True if we already have a copy_trade with this idempotency key
    and the trade is still OPEN. Used to short-circuit duplicate order
    placement across overlapping polls or post-crash recovery.
    """
    if not idempotency_key:
        return False
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT EXISTS(
                    SELECT 1 FROM copy_trades
                    WHERE idempotency_key = %s
                      AND status = 'OPEN'
                )
            """, (idempotency_key,))
            return bool(cur.fetchone()[0])
    finally:
        release_conn(conn)


def get_copy_trade_for_token(source_wallet: str, token_id: str) -> Optional[dict]:
    """Look up our most recent OPEN copy trade for a specific (trader, token)."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT * FROM copy_trades
                WHERE source_wallet = %s AND token_id = %s AND status = 'OPEN'
                ORDER BY copied_at DESC
                LIMIT 1
            """, (source_wallet, token_id))
            return cur.fetchone()
    finally:
        release_conn(conn)


def update_copy_trade_status(trade_id: int, status: str):
    """Update the status of a copy trade (OPEN -> CLOSED/WON/LOST/CANCELLED)."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE copy_trades
                SET status = %s, resolved_at = NOW()
                WHERE id = %s
            """, (status, trade_id))
        conn.commit()
    finally:
        release_conn(conn)


def update_copy_trade_exit(trade_id: int, status: str, exit_price: float = None):
    """Update copy trade with exit price and computed PnL."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE copy_trades
                SET status = %s,
                    exit_price = %s,
                    resolved_at = NOW(),
                    pnl = CASE
                        WHEN %s IS NOT NULL AND entry_price IS NOT NULL AND shares IS NOT NULL
                        THEN (%s - entry_price) * shares
                        ELSE NULL
                    END
                WHERE id = %s
            """, (status, exit_price, exit_price, exit_price, trade_id))
        conn.commit()
    finally:
        release_conn(conn)


def get_orphaned_copy_trades() -> list:
    """
    Find copy_trades that are OPEN but the source trader has already exited
    (followed_position is CLOSED or missing).
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT ct.id, ct.source_wallet, ct.source_username, ct.token_id,
                       ct.condition_id, ct.shares, ct.market_title, ct.outcome, ct.entry_price
                FROM copy_trades ct
                LEFT JOIN followed_positions fp
                    ON ct.source_wallet = fp.proxy_wallet AND ct.token_id = fp.asset_id
                WHERE ct.status = 'OPEN'
                  AND (fp.status = 'CLOSED' OR fp.status IS NULL)
            """)
            return cur.fetchall()
    finally:
        release_conn(conn)


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
        release_conn(conn)


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
        release_conn(conn)


def is_trading_enabled() -> bool:
    """
    Returns True if live trading is enabled, False if alert-only mode.

    DB convention: 'ON' = trading enabled, 'OFF' or missing = trading disabled.
    Fail-safe default: ANY exception or non-'ON' value returns False.
    """
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT value FROM system_config WHERE key = 'kill_switch'")
                row = cur.fetchone()
                return bool(row) and row[0] == 'ON'
        finally:
            release_conn(conn)
    except Exception as e:
        print(f"[SAFETY] Kill switch check failed: {e} — defaulting to DISABLED")
        return False


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
        release_conn(conn)


def upsert_followed_position(position: dict):
    """
    Insert or update a followed position.

    `size_at_last_poll` tracks the size we observed in the *previous* poll so
    the next poll can detect partial entries/exits. On every upsert we set it
    to the current size so the next poll's comparison is well-defined.
    """
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO followed_positions (
                    proxy_wallet, asset_id, condition_id, title, slug,
                    outcome, outcome_index, size, size_at_last_poll,
                    avg_price, entry_price,
                    current_price, current_value, pre_existing, status, last_updated
                ) VALUES (
                    %(proxy_wallet)s, %(asset_id)s, %(condition_id)s, %(title)s, %(slug)s,
                    %(outcome)s, %(outcome_index)s, %(size)s, %(size)s,
                    %(avg_price)s, %(entry_price)s,
                    %(current_price)s, %(current_value)s, %(pre_existing)s, 'OPEN', NOW()
                )
                ON CONFLICT (proxy_wallet, asset_id) DO UPDATE SET
                    current_price = EXCLUDED.current_price,
                    current_value = EXCLUDED.current_value,
                    size = EXCLUDED.size,
                    size_at_last_poll = EXCLUDED.size,
                    status = 'OPEN',
                    closed_at = NULL,
                    last_updated = NOW()
            """, position)
        conn.commit()
    finally:
        release_conn(conn)


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
        release_conn(conn)


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
        release_conn(conn)


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
        release_conn(conn)


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
        release_conn(conn)
