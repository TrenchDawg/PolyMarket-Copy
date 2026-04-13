-- Polymarket Copy Trader — PostgreSQL Schema
-- Run once to initialize the database

-- ============================================================
-- TRADERS: Scored trader profiles from the leaderboard
-- ============================================================
CREATE TABLE IF NOT EXISTS traders (
    proxy_wallet    VARCHAR(42) PRIMARY KEY,  -- 0x-prefixed address
    username        VARCHAR(100),
    profile_image   TEXT,
    x_username      VARCHAR(100),
    verified_badge  BOOLEAN DEFAULT FALSE,

    -- Raw metrics (from Polymarket API)
    total_pnl       NUMERIC(18,2) DEFAULT 0,
    total_volume    NUMERIC(18,2) DEFAULT 0,
    total_positions INTEGER DEFAULT 0,
    active_positions INTEGER DEFAULT 0,

    -- Computed metrics (from our scoring engine)
    roi_pct             NUMERIC(10,4),   -- PnL / total invested
    win_rate            NUMERIC(6,4),    -- wins / total resolved
    consistency_score   NUMERIC(10,4),   -- mean return / stdev (Sharpe-like)
    recency_score       NUMERIC(6,4),    -- weighted toward last 30 days
    diversification     NUMERIC(6,4),    -- distinct markets / total positions
    composite_score     NUMERIC(10,4),   -- final weighted score

    -- Metadata
    last_scored_at  TIMESTAMPTZ,
    first_seen_at   TIMESTAMPTZ DEFAULT NOW(),
    is_followed     BOOLEAN DEFAULT FALSE,  -- are we actively copying this trader?
    last_alert_at   TIMESTAMPTZ,            -- when we last sent an alert for this trader

    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_traders_composite ON traders(composite_score DESC);
CREATE INDEX IF NOT EXISTS idx_traders_followed ON traders(is_followed) WHERE is_followed = TRUE;

-- ============================================================
-- TRADER_POSITIONS: Snapshot of each followed trader's positions
-- Used to detect NEW positions (delta from previous snapshot)
-- ============================================================
CREATE TABLE IF NOT EXISTS trader_positions (
    id              SERIAL PRIMARY KEY,
    proxy_wallet    VARCHAR(42) NOT NULL REFERENCES traders(proxy_wallet),
    condition_id    VARCHAR(66) NOT NULL,     -- market condition ID
    asset_id        TEXT NOT NULL,            -- outcome token asset ID
    title           TEXT,                     -- market title
    outcome         VARCHAR(100),             -- "Yes" or "No"
    size            NUMERIC(18,6),           -- number of shares
    avg_price       NUMERIC(10,6),           -- average entry price
    current_price   NUMERIC(10,6),
    market_slug     TEXT,

    snapshot_at     TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(proxy_wallet, asset_id, snapshot_at)
);

CREATE INDEX IF NOT EXISTS idx_positions_wallet ON trader_positions(proxy_wallet, snapshot_at DESC);

-- ============================================================
-- COPY_TRADES: Our mirror trades — what we actually executed
-- ============================================================
CREATE TABLE IF NOT EXISTS copy_trades (
    id                  SERIAL PRIMARY KEY,
    source_wallet       VARCHAR(42) NOT NULL,     -- trader we copied
    source_username     VARCHAR(100),
    condition_id        VARCHAR(66) NOT NULL,
    token_id            TEXT NOT NULL,
    market_title        TEXT,
    market_slug         TEXT,
    outcome             VARCHAR(10),              -- "Yes" or "No"
    side                VARCHAR(4) NOT NULL,       -- "BUY" or "SELL"

    -- Our execution details
    entry_price         NUMERIC(10,6),
    size_usd            NUMERIC(18,2),
    shares              NUMERIC(18,6),
    order_id            TEXT,                      -- Polymarket order ID
    idempotency_key     TEXT,                      -- uniqueness enforced by a PARTIAL index (OPEN only) so re-entry after exit is allowed

    -- Result tracking
    exit_price          NUMERIC(10,6),
    pnl                 NUMERIC(18,2),
    status              VARCHAR(20) DEFAULT 'OPEN', -- OPEN, WON, LOST, CANCELLED

    -- Metadata
    copied_at           TIMESTAMPTZ DEFAULT NOW(),
    resolved_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_copy_trades_status ON copy_trades(status);
CREATE INDEX IF NOT EXISTS idx_copy_trades_source ON copy_trades(source_wallet);

-- Idempotency: OPEN and PENDING copy_trades must have a unique key. Once a trade is
-- CLOSED/FAILED the same (source, token) can be re-entered without conflict.
-- PENDING records are created BEFORE order placement (B-04 fix) so a crash
-- between order and DB update still leaves a visible breadcrumb.
DROP INDEX IF EXISTS idx_copy_trades_idem_open;
CREATE UNIQUE INDEX IF NOT EXISTS idx_copy_trades_idem_active
    ON copy_trades(idempotency_key)
    WHERE status IN ('OPEN', 'PENDING') AND idempotency_key IS NOT NULL;

-- Migration step 1: add the idempotency_key column on existing tables
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'copy_trades' AND column_name = 'idempotency_key') THEN
        ALTER TABLE copy_trades ADD COLUMN idempotency_key TEXT;
    END IF;
END $$;

-- Migration step 2: drop any legacy full-table UNIQUE constraint left over
-- from the previous iteration of this schema (it would block re-entry).
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.table_constraints
               WHERE table_name = 'copy_trades'
                 AND constraint_name = 'copy_trades_idempotency_key_key'
                 AND constraint_type = 'UNIQUE') THEN
        ALTER TABLE copy_trades DROP CONSTRAINT copy_trades_idempotency_key_key;
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'traders' AND column_name = 'allocation_pct') THEN
        ALTER TABLE traders ADD COLUMN allocation_pct NUMERIC(6,4) DEFAULT 0;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name = 'traders' AND column_name = 'last_alert_at') THEN
        ALTER TABLE traders ADD COLUMN last_alert_at TIMESTAMPTZ;
    END IF;
END $$;

-- ============================================================
-- SCORING_HISTORY: Track how trader scores change over time
-- ============================================================
CREATE TABLE IF NOT EXISTS scoring_history (
    id              SERIAL PRIMARY KEY,
    proxy_wallet    VARCHAR(42) NOT NULL REFERENCES traders(proxy_wallet),
    composite_score NUMERIC(10,4),
    roi_pct         NUMERIC(10,4),
    win_rate        NUMERIC(6,4),
    consistency     NUMERIC(10,4),
    scored_at       TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scoring_history_wallet ON scoring_history(proxy_wallet, scored_at DESC);

-- ============================================================
-- FOLLOWED_POSITIONS: Living record of positions held by followed traders
-- Not snapshots — tracks current state with entry/exit detection
-- ============================================================
CREATE TABLE IF NOT EXISTS followed_positions (
    id              SERIAL PRIMARY KEY,
    proxy_wallet    VARCHAR(42) NOT NULL,
    asset_id        TEXT NOT NULL,
    condition_id    VARCHAR(66) NOT NULL,
    title           TEXT,
    slug            TEXT,
    outcome         VARCHAR(100),
    outcome_index   INTEGER,

    -- Entry data
    size            NUMERIC(18,6),
    size_at_last_poll NUMERIC(18,6) DEFAULT 0,   -- size we saw in the previous poll (for partial-change detection)
    avg_price       NUMERIC(10,6),
    entry_price     NUMERIC(10,6),        -- price when we first detected it

    -- Current data (updated each poll)
    current_price   NUMERIC(10,6),
    current_value   NUMERIC(18,2),

    -- Tracking
    pre_existing    BOOLEAN DEFAULT FALSE, -- TRUE = was there before we started following
    status          VARCHAR(20) DEFAULT 'OPEN',  -- OPEN, CLOSED

    -- Timestamps
    detected_at     TIMESTAMPTZ DEFAULT NOW(),   -- when we first saw this position
    closed_at       TIMESTAMPTZ,                 -- when position disappeared
    last_updated    TIMESTAMPTZ DEFAULT NOW(),

    UNIQUE(proxy_wallet, asset_id)
);

CREATE INDEX IF NOT EXISTS idx_followed_positions_wallet ON followed_positions(proxy_wallet);
CREATE INDEX IF NOT EXISTS idx_followed_positions_status ON followed_positions(status);
CREATE INDEX IF NOT EXISTS idx_followed_positions_open ON followed_positions(proxy_wallet, status) WHERE status = 'OPEN';

-- Migration: add size_at_last_poll to existing followed_positions tables
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'followed_positions' AND column_name = 'size_at_last_poll') THEN
        ALTER TABLE followed_positions ADD COLUMN size_at_last_poll NUMERIC(18,6) DEFAULT 0;
    END IF;
END $$;

-- ============================================================
-- SYSTEM_CONFIG: Runtime configuration (kill switch, etc.)
-- ============================================================
CREATE TABLE IF NOT EXISTS system_config (
    key         VARCHAR(50) PRIMARY KEY,
    value       TEXT NOT NULL,
    updated_at  TIMESTAMPTZ DEFAULT NOW()
);

-- Initialize kill switch (ON = trading enabled, OFF = stopped)
INSERT INTO system_config (key, value) VALUES
    ('kill_switch', 'OFF'),              -- OFF = no auto-trading
    ('max_position_usd', '50'),          -- max per copy trade
    ('max_daily_trades', '10'),          -- circuit breaker
    ('min_composite_score', '0.6'),      -- only copy traders above this
    ('portfolio_fraction', '0.05')       -- 5% of portfolio per copy trade
ON CONFLICT (key) DO NOTHING;

-- ============================================================
-- ALERTS_LOG: Record of all alerts sent
-- ============================================================
CREATE TABLE IF NOT EXISTS alerts_log (
    id          SERIAL PRIMARY KEY,
    alert_type  VARCHAR(30) NOT NULL,  -- NEW_TRADE, DAILY_SUMMARY, KILL_SWITCH, SCORE_UPDATE
    payload     JSONB,
    sent_at     TIMESTAMPTZ DEFAULT NOW()
);
