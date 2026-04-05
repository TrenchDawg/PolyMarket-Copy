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
    outcome         VARCHAR(10),             -- "Yes" or "No"
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
    ('follow_top_n', '10'),             -- number of traders to follow
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
