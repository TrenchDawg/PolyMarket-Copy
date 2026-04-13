"""
Polymarket Copy Trader — Configuration
"""
import os

# ============================================================
# Polymarket API endpoints
# ============================================================
POLYMARKET_DATA_API = "https://data-api.polymarket.com"
POLYMARKET_CLOB_API = "https://clob.polymarket.com"
POLYMARKET_GAMMA_API = "https://gamma-api.polymarket.com"

# ============================================================
# Scoring weights (must sum to 1.0)
# ============================================================
SCORING_WEIGHTS = {
    "consistency": 0.35,   # Sharpe ratio — steady returns, low variance
    "win_rate":    0.30,   # High hit rate means fewer drawdowns
    "roi":         0.20,   # Positive returns, but size doesn't matter
    "recency":     0.15,   # Must be active and winning NOW
}
assert abs(sum(SCORING_WEIGHTS.values()) - 1.0) < 0.001, "Weights must sum to 1.0"

# ============================================================
# Trader filters (hard cutoffs before scoring)
# ============================================================
MIN_RESOLVED_POSITIONS = 30      # minimum closed trades to be scored
MIN_VOLUME_USD = 500            # minimum total volume (basic dust filter)
MAX_DAYS_SINCE_LAST_TRADE = 14   # must have traded recently
REQUIRE_POSITIVE_PNL = True      # only score profitable traders
MIN_RECENT_WIN_RATE = 0.90       # 90% win rate in last 30 days
MIN_POSITIONS_VALUE = 0.01       # must have live positions worth >$0
MIN_ROI = 0.025                  # 2.5% minimum blended ROI

# ============================================================
# Leaderboard scraping parameters
# ============================================================
LEADERBOARD_CATEGORIES = ["OVERALL", "POLITICS", "SPORTS", "CRYPTO", "ECONOMICS", "FINANCE", "TECH"]
LEADERBOARD_TIME_PERIODS = ["MONTH", "ALL"]
LEADERBOARD_LIMIT = 50           # max per API call
LEADERBOARD_MAX_PAGES = 6        # 50 * 6 = 300 traders per period

# ============================================================
# Scheduling intervals
# ============================================================
POSITION_POLL_SECONDS = 120      # normal poll: check all followed traders every 2 minutes
ACTIVE_TRADER_POLL_SECONDS = 30  # poll every 30s for traders with recent activity
ACTIVE_TRADER_WINDOW_MINUTES = 10  # how long a trader stays in "active" mode after a detected entry or exit
RECONCILIATION_INTERVAL_SECONDS = 300  # reconciliation: check for orphaned positions every 5 minutes
ALERT_SUMMARY_HOUR = 20          # daily summary at 8pm
REALTIME_ALERT_MAX_POSITIONS = 50  # Only send real-time alerts for traders with ≤ 50 open positions

# ============================================================
# Copy trading parameters
# ============================================================
MAX_TRADE_SIZE_USD = 50.0            # Hard cap regardless of account size
MAX_DAILY_TRADES = 10               # circuit breaker
MAX_SPREAD_PCT = 0.05               # skip wide-spread markets (5%)
PARTIAL_SIZE_CHANGE_THRESHOLD = 0.20  # Alert if trader resizes a position by more than 20%

# ============================================================
# Order rounding (Polymarket CLOB increments)
# ============================================================
TICK_SIZE = 0.01        # minimum price increment (1 cent)
LOT_SIZE = 0.01         # minimum order size increment
MIN_ORDER_SIZE = 5.00   # Polymarket minimum is 5 shares

# ============================================================
# Open position health check
# ============================================================
HEALTH_HARD_FILTER = -0.20      # exclude traders losing >20% of portfolio on open positions
HEALTH_PENALTY_FACTOR = 0.5     # how aggressively to penalize (0.5 = moderate)

# ============================================================
# Efficiency bonus (rewards traders strong on BOTH WR and ROI)
# ============================================================
EFFICIENCY_BONUS_MAX = 0.15     # max composite score boost (15%)
EFFICIENCY_WR_BASELINE = 0.89   # WR baseline for efficiency calc (90% WR → small positive)

# ============================================================
# Recent performance weighting
# ============================================================
RECENT_WINDOW_DAYS = 30         # positions closed in last 30 days = "recent"
RECENT_WEIGHT = 0.70            # weight for recent metrics
OLDER_WEIGHT = 0.30             # weight for older metrics (must sum to 1.0 with RECENT_WEIGHT)

# ============================================================
# Database
# ============================================================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://localhost:5432/polymarket_copy_trader"
)

# ============================================================
# Alerts (n8n webhook)
# ============================================================
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL", "")

# ============================================================
# Position sizing (U-shaped, balance-relative)
# ============================================================
# High-probability (90¢+) and high-EV (60-70¢) prices get more capital;
# dead zone around 80¢ gets minimum allocation. Floor and ceiling scale
# with account balance so sizing stays proportional as the account grows.
TRADE_SIZE_FLOOR_PCT = 0.01      # 1% of account = minimum trade
TRADE_SIZE_CEILING_PCT = 0.05    # 5% of account = maximum trade
TRADE_SIZE_MIDPOINT = 0.80       # Dead zone center — lowest allocation
TRADE_SIZE_MAX_DISTANCE = 0.20   # Max distance from midpoint for normalization

# ============================================================
# Polymarket CLOB (order execution)
# ============================================================
POLYMARKET_CLOB_HOST = "https://clob.polymarket.com"
POLYMARKET_CHAIN_ID = 137           # Polygon
POLYMARKET_SIGNATURE_TYPE = 1       # 1 = Magic/email wallet

# ============================================================
# Polymarket authentication (for order placement)
# ============================================================
POLY_API_KEY = os.getenv("POLY_API_KEY", "")
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "")
POLY_WALLET_ADDRESS = os.getenv("POLY_WALLET_ADDRESS", "")
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")

# ============================================================
# Order Proxy (Madrid — routes CLOB calls through non-US VPS)
# ============================================================
ORDER_PROXY_URL = os.getenv("ORDER_PROXY_URL", "")
ORDER_PROXY_AUTH_TOKEN = os.getenv("ORDER_PROXY_AUTH_TOKEN", "")
