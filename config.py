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
MIN_ROI = 0.01                   # 1% minimum blended ROI

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
POSITION_POLL_MINUTES = 3        # check followed traders every 3 minutes
ALERT_SUMMARY_HOUR = 20          # daily summary at 8pm
ALERT_COOLDOWN_MINUTES = 30      # min minutes between alerts for same trader

# ============================================================
# Copy trading parameters
# ============================================================
DEFAULT_PORTFOLIO_FRACTION = 0.05   # 5% of portfolio per copy trade
MAX_POSITION_USD = 50.0             # hard cap per trade
MAX_DAILY_TRADES = 10               # circuit breaker
MIN_LIQUIDITY_USD = 5000            # skip illiquid markets
MAX_SPREAD_PCT = 0.05               # skip wide-spread markets (5%)

# ============================================================
# Open position health check
# ============================================================
HEALTH_HARD_FILTER = -0.50      # exclude traders losing >50% of portfolio on open positions
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
# Position allocation
# ============================================================
MAX_POSITION_PCT = 0.15          # max 15% of account on any single trade
MIN_POSITION_CONTRACTS = 1       # minimum 1 contract per trade
ALLOCATION_SHARPE_WEIGHT = 0.5   # 50% weight to consistency in allocation calc
ALLOCATION_ROI_WEIGHT = 0.5      # 50% weight to ROI in allocation calc
ACCOUNT_BALANCE_USD = 1000.0     # starting account balance (will be dynamic later)

# ============================================================
# Polymarket CLOB (order execution)
# ============================================================
POLYMARKET_CLOB_HOST = "https://clob.polymarket.com"
POLYMARKET_CHAIN_ID = 137           # Polygon
POLYMARKET_SIGNATURE_TYPE = 1       # 1 = Magic/email wallet

# ============================================================
# Polymarket authentication (for order placement)
# ============================================================
POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY", "")
POLYMARKET_API_SECRET = os.getenv("POLYMARKET_API_SECRET", "")
POLYMARKET_API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "")
POLY_WALLET_ADDRESS = os.getenv("POLY_WALLET_ADDRESS", "")
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
