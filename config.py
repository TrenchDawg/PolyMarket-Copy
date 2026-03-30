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
    "roi":             0.30,
    "consistency":     0.30,
    "win_rate":        0.20,
    "volume":          0.05,
    "recency":         0.08,
    "diversification": 0.07,
}
assert abs(sum(SCORING_WEIGHTS.values()) - 1.0) < 0.001, "Weights must sum to 1.0"

# ============================================================
# Trader filters (hard cutoffs before scoring)
# ============================================================
MIN_RESOLVED_POSITIONS = 20      # minimum closed trades to be scored
MIN_VOLUME_USD = 1000            # minimum total volume
MAX_DAYS_SINCE_LAST_TRADE = 14   # must have traded recently
REQUIRE_POSITIVE_PNL = True      # only score profitable traders

# ============================================================
# Leaderboard scraping parameters
# ============================================================
LEADERBOARD_CATEGORIES = [
    "OVERALL", "POLITICS", "SPORTS", "CRYPTO",
    "CULTURE", "ECONOMICS", "TECH", "FINANCE"
]
LEADERBOARD_TIME_PERIODS = ["WEEK", "MONTH", "ALL"]
LEADERBOARD_LIMIT = 50           # max per API call
LEADERBOARD_MAX_PAGES = 4        # 50 * 4 = 200 traders per category/period

# ============================================================
# Scheduling intervals
# ============================================================
RANKING_INTERVAL_HOURS = 6       # re-score traders every 6 hours
POSITION_POLL_MINUTES = 3        # check followed traders every 3 minutes
ALERT_SUMMARY_HOUR = 20          # daily summary at 8pm

# ============================================================
# Copy trading parameters
# ============================================================
DEFAULT_PORTFOLIO_FRACTION = 0.05   # 5% of portfolio per copy trade
MAX_POSITION_USD = 50.0             # hard cap per trade
MAX_DAILY_TRADES = 10               # circuit breaker
MIN_LIQUIDITY_USD = 5000            # skip illiquid markets
MAX_SPREAD_PCT = 0.05               # skip wide-spread markets (5%)

# ============================================================
# Database
# ============================================================
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://localhost:5432/polymarket_copy_trader"
)

# ============================================================
# Alerts (Gmail)
# ============================================================
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")       # your email address
GMAIL_SENDER = os.getenv("GMAIL_SENDER", "")           # sending Gmail address
GMAIL_APP_PASSWORD = os.getenv("GMAIL_APP_PASSWORD", "") # Gmail app password

# ============================================================
# Polymarket authentication (for order placement)
# ============================================================
POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY", "")
POLYMARKET_API_SECRET = os.getenv("POLYMARKET_API_SECRET", "")
POLYMARKET_API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE", "")
POLY_WALLET_ADDRESS = os.getenv("POLY_WALLET_ADDRESS", "")
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
