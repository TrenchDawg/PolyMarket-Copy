# Polymarket Copy Trader

An automated system that identifies top Polymarket traders using a composite scoring algorithm and mirrors their trades.

## Architecture

```
┌────────────────────────────────────────────────────────┐
│                    Railway VPS                          │
│                                                        │
│  main.py (APScheduler)                                 │
│  ├── Every 6h  → trader_ranker.py (score traders)      │
│  ├── Every 3m  → copy_trader.py (detect & copy)        │
│  └── Daily 8pm → daily summary alert                   │
│                                                        │
│  polymarket_client.py ←→ Polymarket Data/CLOB API      │
│  db.py ←→ PostgreSQL                                   │
│  config.py (weights, thresholds, env vars)              │
└────────────────────────────────────────────────────────┘
```

## Scoring Algorithm

| Metric | Weight | Description |
|---|---|---|
| ROI % | 30% | PnL / total volume invested |
| Consistency | 30% | Sharpe-like ratio (mean return / stdev) |
| Win Rate | 20% | Profitable positions / total resolved |
| Volume | 5% | Total USDC traded (filters dust) |
| Recency | 8% | Exponential decay from last trade |
| Diversification | 7% | Distinct markets (log-scaled) |

### Hard Filters (before scoring)
- ≥20 resolved positions
- ≥$1,000 total volume
- Traded within last 14 days
- Positive PnL

## Setup

### 1. Create Railway PostgreSQL (do this first)
1. Go to [railway.app](https://railway.app), create a new project
2. Click **+ New** → **Database** → **PostgreSQL**
3. Click the PostgreSQL service → **Variables** tab
4. Copy the `DATABASE_URL` connection string (starts with `postgresql://...`)

### 2. Local Development (Windows PowerShell)
```powershell
# Navigate to the project folder
cd "C:\Users\kiran\Polymarket Copy Trader"

# Install dependencies
pip install -r requirements.txt

# Set environment variables (PowerShell syntax)
$env:DATABASE_URL = "postgresql://postgres:PASSWORD@HOST:PORT/railway"

# Optional: Gmail alerts (get an App Password from Google Account → Security)
$env:GMAIL_SENDER = "your.email@gmail.com"
$env:GMAIL_APP_PASSWORD = "your-app-password"
$env:ALERT_EMAIL_TO = "your.email@gmail.com"

# Initialize DB and run scorer once
python main.py --score-only

# Run one poll cycle
python main.py --poll-only

# Run full scheduler (dry run)
python main.py

# Run with live trading (requires CONFIRM)
python main.py --live
```

### 3. Railway Deployment
```bash
# Push to GitHub, connect to Railway
# Set these environment variables in Railway:
#   DATABASE_URL          → auto-set by Railway PostgreSQL plugin
#   GMAIL_SENDER          → your Gmail address
#   GMAIL_APP_PASSWORD    → Gmail app password
#   ALERT_EMAIL_TO        → where to receive alerts
#   POLYMARKET_API_KEY    → (for live trading, later)
#   POLYMARKET_API_SECRET
#   POLYMARKET_API_PASSPHRASE
#   POLY_WALLET_ADDRESS
#   POLY_PRIVATE_KEY
```

## Kill Switch

The kill switch is stored in the `system_config` table:

```sql
-- Enable auto-trading
UPDATE system_config SET value = 'ON' WHERE key = 'kill_switch';

-- Disable auto-trading (alert only)
UPDATE system_config SET value = 'OFF' WHERE key = 'kill_switch';
```

When OFF (default), the system still detects new positions and sends Gmail alerts — it just doesn't place orders.

## File Structure

```
polymarket_copy_trader/
├── main.py                 # Entry point + APScheduler
├── trader_ranker.py        # Scoring engine
├── copy_trader.py          # Position monitor + trade execution
├── polymarket_client.py    # Polymarket API client
├── config.py               # Configuration + env vars
├── db.py                   # PostgreSQL helpers
├── schema.sql              # Database schema
├── requirements.txt        # Python dependencies
├── Dockerfile              # Railway deployment
└── README.md               # This file
```

## TODO

- [ ] Implement live order placement via py-clob-client
- [ ] Add portfolio value tracking from Polymarket API
- [ ] Build web dashboard for monitoring
- [ ] Add email alerts alongside Slack
- [ ] Cross-reference with Kalshi data for multi-platform signals
