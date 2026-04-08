"""
Polymarket Copy Trader — Main Scheduler

Runs:
  1. Trader ranking pipeline every 24 hours
  2. Position polling every 3 minutes
  3. Daily summary alert at 8pm UTC

Usage:
  python main.py                  # dry run (default)
  python main.py --live           # live trading (requires confirmation)
  python main.py --score-only     # just run the scorer once and exit
  python main.py --poll-only      # just run one poll cycle and exit
"""

print("[DEBUG] main.py starting...")

import sys
import signal
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from db import init_db, get_conn, get_followed_traders
from trader_ranker import score_all_traders
from copy_trader import poll_followed_traders, send_alert
from config import (
    POSITION_POLL_MINUTES,
    ALERT_SUMMARY_HOUR,
)


DRY_RUN = True


def run_scoring():
    """Scheduled: Re-score and re-rank all traders."""
    try:
        print(f"\n{'='*60}")
        print(f"[SCHEDULER] Scoring run at {datetime.now(timezone.utc).isoformat()}")
        print(f"{'='*60}")
        follow_n = 10  # could read from system_config
        score_all_traders(follow_top_n=follow_n)
    except Exception as e:
        print(f"[SCHEDULER] Scoring failed: {e}")
        import traceback
        traceback.print_exc()


def run_polling():
    """Scheduled: Poll followed traders for new positions."""
    try:
        poll_followed_traders(dry_run=DRY_RUN)
    except Exception as e:
        print(f"[SCHEDULER] Polling failed: {e}")
        import traceback
        traceback.print_exc()


def run_daily_summary():
    """Scheduled: Send daily summary alert."""
    try:
        conn = get_conn()
        try:
            with conn.cursor() as cur:
                # Today's trades
                cur.execute("""
                    SELECT COUNT(*),
                           COALESCE(SUM(pnl), 0)
                    FROM copy_trades
                    WHERE copied_at::date = CURRENT_DATE
                """)
                trades_today, daily_pnl = cur.fetchone()

                # Open positions
                cur.execute("""
                    SELECT COUNT(*) FROM copy_trades
                    WHERE status = 'OPEN'
                """)
                open_positions = cur.fetchone()[0]
        finally:
            conn.close()

        followed = get_followed_traders()

        send_alert("DAILY_SUMMARY", {
            "trades_today": trades_today,
            "daily_pnl": float(daily_pnl or 0),
            "open_positions": open_positions,
            "followed_count": len(followed),
        })
    except Exception as e:
        print(f"[SCHEDULER] Daily summary failed: {e}")


def graceful_shutdown(signum, frame):
    """Handle SIGINT/SIGTERM gracefully."""
    print("\n[SCHEDULER] Shutting down gracefully...")
    sys.exit(0)


def main():
    global DRY_RUN

    # Parse args
    if "--live" in sys.argv:
        DRY_RUN = False
        print("⚠️  LIVE MODE — real orders will be placed!")
        confirm = input("Type 'CONFIRM' to proceed: ")
        if confirm != "CONFIRM":
            print("Aborted.")
            sys.exit(0)

    # Initialize database
    print("[MAIN] Initializing database...")
    init_db()

    # One-shot modes
    if "--score-only" in sys.argv:
        score_all_traders(follow_top_n=10)
        return

    if "--poll-only" in sys.argv:
        poll_followed_traders(dry_run=DRY_RUN)
        return

    # Set up signal handlers
    signal.signal(signal.SIGINT, graceful_shutdown)
    signal.signal(signal.SIGTERM, graceful_shutdown)

    # Set up scheduler
    scheduler = BlockingScheduler(timezone="UTC")

    # Job 1: Score traders every other day at 11pm UTC
    scheduler.add_job(
        run_scoring,
        trigger=CronTrigger(day='*/2', hour=23, minute=0),
        id="scoring",
        name="Trader Scoring Pipeline",
        next_run_time=datetime.now(timezone.utc),  # run immediately on startup
    )

    # Job 2: Poll followed traders every N minutes
    scheduler.add_job(
        run_polling,
        trigger=IntervalTrigger(minutes=POSITION_POLL_MINUTES),
        id="polling",
        name="Position Polling",
        # Don't run immediately — let scoring finish first
    )

    # Job 3: Daily summary
    scheduler.add_job(
        run_daily_summary,
        trigger=CronTrigger(hour=ALERT_SUMMARY_HOUR, minute=0),
        id="daily_summary",
        name="Daily Summary Alert",
    )

    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"\n{'='*60}")
    print(f"  Polymarket Copy Trader — {mode} MODE")
    print(f"  Scoring: every other day at 23:00 UTC")
    print(f"  Polling: every {POSITION_POLL_MINUTES}min")
    print(f"  Summary: daily at {ALERT_SUMMARY_HOUR}:00 UTC")
    print(f"{'='*60}\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[MAIN] Scheduler stopped.")


if __name__ == "__main__":
    main()
