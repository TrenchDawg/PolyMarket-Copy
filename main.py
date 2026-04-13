"""
Polymarket Copy Trader — Main Scheduler

Three-job architecture:
  1. Normal poll (every 2 min)   — scan ALL followed traders, detect entries/exits,
                                    save to DB, send alerts. NO order placement.
  2. Active poll (every 30 sec)  — fast-poll recently-active traders, place BUY/SELL orders.
  3. Reconciliation (every 5 min) — find orphaned copy_trades where the source trader
                                    exited but we still hold, and place SELL orders.
  4. Trader scoring (every 2 days at 23:00 UTC)
  5. Daily summary alert (at 8pm UTC)

Usage:
  python main.py                  # dry run (default)
  python main.py --live           # live trading (requires confirmation)
  python main.py --score-only     # just run the scorer once and exit
  python main.py --poll-only      # just run one normal poll cycle and exit
  python main.py --reconcile-only # just run one reconciliation cycle and exit
"""

print("[DEBUG] main.py starting...")

import sys
import signal
from datetime import datetime, timezone
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger
from apscheduler.triggers.cron import CronTrigger

from db import init_db, get_conn, release_conn, get_followed_traders
from trader_ranker import score_all_traders
from copy_trader import poll_followed_traders, run_reconciliation, send_alert
from config import (
    POSITION_POLL_SECONDS,
    ACTIVE_TRADER_POLL_SECONDS,
    RECONCILIATION_INTERVAL_SECONDS,
    ALERT_SUMMARY_HOUR,
)


DRY_RUN = True


def run_scoring():
    """Scheduled: Re-score and re-rank all traders."""
    try:
        print(f"\n{'='*60}")
        print(f"[SCHEDULER] Scoring run at {datetime.now(timezone.utc).isoformat()}")
        print(f"{'='*60}")
        score_all_traders()
    except Exception as e:
        print(f"[SCHEDULER] Scoring failed: {e}")
        import traceback
        traceback.print_exc()


def run_normal_poll():
    """Scheduled: Scan ALL followed traders for changes. Detection only, no orders."""
    try:
        poll_followed_traders(dry_run=DRY_RUN, mode="normal")
    except Exception as e:
        print(f"[SCHEDULER] Normal poll failed: {e}")
        import traceback
        traceback.print_exc()


def run_active_poll():
    """Scheduled: Fast-poll active traders and place orders."""
    try:
        poll_followed_traders(dry_run=DRY_RUN, mode="active")
    except Exception as e:
        print(f"[SCHEDULER] Active poll failed: {e}")
        import traceback
        traceback.print_exc()


def run_reconcile():
    """Scheduled: Find orphaned positions and sell them."""
    try:
        run_reconciliation(dry_run=DRY_RUN)
    except Exception as e:
        print(f"[SCHEDULER] Reconciliation failed: {e}")
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

                # Closed today with real PnL
                cur.execute("""
                    SELECT COUNT(*),
                           COALESCE(SUM(pnl), 0)
                    FROM copy_trades
                    WHERE resolved_at::date = CURRENT_DATE
                      AND pnl IS NOT NULL
                """)
                closed_today, realized_pnl = cur.fetchone()

                # Open positions
                cur.execute("""
                    SELECT COUNT(*) FROM copy_trades
                    WHERE status = 'OPEN'
                """)
                open_positions = cur.fetchone()[0]
        finally:
            release_conn(conn)

        followed = get_followed_traders()

        send_alert("DAILY_SUMMARY", {
            "trades_today": trades_today,
            "daily_pnl": float(daily_pnl or 0),
            "realized_pnl": float(realized_pnl or 0),
            "closed_today": closed_today,
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
        print("WARNING: LIVE MODE — real orders will be placed!")
        confirm = input("Type 'CONFIRM' to proceed: ")
        if confirm != "CONFIRM":
            print("Aborted.")
            sys.exit(0)

    # Initialize database
    print("[MAIN] Initializing database...")
    init_db()

    # One-shot modes
    if "--score-only" in sys.argv:
        score_all_traders()
        return

    if "--poll-only" in sys.argv:
        poll_followed_traders(dry_run=DRY_RUN, mode="normal")
        return

    if "--reconcile-only" in sys.argv:
        run_reconciliation(dry_run=DRY_RUN)
        return

    if "--test-order" in sys.argv:
        from copy_trader import get_clob_client
        from py_clob_client.clob_types import OrderArgs
        clob = get_clob_client()
        price = clob.get_price('42226471287631305147124009130697472279390700811292616532685434752232432877995', 'buy')
        print(f"Celtics price: {price}")
        p = float(price['price'])
        result = clob.create_and_post_order(OrderArgs(token_id='42226471287631305147124009130697472279390700811292616532685434752232432877995', price=p, size=round(1.0/p, 2), side='BUY'))
        print(f"Result: {result}")
        if isinstance(result, dict):
            for k, v in result.items():
                print(f"  {k}: {v} ({type(v).__name__})")
        sys.exit(0)

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
        max_instances=1,
        coalesce=True,
    )

    # Job 2: Normal poll — detection only, no orders
    scheduler.add_job(
        run_normal_poll,
        trigger=IntervalTrigger(seconds=POSITION_POLL_SECONDS),
        id="normal_polling",
        name="Normal Position Polling (detection only)",
        max_instances=1,
        coalesce=True,
    )

    # Job 3: Active trader poll — places BUY/SELL orders
    scheduler.add_job(
        run_active_poll,
        trigger=IntervalTrigger(seconds=ACTIVE_TRADER_POLL_SECONDS),
        id="active_polling",
        name="Active Trader Polling (order execution)",
        max_instances=1,
        coalesce=True,
    )

    # Job 4: Reconciliation — catches orphaned positions
    scheduler.add_job(
        run_reconcile,
        trigger=IntervalTrigger(seconds=RECONCILIATION_INTERVAL_SECONDS),
        id="reconciliation",
        name="Position Reconciliation (orphan cleanup)",
        max_instances=1,
        coalesce=True,
    )

    # Job 5: Daily summary
    scheduler.add_job(
        run_daily_summary,
        trigger=CronTrigger(hour=ALERT_SUMMARY_HOUR, minute=0),
        id="daily_summary",
        name="Daily Summary Alert",
        max_instances=1,
        coalesce=True,
    )

    mode = "DRY RUN" if DRY_RUN else "LIVE"
    print(f"\n{'='*60}")
    print(f"  Polymarket Copy Trader — {mode} MODE")
    print(f"  Scoring:         every other day at 23:00 UTC")
    print(f"  Normal poll:     every {POSITION_POLL_SECONDS}s (detection only)")
    print(f"  Active poll:     every {ACTIVE_TRADER_POLL_SECONDS}s (order execution)")
    print(f"  Reconciliation:  every {RECONCILIATION_INTERVAL_SECONDS}s (orphan cleanup)")
    print(f"  Summary:         daily at {ALERT_SUMMARY_HOUR}:00 UTC")
    print(f"{'='*60}\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("[MAIN] Scheduler stopped.")


if __name__ == "__main__":
    main()
