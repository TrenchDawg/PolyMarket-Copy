"""
Polymarket Copy Trader — Trader Ranking Engine

Scores traders using a weighted composite of:
  Consistency (35%) + Win Rate (30%) + ROI (20%) + Recency (15%)
"""
import math
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional
from polymarket_client import PolymarketClient
from config import (
    POLYMARKET_DATA_API,
    SCORING_WEIGHTS,
    MIN_RESOLVED_POSITIONS,
    MIN_VOLUME_USD,
    MAX_DAYS_SINCE_LAST_TRADE,
    REQUIRE_POSITIVE_PNL,
    MIN_RECENT_WIN_RATE,
    MIN_POSITIONS_VALUE,
    MIN_ROI,
    MIN_RECENT_TRADES_PER_DAY,
    LEADERBOARD_CATEGORIES,
    LEADERBOARD_TIME_PERIODS,
    HEALTH_HARD_FILTER,
    HEALTH_PENALTY_FACTOR,
    RECENT_WINDOW_DAYS,
    RECENT_WEIGHT,
    OLDER_WEIGHT,
    EFFICIENCY_BONUS_MAX,
    EFFICIENCY_WR_BASELINE,
)
from db import update_follow_list, get_conn


client = PolymarketClient()


# ============================================================
# Step 1: Discover traders from leaderboard
# ============================================================

def discover_traders() -> dict:
    """
    Pull leaderboard across all categories and time periods.
    Returns a dict of {proxy_wallet: leaderboard_data} (deduplicated).
    """
    traders = {}
    for category in LEADERBOARD_CATEGORIES:
        for period in LEADERBOARD_TIME_PERIODS:
            print(f"[DISCOVER] Fetching {category} / {period}...")
            batch = client.get_full_leaderboard(
                category=category,
                time_period=period,
                order_by="PNL",
            )
            for t in batch:
                wallet = t.get("proxyWallet", "").lower()
                if wallet and wallet not in traders:
                    traders[wallet] = t
            print(f"  → {len(batch)} traders, {len(traders)} unique total")
    return traders


# ============================================================
# Step 2: Enrich trader with position/trade data
# ============================================================

def enrich_trader(wallet: str) -> Optional[dict]:
    """
    Fetch detailed position and trade data for a trader.
    Returns enrichment dict or None on failure.
    """
    # Get open positions (for active count)
    positions = client.get_positions(wallet, size_threshold=0)
    active_count = len(positions) if positions else 0

    # Get closed positions (all resolved markets, sorted by timestamp)
    closed = client.get_closed_positions(wallet)
    if not closed:
        closed = []

    # Deduplicate closed positions by conditionId + asset
    seen = set()
    unique_closed = []
    for p in closed:
        key = (p.get("conditionId", ""), p.get("asset", ""))
        if key not in seen:
            seen.add(key)
            unique_closed.append(p)
    closed = unique_closed

    # Get most recent trade only (for recency calculation)
    trades = client.get_trades(wallet, limit=1)

    # Current market value of all positions (from /value endpoint)
    positions_value = client.get_portfolio_value_usd(wallet)

    # Compute open position health using initialValue (cost basis) from /positions
    unrealized_pnl = sum(float(p.get("cashPnl", 0)) for p in positions) if positions else 0
    initial_deployed = sum(float(p.get("initialValue", 0)) for p in positions) if positions else 0

    if initial_deployed > 0:
        health_ratio = unrealized_pnl / initial_deployed
    else:
        health_ratio = 0.0

    return {
        "positions": positions or [],
        "closed_positions": closed,
        "trades": trades or [],
        "active_count": active_count,
        "total_positions": len(closed) + active_count,
        "positions_value": positions_value,
        "unrealized_pnl": unrealized_pnl,
        "initial_deployed": initial_deployed,
        "health_ratio": health_ratio,
    }


# ============================================================
# Step 3: Compute individual metrics
# ============================================================

def compute_roi(closed_positions: list, total_volume: float) -> float:
    """ROI = sum(realizedPnl) / sum(totalBought) across all closed positions."""
    total_bought = sum(float(p.get("totalBought", 0)) for p in closed_positions)
    total_pnl = sum(float(p.get("realizedPnl", 0)) for p in closed_positions)
    if total_bought > 0:
        return total_pnl / total_bought
    if total_volume > 0:
        return total_pnl / total_volume
    return 0.0


def compute_win_rate(closed_positions: list) -> float:
    """Win rate = count(realizedPnl > 0) / count(all closed positions)."""
    if not closed_positions:
        return 0.0
    wins = sum(1 for p in closed_positions if float(p.get("realizedPnl", 0)) > 0)
    return wins / len(closed_positions)


def compute_consistency(closed_positions: list) -> float:
    """
    Sharpe-like ratio: mean(returns) / stdev(returns).
    return = realizedPnl / totalBought for each closed position.
    Capped to [0, 3.0].
    """
    returns = []
    for p in closed_positions:
        bought = float(p.get("totalBought", 0))
        pnl = float(p.get("realizedPnl", 0))
        if bought > 0:
            returns.append(pnl / bought)

    if len(returns) < 5:
        return 0.0

    mean_ret = statistics.mean(returns)
    std_ret = statistics.stdev(returns)

    if std_ret == 0:
        return 3.0 if mean_ret > 0 else 0.0

    sharpe = mean_ret / std_ret
    return min(max(sharpe, 0.0), 3.0)


def compute_recency(trades: list) -> tuple:
    """
    Returns (recency_score, days_since_last_trade).
    recency = exp(-0.1 * days_since_last_trade)
    Returns (0.0, inf) if no trades or too old.
    """
    if not trades:
        return 0.0, float("inf")

    latest = None
    for t in trades:
        ts = t.get("timestamp") or t.get("createdAt")
        if ts:
            try:
                if isinstance(ts, (int, float)):
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
                else:
                    dt = datetime.fromisoformat(str(ts).replace("Z", "+00:00"))
                if latest is None or dt > latest:
                    latest = dt
            except (ValueError, OSError):
                continue

    if latest is None:
        return 0.0, float("inf")

    days_ago = (datetime.now(timezone.utc) - latest).total_seconds() / 86400
    if days_ago > MAX_DAYS_SINCE_LAST_TRADE:
        return 0.0, days_ago

    return math.exp(-0.1 * days_ago), days_ago


# ============================================================
# Step 4: Normalize and compute composite score
# ============================================================

def normalize(value: float, min_val: float, max_val: float) -> float:
    """Min-max normalize to [0, 1]."""
    if max_val <= min_val:
        return 0.5
    return max(0.0, min(1.0, (value - min_val) / (max_val - min_val)))


def compute_composite_score(metrics: dict, normalization_ranges: dict) -> float:
    """Weighted composite score from normalized metrics."""
    score = 0.0
    for metric, weight in SCORING_WEIGHTS.items():
        raw = metrics.get(metric, 0)
        r = normalization_ranges.get(metric, (0, 1))
        normalized = normalize(raw, r[0], r[1])
        score += weight * normalized
    return round(score, 4)


# ============================================================
# Step 5: Recency split helper
# ============================================================

def split_by_recency(closed_positions: list, days: int = 30) -> tuple:
    """Split closed positions into recent (last N days) and older."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_ts = cutoff.timestamp()

    recent = []
    older = []
    for p in closed_positions:
        ts = p.get("timestamp", 0)
        if isinstance(ts, str):
            try:
                ts = datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
            except (ValueError, OSError):
                ts = 0
        if float(ts) >= cutoff_ts:
            recent.append(p)
        else:
            older.append(p)
    return recent, older


# ============================================================
# Step 6: Validation report
# ============================================================

def validate_top_traders(scored_traders: list, n: int = 10):
    """Print a detailed validation report for the top N traders."""
    print(f"\n{'=' * 60}")
    print(f"[VALIDATE] Top {n} Trader Deep Dive")
    print(f"{'=' * 60}")

    for i, t in enumerate(scored_traders[:n]):
        lb = t["leaderboard"]
        m = t["metrics"]
        closed = t["enrichment"]["closed_positions"]
        wallet = t["wallet"]
        name = lb.get("userName") or wallet[:10]

        computed_pnl = sum(float(p.get("realizedPnl", 0)) for p in closed)
        wins = [p for p in closed if float(p.get("realizedPnl", 0)) > 0]
        losses = [p for p in closed if float(p.get("realizedPnl", 0)) < 0]

        biggest_win = max(wins, key=lambda p: float(p.get("realizedPnl", 0)), default=None)
        biggest_loss = min(losses, key=lambda p: float(p.get("realizedPnl", 0)), default=None)

        days = t.get("days_since_trade", 0)
        health_multiplier = t.get("health_multiplier", 1.0)

        # Health info
        unrealized_pnl = t["enrichment"]["unrealized_pnl"]
        initial_deployed = t["enrichment"]["initial_deployed"]
        positions_value = t["enrichment"]["positions_value"]
        health_ratio = t["enrichment"]["health_ratio"]

        # Leaderboard PnL from scored data (no extra API calls)
        lb_month_pnl = t.get("month_pnl", 0)
        lb_all_pnl = t.get("all_pnl", 0)

        # Recency split
        recent_closed, older_closed = split_by_recency(closed, RECENT_WINDOW_DAYS)
        recent_wins = [p for p in recent_closed if float(p.get("realizedPnl", 0)) > 0]
        older_wins = [p for p in older_closed if float(p.get("realizedPnl", 0)) > 0]

        recent_wr = len(recent_wins) / len(recent_closed) if recent_closed else 0
        older_wr = len(older_wins) / len(older_closed) if older_closed else 0
        total_bought_r = sum(float(p.get("totalBought", 0)) for p in recent_closed)
        total_bought_o = sum(float(p.get("totalBought", 0)) for p in older_closed)
        recent_roi = (sum(float(p.get("realizedPnl", 0)) for p in recent_closed) / total_bought_r
                      if total_bought_r > 0 else 0)
        older_roi = (sum(float(p.get("realizedPnl", 0)) for p in older_closed) / total_bought_o
                     if total_bought_o > 0 else 0)
        if recent_closed and older_closed:
            blended_wr = RECENT_WEIGHT * recent_wr + OLDER_WEIGHT * older_wr
            blended_roi = RECENT_WEIGHT * recent_roi + OLDER_WEIGHT * older_roi
        elif recent_closed:
            blended_wr = recent_wr
            blended_roi = recent_roi
        else:
            blended_wr = older_wr
            blended_roi = older_roi

        print(f"\n{'=' * 60}")
        print(f"TRADER #{i+1}: {name} ({wallet[:6]}...{wallet[-4:]})")
        print(f"{'=' * 60}")
        efficiency_bonus = t.get("efficiency_bonus", 1.0)
        print(f"Composite Score: {t['composite_score']:.3f} (health multiplier: {health_multiplier:.2f}, efficiency bonus: {efficiency_bonus:.2f}x)")
        print(f"Positions Value: ${positions_value:,.0f}")
        print(
            f"Health: {unrealized_pnl:+,.0f} unrealized / "
            f"${initial_deployed:,.0f} deployed "
            f"(ratio: {health_ratio:+.1%})"
        )
        print(f"Leaderboard PnL (MONTH): ${lb_month_pnl:,.0f}")
        print(f"Leaderboard PnL (ALL):   ${lb_all_pnl:,.0f}")
        print(f"Our Computed PnL:        ${computed_pnl:,.0f}")
        print(f"{'-' * 60}")
        print(
            f"Closed Positions: {len(closed)} ({len(wins)} wins, {len(losses)} losses) [after dedup]"
        )
        if recent_closed:
            print(
                f"  Recent ({RECENT_WINDOW_DAYS}d): {len(recent_closed)} positions "
                f"({len(recent_wins)} wins, {len(recent_closed)-len(recent_wins)} losses, "
                f"WR={recent_wr:.1%}, ROI={recent_roi:.1%})"
            )
        if older_closed:
            print(
                f"  Older:        {len(older_closed)} positions "
                f"({len(older_wins)} wins, {len(older_closed)-len(older_wins)} losses, "
                f"WR={older_wr:.1%}, ROI={older_roi:.1%})"
            )
        print(f"  Blended:      WR={blended_wr:.1%}, ROI={blended_roi:.1%}")
        freq = m.get("trades_per_day", len(recent_closed) / RECENT_WINDOW_DAYS)
        print(f"Frequency:            {freq:.1f} trades/day ({len(recent_closed)} trades in {RECENT_WINDOW_DAYS}d)")
        print(f"Efficiency (WR×ROI):  {m.get('efficiency', 0):.4f} (bonus: {t.get('efficiency_bonus', 1.0):.2f}x)")
        print(f"Consistency (Sharpe): {m['consistency']:.2f}")
        print(f"Recency:              {m['recency']:.2f} (traded {days:.1f} days ago)")
        print(f"{'-' * 60}")

        if biggest_win:
            print(
                f"Biggest Win:  ${float(biggest_win.get('realizedPnl', 0)):,.0f} "
                f"— \"{biggest_win.get('title', 'N/A')}\""
            )
        if biggest_loss:
            print(
                f"Biggest Loss: ${float(biggest_loss.get('realizedPnl', 0)):,.0f} "
                f"— \"{biggest_loss.get('title', 'N/A')}\""
            )

        # Top 5 markets by volume traded
        if closed:
            by_vol = sorted(
                closed,
                key=lambda p: float(p.get("totalBought", 0)),
                reverse=True,
            )
            print(f"{'-' * 60}")
            print("Top Markets by Volume:")
            for j, p in enumerate(by_vol[:5], 1):
                vol = float(p.get("totalBought", 0))
                title = p.get("title", "N/A")
                print(f"  {j}. ${vol:,.0f} — {title}")

        print(f"{'=' * 60}")


# ============================================================
# Step 7: Full scoring pipeline
# ============================================================

def score_all_traders(follow_top_n: int = 10):
    """
    Main pipeline:
    1. Discover traders from leaderboard
    2. Apply cheap pre-filters (pnl > 0, volume > $1000) — no extra API calls
    3. Enrich each with position/trade data
    4. Apply post-filters: positions value > $0, health ratio, min closed,
       recent win rate >= 90%, inactive
    5. Per-trader MONTH/ALL leaderboard lookup (only survivors from step 4)
    6. Boot if MONTH PnL < 0 or ALL PnL < 0
    7. Compute blended metrics and composite score
    8. Apply health penalty multiplier
    9. Upsert to database
    10. Update follow list
    11. Print validation report for top 10
    """
    print("=" * 60)
    print("[SCORER] Starting full trader scoring pipeline")
    print("=" * 60)

    # Step 1: Discover
    discovered = discover_traders()
    print(f"\n[SCORER] Discovered {len(discovered)} unique traders")

    # Steps 2-6: Filter, enrich, and compute metrics
    scored_traders = []
    skipped = {
        "pnl": 0, "vol": 0, "no_value": 0, "health": 0,
        "positions": 0, "frequency": 0, "win_rate": 0, "roi": 0,
        "recency": 0, "lb_month": 0, "lb_all": 0,
    }

    for i, (wallet, leaderboard_data) in enumerate(discovered.items()):
        if (i + 1) % 25 == 0:
            print(f"[SCORER] Processing trader {i+1}/{len(discovered)}...")

        total_volume = float(leaderboard_data.get("vol", 0))
        total_pnl = float(leaderboard_data.get("pnl", 0))

        # Step 2: Cheap pre-filters (no API calls)
        if REQUIRE_POSITIVE_PNL and total_pnl <= 0:
            skipped["pnl"] += 1
            continue
        if total_volume < MIN_VOLUME_USD:
            skipped["vol"] += 1
            continue

        # Step 3: Enrich with detailed data (4-5 API calls)
        enrichment = enrich_trader(wallet)
        if not enrichment:
            continue

        # Step 4a: Boot if no live positions
        if enrichment["positions_value"] <= MIN_POSITIONS_VALUE:
            skipped["no_value"] += 1
            continue

        # Step 4b: Boot if health ratio too bad (losing >50% of deployed capital)
        if enrichment["health_ratio"] < HEALTH_HARD_FILTER:
            skipped["health"] += 1
            continue

        closed = enrichment["closed_positions"]

        # Step 4c: Minimum closed positions
        if len(closed) < MIN_RESOLVED_POSITIONS:
            skipped["positions"] += 1
            continue

        # Step 4d: Frequency + recent win rate
        recent_closed, older_closed = split_by_recency(closed, RECENT_WINDOW_DAYS)

        # Frequency check: must average at least MIN_RECENT_TRADES_PER_DAY in last 30 days
        trades_per_day = len(recent_closed) / RECENT_WINDOW_DAYS
        if trades_per_day < MIN_RECENT_TRADES_PER_DAY:
            skipped["frequency"] += 1
            continue

        # Recent win rate check (>= 30 positions guaranteed by frequency filter above)
        recent_wr_check = compute_win_rate(recent_closed)
        if recent_wr_check < MIN_RECENT_WIN_RATE:
            skipped["win_rate"] += 1
            continue

        # Step 4e: Must have traded within last 7 days
        recency_score, days_since = compute_recency(enrichment["trades"])
        if days_since > MAX_DAYS_SINCE_LAST_TRADE:
            skipped["recency"] += 1
            continue

        # Step 5-6: Per-trader MONTH/ALL leaderboard (only ~50-100 traders reach here)
        lb_month = client.get_leaderboard_for_user(wallet, "MONTH")
        lb_all = client.get_leaderboard_for_user(wallet, "ALL")

        month_pnl = float(lb_month.get("pnl", 0)) if lb_month else total_pnl
        all_pnl = float(lb_all.get("pnl", 0)) if lb_all else total_pnl

        if all_pnl < 0:
            skipped["lb_all"] += 1
            continue
        if month_pnl < 0:
            skipped["lb_month"] += 1
            continue

        # Step 7: Blend metrics across recent vs older windows
        recent_roi = compute_roi(recent_closed, total_volume) if recent_closed else 0
        older_roi = compute_roi(older_closed, total_volume) if older_closed else 0
        if recent_closed and older_closed:
            blended_roi = RECENT_WEIGHT * recent_roi + OLDER_WEIGHT * older_roi
        elif recent_closed:
            blended_roi = recent_roi
        else:
            blended_roi = older_roi

        recent_wr = compute_win_rate(recent_closed) if recent_closed else 0
        older_wr = compute_win_rate(older_closed) if older_closed else 0
        if recent_closed and older_closed:
            blended_wr = RECENT_WEIGHT * recent_wr + OLDER_WEIGHT * older_wr
        elif recent_closed:
            blended_wr = recent_wr
        else:
            blended_wr = older_wr

        recent_con = compute_consistency(recent_closed) if len(recent_closed) >= 5 else 0
        older_con = compute_consistency(older_closed) if len(older_closed) >= 5 else 0
        if recent_con > 0 and older_con > 0:
            blended_con = RECENT_WEIGHT * recent_con + OLDER_WEIGHT * older_con
        elif recent_con > 0:
            blended_con = recent_con
        elif older_con > 0:
            blended_con = older_con
        else:
            blended_con = 0

        # ROI floor: boot traders making pennies per trade
        if blended_roi < MIN_ROI:
            skipped["roi"] += 1
            continue

        # Efficiency = excess win rate above baseline × ROI
        raw_efficiency = (blended_wr - EFFICIENCY_WR_BASELINE) * blended_roi

        metrics = {
            "roi": blended_roi,
            "win_rate": blended_wr,
            "consistency": blended_con,
            "recency": recency_score,
            "efficiency": raw_efficiency,
            "trades_per_day": trades_per_day,
        }

        scored_traders.append({
            "wallet": wallet,
            "leaderboard": leaderboard_data,
            "enrichment": enrichment,
            "metrics": metrics,
            "total_pnl": total_pnl,
            "total_volume": total_volume,
            "days_since_trade": days_since,
            "month_pnl": month_pnl,
            "all_pnl": all_pnl,
        })

    print(f"\n[SCORER] {len(scored_traders)} traders passed all filters")
    print(
        f"  Skipped: {skipped['pnl']} neg-PnL, {skipped['vol']} low-vol, "
        f"{skipped['no_value']} no-positions, {skipped['health']} bad-health, "
        f"{skipped['positions']} few-closed, {skipped['frequency']} low-freq, "
        f"{skipped['win_rate']} low-WR, {skipped['roi']} low-ROI, "
        f"{skipped['recency']} inactive, {skipped['lb_month']} neg-month, "
        f"{skipped['lb_all']} neg-alltime"
    )

    if not scored_traders:
        print("[SCORER] No traders to score. Done.")
        return

    # Compute normalization ranges across the cohort
    normalization_ranges = {}
    for metric in SCORING_WEIGHTS:
        values = [t["metrics"][metric] for t in scored_traders]
        normalization_ranges[metric] = (min(values), max(values))

    # Step 8: Compute composite scores and apply health penalty
    for t in scored_traders:
        t["composite_score"] = compute_composite_score(
            t["metrics"], normalization_ranges
        )
        health_ratio = t["enrichment"]["health_ratio"]
        if health_ratio < 0:
            multiplier = max(1.0 + (health_ratio * HEALTH_PENALTY_FACTOR), 0.5)
            t["health_multiplier"] = round(multiplier, 4)
            t["composite_score"] = round(t["composite_score"] * multiplier, 4)
        else:
            t["health_multiplier"] = 1.0

    # Normalize efficiency across all scored traders and apply bonus
    efficiency_values = [t["metrics"]["efficiency"] for t in scored_traders]
    eff_min = min(efficiency_values)
    eff_max = max(efficiency_values)
    for t in scored_traders:
        raw_eff = t["metrics"]["efficiency"]
        if eff_max > eff_min:
            norm_eff = (raw_eff - eff_min) / (eff_max - eff_min)
        else:
            norm_eff = 0.5
        efficiency_bonus = 1.0 + (EFFICIENCY_BONUS_MAX * norm_eff)
        t["efficiency_bonus"] = round(efficiency_bonus, 4)
        t["composite_score"] = round(t["composite_score"] * efficiency_bonus, 4)

    # Sort by composite score descending
    scored_traders.sort(key=lambda x: x["composite_score"], reverse=True)

    # Batch upsert to database (single connection, single transaction)
    now = datetime.now(timezone.utc)
    print(f"[SCORER] Writing {len(scored_traders)} traders to database...")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for i, t in enumerate(scored_traders):
                lb = t["leaderboard"]
                m = t["metrics"]

                cur.execute("""
                    INSERT INTO traders (
                        proxy_wallet, username, profile_image, x_username,
                        verified_badge, total_pnl, total_volume,
                        total_positions, active_positions,
                        roi_pct, win_rate, consistency_score,
                        recency_score, diversification, composite_score,
                        last_scored_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, NOW()
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
                """, (
                    t["wallet"], lb.get("userName", ""),
                    lb.get("profileImage", ""), lb.get("xUsername", ""),
                    lb.get("verifiedBadge", False),
                    t["total_pnl"], t["total_volume"],
                    len(t["enrichment"]["closed_positions"]) + t["enrichment"]["active_count"],
                    t["enrichment"]["active_count"],
                    round(m["roi"], 4), round(m["win_rate"], 4),
                    round(m["consistency"], 4), round(m["recency"], 4),
                    0, t["composite_score"],
                    now,
                ))

                cur.execute("""
                    INSERT INTO scoring_history
                    (proxy_wallet, composite_score, roi_pct, win_rate, consistency)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    t["wallet"], t["composite_score"],
                    round(m["roi"], 4), round(m["win_rate"], 4),
                    round(m["consistency"], 4),
                ))

                if (i + 1) % 200 == 0:
                    print(f"  → Written {i+1}/{len(scored_traders)}...")

        conn.commit()
        print(f"[SCORER] All {len(scored_traders)} traders written to database")
    finally:
        conn.close()

    # Update follow list
    update_follow_list(follow_top_n)

    # Print leaderboard summary
    print(f"\n{'='*60}")
    print(f"[SCORER] Scoring complete — Top {min(10, len(scored_traders))} traders:")
    print(f"{'='*60}")
    for i, t in enumerate(scored_traders[:10]):
        lb = t["leaderboard"]
        m = t["metrics"]
        name = lb.get("userName", t["wallet"][:10])
        print(
            f"  #{i+1} {name:<20} "
            f"Score={t['composite_score']:.3f}  "
            f"ROI={m['roi']:.1%}  "
            f"WR={m['win_rate']:.1%}  "
            f"Sharpe={m['consistency']:.2f}  "
            f"Eff={m['efficiency']:.4f}  "
            f"PnL=${t['total_pnl']:,.0f}"
        )

    # Detailed validation report
    validate_top_traders(scored_traders, n=10)

    return scored_traders


# ============================================================
# CLI entry point
# ============================================================

if __name__ == "__main__":
    import sys
    from db import init_db

    print("[SCORER] Initializing database...")
    init_db()

    top_n = int(sys.argv[1]) if len(sys.argv) > 1 else 10
    score_all_traders(follow_top_n=top_n)
