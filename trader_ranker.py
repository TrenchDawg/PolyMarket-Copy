"""
Polymarket Copy Trader — Trader Ranking Engine

Scores traders using a weighted composite of:
  ROI (30%) + Consistency (30%) + Win Rate (20%) +
  Volume (5%) + Recency (8%) + Diversification (7%)
"""
import math
import statistics
from datetime import datetime, timezone, timedelta
from typing import Optional
from polymarket_client import PolymarketClient
from config import (
    SCORING_WEIGHTS,
    MIN_RESOLVED_POSITIONS,
    MIN_VOLUME_USD,
    MAX_DAYS_SINCE_LAST_TRADE,
    REQUIRE_POSITIVE_PNL,
    LEADERBOARD_CATEGORIES,
    LEADERBOARD_TIME_PERIODS,
)
from db import upsert_trader, update_follow_list, get_conn


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
    Returns enrichment dict or None if trader doesn't meet filters.
    """
    # Get open positions
    positions = client.get_positions(wallet, size_threshold=0)
    active_count = len(positions) if positions else 0

    # Get closed positions (resolved markets)
    closed = client.get_closed_positions(wallet)
    if not closed:
        closed = []

    total_positions = active_count + len(closed)

    # Get trade history for recency check
    trades = client.get_trades(wallet, limit=50)

    # Get total markets traded
    total_markets = client.get_total_markets_traded(wallet)

    return {
        "positions": positions or [],
        "closed_positions": closed,
        "trades": trades or [],
        "active_count": active_count,
        "total_positions": total_positions,
        "total_markets": total_markets or 0,
    }


# ============================================================
# Step 3: Compute individual metrics
# ============================================================

def compute_roi(closed_positions: list, total_volume: float) -> float:
    """ROI = total realized PnL / total volume invested."""
    if total_volume <= 0:
        return 0.0
    total_pnl = sum(float(p.get("cashPnl", 0)) for p in closed_positions)
    return total_pnl / total_volume


def compute_win_rate(closed_positions: list) -> float:
    """Win rate = positions with positive PnL / total resolved."""
    if not closed_positions:
        return 0.0
    wins = sum(1 for p in closed_positions if float(p.get("cashPnl", 0)) > 0)
    return wins / len(closed_positions)


def compute_consistency(closed_positions: list) -> float:
    """
    Sharpe-like ratio: mean return / stdev of returns.
    Higher = more consistent profits. Capped at 3.0 for normalization.
    """
    if len(closed_positions) < 5:
        return 0.0

    returns = []
    for p in closed_positions:
        initial = float(p.get("initialValue", 0))
        pnl = float(p.get("cashPnl", 0))
        if initial > 0:
            returns.append(pnl / initial)

    if len(returns) < 5:
        return 0.0

    mean_ret = statistics.mean(returns)
    std_ret = statistics.stdev(returns)

    if std_ret == 0:
        return 3.0 if mean_ret > 0 else 0.0

    sharpe = mean_ret / std_ret
    return min(max(sharpe, 0), 3.0)  # clamp to [0, 3]


def compute_recency(trades: list) -> float:
    """
    Score based on how recently the trader was active.
    1.0 = traded today, decays toward 0 over MAX_DAYS_SINCE_LAST_TRADE days.
    """
    if not trades:
        return 0.0

    # Find most recent trade timestamp
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
        return 0.0

    days_ago = (datetime.now(timezone.utc) - latest).total_seconds() / 86400
    if days_ago > MAX_DAYS_SINCE_LAST_TRADE:
        return 0.0

    # Exponential decay
    return math.exp(-0.1 * days_ago)


def compute_diversification(total_markets: int, total_positions: int) -> float:
    """
    Diversification = distinct markets / total positions.
    Normalized: more markets = better, but diminishing returns.
    """
    if total_positions <= 0:
        return 0.0
    ratio = total_markets / total_positions
    # Log scale so 50 markets isn't 50x better than 1
    return min(math.log1p(total_markets) / math.log1p(100), 1.0)


# ============================================================
# Step 4: Normalize and compute composite score
# ============================================================

def normalize(value: float, min_val: float, max_val: float) -> float:
    """Min-max normalize to [0, 1]."""
    if max_val <= min_val:
        return 0.5
    return max(0, min(1, (value - min_val) / (max_val - min_val)))


def compute_composite_score(metrics: dict, normalization_ranges: dict) -> float:
    """
    Weighted composite score from normalized metrics.
    """
    score = 0.0
    for metric, weight in SCORING_WEIGHTS.items():
        raw = metrics.get(metric, 0)
        r = normalization_ranges.get(metric, (0, 1))
        normalized = normalize(raw, r[0], r[1])
        score += weight * normalized
    return round(score, 4)


# ============================================================
# Step 5: Full scoring pipeline
# ============================================================

def score_all_traders(follow_top_n: int = 10):
    """
    Main pipeline:
    1. Discover traders from leaderboard
    2. Enrich each with position/trade data
    3. Compute raw metrics
    4. Normalize across the cohort
    5. Compute composite scores
    6. Upsert to database
    7. Update follow list
    """
    print("=" * 60)
    print("[SCORER] Starting full trader scoring pipeline")
    print("=" * 60)

    # Step 1: Discover
    discovered = discover_traders()
    print(f"\n[SCORER] Discovered {len(discovered)} unique traders")

    # Step 2 & 3: Enrich and compute raw metrics
    scored_traders = []
    for i, (wallet, leaderboard_data) in enumerate(discovered.items()):
        if (i + 1) % 25 == 0:
            print(f"[SCORER] Enriching trader {i+1}/{len(discovered)}...")

        total_volume = float(leaderboard_data.get("vol", 0))
        total_pnl = float(leaderboard_data.get("pnl", 0))

        # Hard filters
        if REQUIRE_POSITIVE_PNL and total_pnl <= 0:
            continue
        if total_volume < MIN_VOLUME_USD:
            continue

        # Enrich with detailed data
        enrichment = enrich_trader(wallet)
        if not enrichment:
            continue

        closed = enrichment["closed_positions"]
        if len(closed) < MIN_RESOLVED_POSITIONS:
            continue

        # Compute raw metrics
        metrics = {
            "roi": compute_roi(closed, total_volume),
            "win_rate": compute_win_rate(closed),
            "consistency": compute_consistency(closed),
            "volume": total_volume,
            "recency": compute_recency(enrichment["trades"]),
            "diversification": compute_diversification(
                enrichment["total_markets"],
                enrichment["total_positions"]
            ),
        }

        scored_traders.append({
            "wallet": wallet,
            "leaderboard": leaderboard_data,
            "enrichment": enrichment,
            "metrics": metrics,
            "total_pnl": total_pnl,
            "total_volume": total_volume,
        })

    print(f"\n[SCORER] {len(scored_traders)} traders passed filters")

    if not scored_traders:
        print("[SCORER] No traders to score. Done.")
        return

    # Step 4: Compute normalization ranges across the cohort
    normalization_ranges = {}
    for metric in SCORING_WEIGHTS:
        values = [t["metrics"][metric] for t in scored_traders]
        normalization_ranges[metric] = (min(values), max(values))

    # Step 5: Compute composite scores
    for t in scored_traders:
        t["composite_score"] = compute_composite_score(
            t["metrics"], normalization_ranges
        )

    # Sort by composite score
    scored_traders.sort(key=lambda x: x["composite_score"], reverse=True)

    # Step 6: Batch upsert to database (single connection, single transaction)
    now = datetime.now(timezone.utc)
    print(f"[SCORER] Writing {len(scored_traders)} traders to database...")
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for i, t in enumerate(scored_traders):
                lb = t["leaderboard"]
                m = t["metrics"]

                # Upsert trader
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
                    t["enrichment"]["total_positions"],
                    t["enrichment"]["active_count"],
                    round(m["roi"], 4), round(m["win_rate"], 4),
                    round(m["consistency"], 4), round(m["recency"], 4),
                    round(m["diversification"], 4), t["composite_score"],
                    now,
                ))

                # Scoring history
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

    # Step 7: Update follow list
    update_follow_list(follow_top_n)

    # Print summary
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
            f"PnL=${t['total_pnl']:,.0f}"
        )

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
