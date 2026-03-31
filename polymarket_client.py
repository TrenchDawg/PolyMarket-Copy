"""
Polymarket Copy Trader — API Client
Handles all interactions with Polymarket's Data API, Gamma API, and CLOB API.
"""
import time
import requests
from typing import Optional
from config import (
    POLYMARKET_DATA_API,
    POLYMARKET_GAMMA_API,
    POLYMARKET_CLOB_API,
    LEADERBOARD_LIMIT,
    LEADERBOARD_MAX_PAGES,
)


class PolymarketClient:
    """Read-only client for Polymarket public APIs."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "Content-Type": "application/json",
            "Accept": "application/json",
        })

    def _get(self, url: str, params: dict = None, retries: int = 3) -> Optional[dict | list]:
        """GET with retry and rate-limit handling."""
        for attempt in range(retries):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                if resp.status_code == 429:
                    wait = int(resp.headers.get("Retry-After", 5))
                    print(f"[API] Rate limited, waiting {wait}s...")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.RequestException as e:
                print(f"[API] Request failed (attempt {attempt+1}/{retries}): {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)
        return None

    # ============================================================
    # Leaderboard
    # ============================================================

    def get_leaderboard(
        self,
        category: str = "OVERALL",
        time_period: str = "MONTH",
        order_by: str = "PNL",
        limit: int = LEADERBOARD_LIMIT,
        offset: int = 0,
    ) -> list:
        """Fetch leaderboard rankings."""
        data = self._get(
            f"{POLYMARKET_DATA_API}/v1/leaderboard",
            params={
                "category": category,
                "timePeriod": time_period,
                "orderBy": order_by,
                "limit": limit,
                "offset": offset,
            }
        )
        return data if isinstance(data, list) else []

    def get_full_leaderboard(
        self,
        category: str = "OVERALL",
        time_period: str = "MONTH",
        order_by: str = "PNL",
    ) -> list:
        """Paginate through full leaderboard for a category/period combo."""
        all_traders = []
        for page in range(LEADERBOARD_MAX_PAGES):
            offset = page * LEADERBOARD_LIMIT
            batch = self.get_leaderboard(
                category=category,
                time_period=time_period,
                order_by=order_by,
                limit=LEADERBOARD_LIMIT,
                offset=offset,
            )
            if not batch:
                break
            all_traders.extend(batch)
            if len(batch) < LEADERBOARD_LIMIT:
                break
            time.sleep(0.5)
        return all_traders

    # ============================================================
    # Trader profile & positions
    # ============================================================

    def get_positions(self, wallet: str, size_threshold: float = 10) -> list:
        """Get current open positions for a wallet."""
        data = self._get(
            f"{POLYMARKET_DATA_API}/positions",
            params={
                "user": wallet,
                "sizeThreshold": size_threshold,
                "sortBy": "CURRENT",
                "sortDirection": "DESC",
            }
        )
        return data if isinstance(data, list) else []

    def get_closed_positions(self, wallet: str, max_pages: int = 10) -> list:
        """Get resolved/closed positions for a wallet. Paginates up to max_pages * 50."""
        all_positions = []
        for page in range(max_pages):
            offset = page * 50
            data = self._get(
                f"{POLYMARKET_DATA_API}/closed-positions",
                params={
                    "user": wallet,
                    "limit": 50,
                    "offset": offset,
                    "sortBy": "REALIZEDPNL",
                    "sortDirection": "DESC",
                }
            )
            if not data or not isinstance(data, list):
                break
            all_positions.extend(data)
            if len(data) < 50:
                break
            time.sleep(0.3)
        return all_positions

    def get_trades(self, wallet: str, limit: int = 100) -> list:
        """Get trade history for a wallet."""
        data = self._get(
            f"{POLYMARKET_DATA_API}/trades",
            params={
                "user": wallet,
                "limit": limit,
                "sortDirection": "DESC",
            }
        )
        return data if isinstance(data, list) else []

    def get_activity(self, wallet: str, activity_type: str = "TRADE", limit: int = 200) -> list:
        """Get on-chain activity for a wallet."""
        data = self._get(
            f"{POLYMARKET_DATA_API}/activity",
            params={
                "user": wallet,
                "type": activity_type,
                "limit": limit,
                "sortDirection": "DESC",
            }
        )
        return data if isinstance(data, list) else []

    def get_portfolio_value(self, wallet: str) -> Optional[dict]:
        """Get total portfolio value for a wallet."""
        return self._get(
            f"{POLYMARKET_DATA_API}/value",
            params={"user": wallet}
        )

    def get_total_markets_traded(self, wallet: str) -> Optional[int]:
        """Get count of markets a user has traded."""
        data = self._get(
            f"{POLYMARKET_DATA_API}/traded",
            params={"user": wallet}
        )
        if isinstance(data, dict):
            return data.get("totalMarkets", 0)
        return 0

    # ============================================================
    # Market data
    # ============================================================

    def get_market(self, slug: str) -> Optional[dict]:
        """Get market details by slug."""
        return self._get(f"{POLYMARKET_GAMMA_API}/markets/{slug}")

    def get_order_book(self, token_id: str) -> Optional[dict]:
        """Get the order book for a token."""
        return self._get(
            f"{POLYMARKET_CLOB_API}/book",
            params={"token_id": token_id}
        )

    def get_market_price(self, token_id: str, side: str = "buy") -> Optional[dict]:
        """Get current price for a token."""
        return self._get(
            f"{POLYMARKET_CLOB_API}/price",
            params={"token_id": token_id, "side": side}
        )

    def get_spread(self, token_id: str) -> Optional[dict]:
        """Get bid-ask spread for a token."""
        return self._get(
            f"{POLYMARKET_CLOB_API}/spread",
            params={"token_id": token_id}
        )

    # ============================================================
    # Market holders
    # ============================================================

    def get_top_holders(self, condition_id: str) -> list:
        """Get top holders for a market."""
        data = self._get(
            f"{POLYMARKET_DATA_API}/holders",
            params={"conditionId": condition_id}
        )
        return data if isinstance(data, list) else []