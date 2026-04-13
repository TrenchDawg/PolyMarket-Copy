#!/usr/bin/env python3
"""
Polymarket Order Proxy — Madrid VPS
Receives order requests from Railway and forwards them to Polymarket's CLOB API.
This server runs in Spain (unblocked) so the CLOB geo-check passes.

Railway (monitoring) → POST /execute-order → This server → Polymarket CLOB API
"""

import os
import json
import hmac
import hashlib
from flask import Flask, request, jsonify

app = Flask(__name__)

# Auth token — Railway must send this in the Authorization header
# Generate one: python3 -c "import secrets; print(secrets.token_hex(32))"
PROXY_AUTH_TOKEN = os.getenv("PROXY_AUTH_TOKEN", "")

# Polymarket credentials
POLY_PRIVATE_KEY = os.getenv("POLY_PRIVATE_KEY", "")
POLY_API_KEY = os.getenv("POLY_API_KEY", "")
POLY_API_SECRET = os.getenv("POLY_API_SECRET", "")
POLY_API_PASSPHRASE = os.getenv("POLY_API_PASSPHRASE", "")

_clob_client = None


def get_clob_client():
    global _clob_client
    if _clob_client is not None:
        return _clob_client

    if not POLY_PRIVATE_KEY:
        print("[PROXY] No private key configured")
        return None

    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds

        creds = ApiCreds(
            api_key=POLY_API_KEY,
            api_secret=POLY_API_SECRET,
            api_passphrase=POLY_API_PASSPHRASE,
        )
        _clob_client = ClobClient(
            host="https://clob.polymarket.com",
            key=POLY_PRIVATE_KEY,
            chain_id=137,
            signature_type=1,
            creds=creds,
        )
        print("[PROXY] CLOB client initialized")
        return _clob_client
    except Exception as e:
        print(f"[PROXY] Failed to init CLOB client: {e}")
        return None


def verify_auth(req):
    """Verify the request came from Railway using the auth token."""
    if not PROXY_AUTH_TOKEN:
        print("[PROXY] WARNING: No PROXY_AUTH_TOKEN set — accepting all requests")
        return True
    token = req.headers.get("Authorization", "").replace("Bearer ", "")
    return hmac.compare_digest(token, PROXY_AUTH_TOKEN)


@app.route("/health", methods=["GET"])
def health():
    clob = get_clob_client()
    return jsonify({
        "status": "ok",
        "clob_ready": clob is not None,
        "location": "Madrid, ES",
    })


@app.route("/geocheck", methods=["GET"])
def geocheck():
    """Check if this server's IP is blocked by Polymarket."""
    import requests
    try:
        resp = requests.get("https://polymarket.com/api/geoblock", timeout=10)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/balance", methods=["GET"])
def get_balance():
    """Get USDC balance."""
    if not verify_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    clob = get_clob_client()
    if not clob:
        return jsonify({"error": "CLOB client not ready"}), 500

    try:
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        result = clob.get_balance_allowance(
            BalanceAllowanceParams(asset_type=AssetType.COLLATERAL, signature_type=1)
        )
        raw = int(result.get("balance", 0))
        balance_usd = raw / 1_000_000
        return jsonify({"balance_usd": balance_usd, "raw": raw})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/price", methods=["GET"])
def get_price():
    """Get current price for a token."""
    if not verify_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    token_id = request.args.get("token_id")
    side = request.args.get("side", "buy")

    if not token_id:
        return jsonify({"error": "token_id required"}), 400

    clob = get_clob_client()
    if not clob:
        return jsonify({"error": "CLOB client not ready"}), 500

    try:
        result = clob.get_price(token_id, side)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/execute-order", methods=["POST"])
def execute_order():
    """
    Place an order on the Polymarket CLOB.
    
    Request body:
    {
        "token_id": "123...",
        "side": "BUY" or "SELL",
        "size": 1.5,        # Number of shares
        "price": 0.85       # Price per share (limit order)
    }
    
    Response:
    {
        "success": true/false,
        "order_result": { ... full CLOB response ... },
        "error": "..." (if failed)
    }
    """
    if not verify_auth(request):
        return jsonify({"error": "unauthorized"}), 401

    clob = get_clob_client()
    if not clob:
        return jsonify({"error": "CLOB client not ready", "success": False}), 500

    try:
        data = request.get_json()
        if not data:
            return jsonify({"error": "no JSON body", "success": False}), 400

        token_id = data.get("token_id")
        side = data.get("side", "BUY").upper()
        size = float(data.get("size", 0))
        price = float(data.get("price", 0))

        if not token_id or size <= 0 or price <= 0:
            return jsonify({
                "error": f"invalid params: token_id={token_id}, size={size}, price={price}",
                "success": False,
            }), 400

        print(f"[PROXY] Placing {side} order: {size} shares @ ${price} for token {token_id[:30]}...")

        from py_clob_client.clob_types import OrderArgs
        result = clob.create_and_post_order(
            OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=side,
            )
        )

        print(f"[PROXY] Order result: {result}")

        return jsonify({
            "success": True,
            "order_result": result,
        })

    except Exception as e:
        print(f"[PROXY] Order failed: {e}")
        return jsonify({
            "success": False,
            "error": str(e),
        }), 500


if __name__ == "__main__":
    print("=" * 60)
    print("  Polymarket Order Proxy")
    print("  Location: Madrid, ES")
    print(f"  Auth: {'ENABLED' if PROXY_AUTH_TOKEN else 'DISABLED (set PROXY_AUTH_TOKEN!)'}")
    print(f"  CLOB: {'READY' if POLY_PRIVATE_KEY else 'NO KEY (set POLY_PRIVATE_KEY)'}")
    print("  Endpoints:")
    print("    GET  /health")
    print("    GET  /geocheck")
    print("    GET  /balance")
    print("    GET  /price?token_id=...&side=buy")
    print("    POST /execute-order")
    print("=" * 60)
    app.run(host="0.0.0.0", port=8080)
