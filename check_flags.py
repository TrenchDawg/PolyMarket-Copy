from db import get_conn, release_conn
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT fp.asset_id, t.username, fp.needs_order, fp.needs_order_attempts, fp.status, fp.detected_at FROM followed_positions fp JOIN traders t ON fp.proxy_wallet = t.proxy_wallet WHERE fp.needs_order = TRUE OR fp.needs_order_attempts > 0 ORDER BY fp.detected_at DESC LIMIT 10")
for r in cur.fetchall():
    print(r)
if not cur.rowcount:
    print('No positions with needs_order flag')
release_conn(conn)
