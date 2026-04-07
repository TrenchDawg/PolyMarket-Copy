from db import get_conn
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT asset_id, pre_existing, detected_at FROM followed_positions WHERE proxy_wallet LIKE '0x66fb%' ORDER BY detected_at DESC LIMIT 5")
for r in cur.fetchall():
    print(r)
conn.close()
