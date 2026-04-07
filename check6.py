from db import get_conn
conn = get_conn()
cur = conn.cursor()
cur.execute('SELECT asset_id[-10:], pre_existing, status, detected_at FROM followed_positions WHERE proxy_wallet LIKE chr(37) || chr(54) || chr(54) || chr(102) || chr(98) || chr(37) AND pre_existing = FALSE ORDER BY detected_at DESC LIMIT 20')
for r in cur.fetchall():
    print(r)
conn.close()
