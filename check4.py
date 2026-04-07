from db import get_conn
conn = get_conn()
cur = conn.cursor()
cur.execute('SELECT username, proxy_wallet, composite_score, is_followed FROM traders WHERE is_followed = TRUE ORDER BY composite_score DESC')
for r in cur.fetchall():
    print(r)
conn.close()
