from db import get_conn
conn = get_conn()
cur = conn.cursor()
cur.execute('SELECT pre_existing, status, COUNT(*) FROM followed_positions GROUP BY pre_existing, status')
for r in cur.fetchall():
    print(r)
conn.close()
