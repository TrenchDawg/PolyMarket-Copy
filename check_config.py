from db import get_conn, release_conn
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT key, value FROM system_config")
for r in cur.fetchall():
    print(r)
release_conn(conn)
