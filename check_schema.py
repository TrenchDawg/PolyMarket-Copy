from db import get_conn, release_conn
conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'followed_positions' AND column_name = 'needs_order'")
print('needs_order column:', cur.fetchall())
cur.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'copy_trades' AND column_name = 'needs_sell'")
print('needs_sell column:', cur.fetchall())
release_conn(conn)
