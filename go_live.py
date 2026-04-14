from db import get_conn, release_conn
conn = get_conn()
cur = conn.cursor()
cur.execute("UPDATE system_config SET value = 'ON' WHERE key = 'kill_switch'")
conn.commit()
print('LIVE TRADING ENABLED')
release_conn(conn)
