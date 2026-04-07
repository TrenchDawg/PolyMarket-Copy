from db import get_conn
conn = get_conn()
cur = conn.cursor()
cur.execute('SELECT COUNT(*), COUNT(DISTINCT asset_id) FROM followed_positions WHERE proxy_wallet LIKE chr(37) || chr(54) || chr(54) || chr(102) || chr(98) || chr(37)')
total, unique = cur.fetchone()
print('Total rows:', total)
print('Unique asset_ids:', unique)
print('Duplicates:', total - unique)
conn.close()
