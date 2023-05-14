# =============================================================================
# Adds onto the database through iteratively updating column values by row.
# =============================================================================
# Adds the columns of lowest concurrent price and its percentage discount.
# This is in addition to the column which states the discount of each price.
# =============================================================================
#  Takes seven seconds to complete at (670215, 14).
# =============================================================================

import sqlite3
from time import time


start_time = round(time())

conn = sqlite3.connect('game-price_db.sqlite', timeout=10)
cur = conn.cursor()

try:
    cur.executescript('''
                    ALTER TABLE dataframe
                      ADD low_price INTEGER
                    ;
                    ''')
    cur.executescript('''
                    ALTER TABLE dataframe
                      ADD percent_discount INTEGER
                    ;
                    ''')
except: pass

cur.execute('''
            SELECT df.y, s.keystore, df.game_id, df.low_price, df.percent_discount, g.original_price, df.id
            FROM dataframe as df
            JOIN shop as s
            ON s.id = df.shop_id
            JOIN game as g
            on g.id = df.game_id
            ;''') 

keystore = None
low_price0 = None
low_price1 = None
game_id = None
for row in cur: 
    if not row[2] == game_id:
        keystore = None
    if keystore == None:
        keystore = row[1]
        low_price0 = 999
        low_price1 = 999
    if row[1] == 1:
        low_price1 = row[0]                        
        if keystore == row[1]:
            conn.execute("""Update dataframe
                        set low_price = ?
                        WHERE id = ?""", (low_price1, row[6]))
        keystore = row[1]
        if low_price1 < low_price0:
            conn.execute("""Update dataframe
                        set low_price = ?
                        WHERE id = ?""", (low_price1, row[6]))
        else:
            conn.execute("""Update dataframe
                        set low_price = ?
                        WHERE id = ?""", (low_price0, row[6]))
    else:
        low_price0 = row[0]
        if keystore == row[1]:
            conn.execute("""Update dataframe
                        set low_price = ?
                        WHERE id = ?""", (low_price0, row[6]))
        keystore = row[1]
        if low_price0 < low_price1:
            conn.execute("""Update dataframe
                        set low_price = ?
                        WHERE id = ?""", (low_price0, row[6]))
        else:
            conn.execute("""Update dataframe
                        set low_price = ?
                        WHERE id = ?""", (low_price1, row[6]))
    game_id = row[2]
conn.commit()

cur.execute('''
            SELECT df.y, s.keystore, df.game_id, df.low_price, df.percent_discount, g.original_price, df.id
            FROM dataframe as df
            JOIN shop as s
            ON s.id = df.shop_id
            JOIN game as g
            on g.id = df.game_id
            ;''')

for row in cur:
    percent_discount = (row[5] - row[3])*100 / row[5]
    if row[3] > row[5]:
        conn.execute("""Update dataframe
                    set low_price = ?
                    WHERE id = ?""", (row[5], row[6]))
        percent_discount = 0
    conn.execute("""Update dataframe
                  set percent_discount = ?
                  WHERE id = ?""", (percent_discount, row[6]))
conn.commit()

conn.close()

if round(time() - start_time) // 60 != 1:
    if (round(time()) - start_time) % 60 != 1:
        print("Ran for", (round(time()) - start_time) // 60, "minutes,", (round(time()) - start_time) % 60, 'seconds.')
    else:
        print("Ran for", (round(time()) - start_time) // 60, "minutes,", (round(time()) - start_time) % 60, 'second.')
else:
    if (round(time()) - start_time) % 60 != 1:
        print("Ran for", (round(time()) - start_time) // 60, "minute,", (round(time()) - start_time) % 60, 'seconds.')
    else:
        print("Ran for", (round(time()) - start_time) // 60, "minute,", (round(time()) - start_time) % 60, 'second.')
