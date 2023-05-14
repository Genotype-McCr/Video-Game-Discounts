# =============================================================================
# Adds onto the database through replacing or by appending, df to db.
# =============================================================================
# Adds the columns of lowest concurrent price and its percentage discount.
# This is in addition to the column which states the discount of each price.
# =============================================================================
#  Takes four minutes to complete at (670215, 14) concatenated shape.
#  Must restart kernel after running to modify the file outside Python.
# =============================================================================

import sqlite3
from time import time
import pandas as pd
from sqlalchemy import create_engine


start_time = round(time())

conn = sqlite3.connect('game-price_db.sqlite', timeout=10)
cur = conn.cursor()

df = pd.read_sql_query('''
                        SELECT df.y, s.keystore, df.game_id, g.original_price
                        FROM dataframe as df
                        JOIN shop as s
                        ON s.id = df.shop_id
                        JOIN game as g
                        on g.id = df.game_id
                        ;
    
                        ''', conn)

keystore = None
low_price0 = None
low_price1 = None
name = None
x = 1
try:
    df.insert(0, 'low_price', None)
except:
    pass
for i, row in df.iterrows():
    if not row['game_id'] == name:
        keystore = row['keystore']
        low_price0 = 999
        low_price1 = 999
    if row['keystore'] == 1:
        low_price1 = row['y']
        if keystore == row['keystore']:
            df.loc[i,'low_price'] = low_price1
            price = low_price1
        keystore = row['keystore']
        if low_price1 < low_price0:
            df.loc[i,'low_price'] = low_price1
            price = low_price1
        else:
            df.loc[i,'low_price'] = low_price0
            price = low_price0
    else:
        low_price0 = row['y']
        if keystore == row['keystore']:
            df.loc[i,'low_price'] = low_price0
            price = low_price0
        keystore = row['keystore']
        if low_price0 < low_price1:
            df.loc[i,'low_price'] = low_price0
            price = low_price0
        else:
            df.loc[i,'low_price'] = low_price1
            price = low_price1
    orig_price = row['original_price']
    if price > orig_price:
        df.loc[i,'low_price'] = orig_price
    name = row['game_id']
    x += 1
    if x % 10000 == 0:
        print(x, '\n', row)

df['percent_discount'] = (df['original_price'] - df['low_price'])*100 / df['original_price']

df_vals = df[['low_price','percent_discount']]
df = pd.read_sql_query('SELECT * FROM dataframe', conn)
df2 = pd.concat([df, df_vals], axis=1)
conn.close

engine = create_engine('sqlite:///game-price_db.sqlite', echo=False)
df2.to_sql(name='dataframe', con=engine, if_exists='replace', index=False)
engine.dispose()


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
