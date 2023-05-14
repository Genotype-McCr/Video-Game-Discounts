import pandas as pd
import requests
from bs4 import BeautifulSoup as bs
import re
from time import strptime
from datetime import datetime, date
from time import time
import calendar
import numpy as np
import sqlite3
Overrided_prices = {}
Games__prices = {}
free_or_missing = []
No_Scheduled_Release = []
Upcoming_Release = []
url_list = []
url_scraped = []
missing_appid = []
start_time = round(time())

df_total = None
df_official = None
df_total = pd.DataFrame()

# =============================================================================
# Creation of database if it doesn't otherwise exist under the file name
# =============================================================================

conn = sqlite3.connect('game-price_db.sqlite', timeout=10)
cur = conn.cursor()

cur.executescript('''

CREATE TABLE IF NOT EXISTS shop (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE,
    keystore INTEGER
);

CREATE TABLE IF NOT EXISTS ratings_category (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name   TEXT UNIQUE
);


CREATE TABLE IF NOT EXISTS developer (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS publisher (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS game (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE,
    
    release_time INTEGER,
    release_date INTEGER,
    original_price INTEGER,
    o_price_type INTEGER,
    metacritic INTEGER,
    ATH_players INTEGER,
    steam_review_count INTEGER,
    steam_review_rating INTEGER,
    appid INTEGER,
    
    ratings_category_id INTEGER,
    developer_id INTEGER,
    publisher_id INTEGER
);

CREATE TABLE IF NOT EXISTS dataframe (
    id  INTEGER NOT NULL PRIMARY KEY 
        AUTOINCREMENT UNIQUE,
    
    x INTEGER,
    y INTEGER,
    time_from_release INTEGER,
    days_from_release INTEGER,
    startdate_utc INTEGER,
    enddate_utc INTEGER,
    price_duration_utc INTEGER,
    price_duration_days INTEGER,
    price_discount INTEGER,
    
    game_id INTEGER,
    shop_id INTEGER
);

CREATE TABLE IF NOT EXISTS free_or_missing (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS no_scheduled_release (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS upcoming_release (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    name    TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS scrape_list (
    id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    link    TEXT UNIQUE,
    completed INTEGER
);

''')


#---------------------------------------------------------------------------
# User input: How many games to count on this run of the program?
#---------------------------------------------------------------------------

games_counted = 0
games_to_count = input('Enter desired number of games: ')
if games_to_count == '':
    games_to_count = 500
else:
    try:
        games_to_count = int(games_to_count)
    except:
        games_to_count = 0
        print('***INVALD NUMBER***')
default = 0
# for each of the quantified number of games ...
while games_to_count >= 1:
    games_to_count -= 1
    games_counted += 1
    # Print every so often the duration and completions of the current run.
    if (games_counted > 0) & (games_counted % 10 == 0) & (games_to_count != 0):
        print("----------")
        print("Running Batch Count:", games_counted)
        print("----------\n")
    if (games_counted > 0) & (games_counted % 50 == 0) & (games_to_count != 0):
        print("------------------------------")
        print("Has been running for", (round(time()) - start_time) // 60, "minute(s),", (round(time()) - start_time) % 60, 'second(s).')
        print("------------------------------\n")
    
    try:
        try:
            # if there are links stored in the database already ...
            cur.execute('SELECT link FROM scrape_list WHERE completed = ? ', (1, ))
            url_completed = cur.fetchone()[0]
            cur.execute('SELECT link FROM scrape_list WHERE completed = ? ', (0, ))
            url_piece = cur.fetchone()[0]
            
            if (url_piece == None) & (len(url_completed) >= 1) & (default == 1):
                conn.close()
                games_counted -= 1
                print('No unscraped links left in collection!')
                break
            url = 'https://gg.deals/game' + url_piece
            print(url)
        except:
            # if not and the default has been performed already,
            # grab links off the /deals/ page and add them to the database
            try:
                if (url_piece == None) & (len(url_completed) >= 1):
                    url = 'https://gg.deals/deals/'
                    r = requests.get(url)
                    soup = bs(r.text, 'lxml')
                    scrape_list = soup.find_all("a", {"class":"main-image"})
                    for row in scrape_list:
                        row = row.get('href')
                        if re.search(r'(/game/)', row):
                            if not re.search(row, url):
                                url_piece_add = re.findall(r'/game(.*)', row)[0]
                                cur.execute('''INSERT OR IGNORE INTO scrape_list (link, completed) 
                                VALUES ( ?, ? )''', ( url_piece_add, 0) )
                continue
            # The default link, can manually change the starting point of a new database
            except:  
                url = 'https://gg.deals/game/hogwarts-legacy/'
                print("No unscraped links in collection!\n** Using default!\n")
                print(url)
                default = 1

        regex = r'game\/\b(.*)\/'
        title = re.findall(regex, url)
        title = ' '.join(word.capitalize() for word in title[0].split('-'))

        r = requests.get(url)
        soup = bs(r.text, 'lxml')
        scrape_list = soup.find_all("a", {"class":"main-image"})
        
# Source for dataURL from:
# https://stackoverflow.com/questions/70506477/scrape-data-from-graph-generated-with-highcharts
        # Toggle unsupported keystores: data-with-keyshops-url or data-without-keyshops-url
        dataUrl = 'https://gg.deals'+soup.select_one('#historical-chart-container')['data-with-keyshops-url']
        r  = requests.get(dataUrl, headers={'X-Requested-With': 'XMLHttpRequest'})
    except:
        # If no price data, insert into database table and update scrape list.
        regex = r'game\/\b(.*)\/'
        title = re.findall(regex, url)
        title = ' '.join(word.capitalize() for word in title[0].split('-'))
        No_Scheduled_Release.append(title)
        url_piece = re.findall(r'game(.*)', url)[0]
        url_list.append(url_piece)
        url_scraped.append(url_piece)
        Games__prices[title] = 'No Scheduled Release'
        cur.execute('''INSERT OR IGNORE INTO no_scheduled_release (name) 
                    VALUES ( ? )''', ( title, ) )
        cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
        VALUES ( ?, ? )''', ( url_piece, 1) )
        print('No Scheduled Release \n')
        continue
    
    try:
        # For each of the links on the page, if the regex expression is found and is not
        # a link to the current page itself, add to links to scrape.
        for row in scrape_list:
            row = row.get('href')
            if re.search(r'(/game/)', row):
                if not re.search(row, url):
                    url_piece_add = re.findall(r'/game(.*)', row)[0]
                    cur.execute('''INSERT OR IGNORE INTO scrape_list (link, completed) 
                    VALUES ( ?, ? )''', ( url_piece_add, 0) )
    except: pass
    
    url_piece = re.findall(r'game(.*)', url)[0]

    chartdata = r.json()['chartData']
    
    df_official = pd.DataFrame.from_dict(data = chartdata['retail'])
    df_keystores = pd.DataFrame.from_dict(data = chartdata['keyshops'])
    
    
    df_official['Keystore'] = 0
    df_keystores['Keystore'] = 1
    
    try:
        len(df_official['x'])
    except:
        cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
                    VALUES ( ?, ? )''', ( url_piece, 1) )
        Games__prices[title] = 'Free'
        cur.execute('''INSERT OR IGNORE INTO free_or_missing (name) 
                    VALUES ( ? )''', ( title, ) )
        print('Missing/Free \n')
        continue
    
    #Preparing Keystore df
    df_keystores['name2'] = df_keystores['name'].shift(-1)
    def dates(row):
        if len(str(row['name2']).split(" ")) == 4:
            row['name'] = str(row['name']) + ' 00:00 - ' + str(row['name2'])
        if (len(str(row['name']).split(" ")) != 4) & (len(str(row['name2']).split(" ")) == 3):
            row['name'] = str(row['name']) + ' 00:00 - ' + str(row['name2']) + ' 00:00'
    #    if len(str(row['name']).split(" ")) == 4:
    #        row['name'] = str(row['name']) + ' - now'
        return row
    
    df_keystores = df_keystores.apply(dates, axis = 'columns')
    
    del df_keystores['name2']
    
    
# 'x' is timestamp in UTC with three trailing zeros
# Dividing this variable by 1000 gives a number interpretable by the 'datetime' library
    
    df_official.x = df_official.x.apply(lambda x: x/1000)
    df_keystores.x = df_keystores.x.apply(lambda x: x/1000)

    frames = [df_official, df_keystores]
    result = pd.concat(frames, ignore_index=True)
    
    if max(result['y'].dropna()) == 0:
        url_list.append(url)
        url_scraped.append(url)
        free_or_missing.append(title)
        Games__prices[title] = 'Free'
        cur.execute('''INSERT OR IGNORE INTO free_or_missing (name) 
                    VALUES ( ? )''', ( title, ) )
        cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
        VALUES ( ?, ? )''', ( url_piece, 1) )
        print("Missing/Free \n")
        continue

    df_official = result.sort_values("x")
    
    ## Adding game name column for when adding multiple games to the database
 
    title = str(soup.find_all("a", class_="game-info-title title no-icons"))
    regex = r'data-title-auto-hide=\"(.*?)[?=\"]'
    try:
        title = re.findall(regex, title)
        title = title[0]
    except:
        regex = r'game\/\b(.*)\/'
        title = re.findall(regex, url)
        title = ' '.join(word.capitalize() for word in title[0].split('-'))
        cur.execute('''INSERT OR IGNORE INTO free_or_missing (name) 
                    VALUES ( ? )''', ( title, ) )
        cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
        VALUES ( ?, ? )''', ( url_piece, 1) )
        print(title)
        print('Missing/Free \n')
        continue
    print(title)

    df_official['Game'] = title
    
    
    try:
        jar = soup.find_all("a", class_="score-grade")[0]
        appid = jar.get('href')
        appid = re.findall(r'(\d+)', appid)[0]
    
        Ratings_Category = jar.get_text()
        df_official['Rating_Category'] = re.findall(r'(.*)(?= \()', Ratings_Category)[0]
    
        jar2 = soup.find_all("span", class_="reviews-label")
        steam_ratings = jar2[0].get("title")
        steam_review_rating, steam_review_count = re.findall(r'(\d+).*?(\d*,\d*)', steam_ratings)[0]
        steam_review_rating = int(steam_review_rating)/100
        steam_review_count = int(''.join(steam_review_count.split(',')))
    
        del(jar)
        del(jar2)
        del(steam_ratings)
        
# =============================================================================
#         Grabbing Steam info
# =============================================================================

        url2 = "https://steamcharts.com/app/" + appid
        r = requests.get(url2, headers={'X-Requested-With': 'XMLHttpRequest'})
        soup2 = bs(r.content, 'html.parser')
    
        ATH_players = int(soup2.find_all("span", class_="num")[2].string)
    
        print("All time High:", ATH_players, '\n')
    
    except:
        print("**No Steam ID. Upcoming Release?** \n")
        appid = None
        ATH_players = None
        steam_review_count = None
        steam_review_rating = None
        missing_appid.append(title)
        cur.execute('''INSERT OR IGNORE INTO upcoming_release (name) 
                    VALUES ( ? )''', ( title, ) )
        cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
        VALUES ( ?, ? )''', ( url_piece, 1) )
        continue

    
    
    def splitname(row):
        try:
            row['startdate'] = row['name'].split(" - ")[0]
            row['enddate'] = row['name'].split(" - ")[-1]
        except:
            row['startdate'] = None
            row['enddate'] = None
        return row
        
    df_official = df_official.apply(splitname, axis = 'columns')
    
    df_official = df_official[~df_official['y'].isna()].reset_index()
    
    
    # Release date in epoch time & calendar date, utc()[0] and utc()[1] respectively.
    
    def utc_release():
        time = soup.find_all("p", class_="game-info-details-content")[0].string
        str(time)
        time = time.split(" ")
        try:
            d = int(time[0])
            m = time[1]
            m = strptime(m,'%b').tm_mon
            y = int(time[2])
        
            d = date(y, m, d)
            release_date_et = calendar.timegm(d.timetuple())
            release_date = str(datetime.utcfromtimestamp(release_date_et)).split(" ")[0]
        except:
            try:
                m = time[0]
                m = strptime(m,'%b').tm_mon
                y = int(time[1])
            
                d = date(y, m, 15)
                release_date_et = calendar.timegm(d.timetuple())
                release_date = str(datetime.utcfromtimestamp(release_date_et)).split(" ")[0]
            except:
                return
        return release_date_et, release_date 
    try:
        df_official['Release Time'] = utc_release()[0]
        df_official['Release Date'] = utc_release()[1]
        df_official['Time From Release'] = df_official['x'] - df_official['Release Time']
        df_official['Days From Release'] = df_official['Time From Release'] // (24 * 3600)
    except:
        df_official['Release Time'] = (df_official['x'])[int((len(df_official['x'])/2))]
        df_official['Release Date'] = None
        df_official['Time From Release'] = None
        df_official['Days From Release'] = None
    
    prices = soup.find_all("span", class_="price-label price-old")
    
    try:
        if max(df_official['Time From Release']) < 0:
            Upcoming_Release.append(title)
    except: pass
    
    #---
    
    
    # Timestamps start and end dates of deals,
    # does not convert timezone (4 or 5 hour difference)
    # does not add the time of day to the total
    
    
    for i, row in df_official.iterrows():
        if (row['enddate'] == 'now') or (row['enddate'] == row['startdate']):
            df_official.at[i,'enddate'] = 'now'
    
    
    def utc_deals(row):
        try:
            d, m, y, i = row['startdate'].split(' ')
        except:
            try:
                d, m, y = row['startdate'].split(' ')
            except: pass
        try:
            m = strptime(m,'%b').tm_mon
            d = date(int(y), m, int(d))
            release_date_et = calendar.timegm(d.timetuple())
            row['startdate_utc'] = release_date_et
          
            row['enddate_utc'] = None
            if not row['enddate'] == 'now':
                d, m, y, i = row['enddate'].split(' ')
                m = strptime(m,'%b').tm_mon
                d = date(int(y), m, int(d))
                release_date_et = calendar.timegm(d.timetuple())
                row['enddate_utc'] = release_date_et
            if row['enddate'] == 'now':
                row['enddate_utc'] = round(time())
        except:
            pass
        return row
    
    df_official = df_official.apply(utc_deals, axis = 'columns')
    
    df_official['price_duration_utc'] = df_official['enddate_utc'] - df_official['startdate_utc']
    df_official['price_duration_days'] = df_official['price_duration_utc'] // (24 * 3600)
    
    
# =============================================================================
# For uncovering the orginial price of games, there is a multi-layered approach
# Where game prices aren't in USD on the steam page or no steam page is given,
# Gives the layer tier in the database as o_price_type for reference
# =============================================================================
    
    
    official_stores = ['Ubisoft Store', 'Epic Games Store', 'Steam', 'Amazon.com',
                       'Battle.net', 'Microsoft Store', 'Rockstar Store', 'Gog.com']
    
    try:
        df_official['Original price'] = float(str(prices[0].string)[1:])
        df_official['O_price_type'] = 0
    except:
        try:
            df_official['Original price'] = max(df_official.loc\
            [df_official.shop.isin(official_stores), 'y'].tolist()[0:16])
            df_official['O_price_type'] = 0
        except:
            df_official['Original price'] = None
            df_official['O_price_type'] = np.nan
    
    largest = None
    for x in prices[:-1]:
        pattern = '\d+\.\d+'
        result = re.findall(pattern, x.string)
        x = float(result[0])
        if x > 59.99: continue
        if largest == None: largest = x
        if x > 59.99: continue
        if (x >= largest) & (str(x)[-2:] == '99'):
            df_official['Original price'] = x
            df_official['O_price_type'] = 1
            if x > largest: largest = x
    
    
    #----------------------------------------------------------------------
    # MOST COMMON PRICE APPROACH, IS 59.99 IN FIRST 5 VALUES?
    
    num = 0
    for i in df_official[df_official.Keystore == 0][0:5]['y']:
        if (i == 59.99) & (str(df_official[df_official.Keystore == 0][num:num+1]['shop'].values[0]) in official_stores):
    #        print('yes')
            df_official['Original price'] = 59.99
            df_official['O_price_type'] = 2
    #    else:
    #        print('no', i, str(df_official[num:num+1]['shop'].values[0]))
        num += 1
    
    #----------------------------------------------------------------------
    # MODE APPROACH
    
    List = df_official.loc[df_official.shop.isin(official_stores), 'y'].tolist()[0:12]
    
    def check_score(number):
        if round(number % 1, 2) == .99:
            return True
        return False
    
    percentage_score = filter(check_score, List)
    List2 = list(percentage_score)
    
    def most_frequent(List):
        if max(set(List), key = List.count) == 1:
            return("No mode")
        else:
            return max(set(List), key = List.count)
    
    #----------------------------------------------------------------------
    #MAX APPROACH
    try:
        max_val = max(df_official.loc[df_official.shop.isin(official_stores), 'y'].tolist()[0:16])
    except:
        pass
    
    #----------------------------------------------------------------------
    #UTILIZED APPROACH, COMBINATION OF MAX & MODE
    
    try:
        if (len(List2) >= 5) & (most_frequent(List2) == max_val):
            df_official['Original price'] = max_val
            df_official['O_price_type'] = 2
    except:
        pass
    
    #----------------------------------------------------------------------
    #UTILIZED APPROACH, COMBINATION OF MAX & MODE
    
    
    df_prices = df_official.loc[(df_official.price_duration_days >= 7) & (df_official.shop.isin(official_stores)) & (df_official['Days From Release'] <= 3) & (df_official['y'].astype(str).str.endswith('.99'))]
    if len(df_prices) == 0:
        Games__prices[df_official['Game'][0]] = df_official['Original price'][0]
    if len(df_prices) == 1:
        Overrided_prices[df_official['Game'][0]] = str(df_official['Original price'][0])
        df_official['Original price'] = df_prices['y'].values[0]
        df_official['O_price_type'] = 3
        Games__prices[df_official['Game'][0]] = df_official['Original price'][0]
    if len(df_prices) > 1:
        Overrided_prices[df_official['Game'][0]] = str(df_official['Original price'][0]) + ' Multiple'
        df_official['Original price'] = df_prices.groupby(by='y')['price_duration_days'].sum().index[0]
        df_official['O_price_type'] = 4
        Games__prices[df_official['Game'][0]] = df_official['Original price'][0]
    
    
    #----------------------------------------------------------------------
    # Original Price from Steam Request
    # stripped down from https://nik-davis.github.io/posts/2019/steam-data-collection/
    
    if appid != None:
        def steam_request(appid):
            url = "http://store.steampowered.com/api/appdetails/"
            parameters = {"appids": appid}    
            try:
                data = requests.get(url, parameters).json()[str(appid)]['data']
                return data
            except:
                return
    
        steaminfo = steam_request(appid)
    
    # If currency is not USD, currency conversion != real initial price.
    # Steam charges by market and currency perhaps reflects that geographical market.
    # This is what appeared to be the case when I tested with Witcher 3.
        try:
            if steaminfo['price_overview']['currency'] == 'USD':
                df_official['Original price'] = float(steaminfo['price_overview']['initial'])/100
                df_official['O_price_type'] = 5
        except: pass
        try:
            df_official['Developer'] = steaminfo['developers'][0]
        except:
            df_official['Developer'] = None
        try:
            df_official['Publisher'] = steaminfo['publishers'][0]
        except:
            df_official['Publisher'] = None
        try:
            df_official['Metacritic'] = int(steaminfo['metacritic']['score']) / 100
        except:
            df_official['Metacritic'] = None
   
    #----------------------------------------------------------------------
    # Last resort price before dropping game from analysis
    
    if df_official['Original price'][0] == None or df_official['Original price'][0] == 'No mode':
        try:
            df_official['Original price'] = float(most_frequent(list(df_official['y'][0:12])))
            df_official['O_price_type'] = 6
        except:
            cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
                        VALUES ( ?, ? )''', ( url_piece, 1) )
            Games__prices[title] = 'Free'
            cur.execute('''INSERT OR IGNORE INTO free_or_missing (name) 
                        VALUES ( ? )''', ( title, ) )
            print('Missing/Free')
            continue
    round(float(1), 3)
    format(float(1), '.2f')
    prices

    #----------------------  
    
    if df_official['Original price'][0] == 0:
        cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
                    VALUES ( ?, ? )''', ( url_piece, 1) )
        Games__prices[title] = 'Free'
        cur.execute('''INSERT OR IGNORE INTO free_or_missing (name) 
                        VALUES ( ? )''', ( title, ) )
        continue
    
    df_official['Price Discount %'] = (df_official['Original price'] - df_official['y']) / df_official['Original price'] * 100
    
    df_official['appid'] = appid
    df_official['ATH_players'] = ATH_players
    df_official['steam_review_count'] = steam_review_count
    df_official['steam_review_rating'] = steam_review_rating

    df_official.drop('index',axis = 1, inplace = True)
    
# =============================================================================
#     Inserting values into database
# =============================================================================

    for shopname in df_official['shop'].unique():
        if shopname in df_keystores['shop'].unique():
            cur.execute('''INSERT OR IGNORE INTO shop (name, keystore) 
                        VALUES ( ?, ? )''', ( shopname, 1) ) 
        else:
            cur.execute('''INSERT OR IGNORE INTO shop (name, keystore) 
                        VALUES ( ?, ? )''', ( shopname, 0) ) 
    cur.execute('SELECT id FROM shop WHERE name = ? ', (df_official['shop'][0], ))
    shop_id = cur.fetchone()[0]
    
    cur.execute('''INSERT OR IGNORE INTO ratings_category (name) 
    VALUES ( ? )''', ( df_official['Rating_Category'][0], ) ) 
    cur.execute('SELECT id FROM ratings_category WHERE name = ? ', ( df_official['Rating_Category'][0], ))
    ratings_category_id = cur.fetchone()[0]


    cur.execute('''INSERT OR IGNORE INTO developer (name) 
    VALUES ( ? )''', ( df_official['Developer'][0], ) ) 
    cur.execute('SELECT id FROM developer WHERE name = ? ', (df_official['Developer'][0], ))
    try:
        developer_id = cur.fetchone()[0]
    except:
        developer_id = None
    cur.execute('''INSERT OR IGNORE INTO publisher (name) 
    VALUES ( ? )''', ( df_official['Publisher'][0], ) ) 
    cur.execute('SELECT id FROM publisher WHERE name = ? ', (df_official['Publisher'][0], ))
    try:
        publisher_id = cur.fetchone()[0]
    except:
        publisher_id = None
    
    cur.execute('''INSERT OR IGNORE INTO game (name, release_time, release_date,
                original_price, o_price_type, metacritic, ATH_players,
                steam_review_count, steam_review_rating, appid, ratings_category_id,
                developer_id, publisher_id) 
    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''',
    ( df_official['Game'][0], int(df_official['Release Time'][0]),
     df_official['Release Date'][0], df_official['Original price'][0],
     int(df_official['O_price_type'][0]), df_official['Metacritic'][0],
     int(df_official['ATH_players'][0]), int(df_official['steam_review_count'][0]),
     df_official['steam_review_rating'][0], appid, ratings_category_id,
     developer_id, publisher_id ) ) 
    
    cur.execute('SELECT id FROM game WHERE name = ? ',(df_official['Game'][0], ))
    game_id = cur.fetchone()[0]
    
    if not cur.execute('SELECT game_id FROM dataframe WHERE game_id = ? ', (game_id, )).fetchone():
        for index, row in df_official.iterrows():
            cur.execute('SELECT id FROM shop WHERE name = ? ', (row['shop'], ))
            shop_id = cur.fetchone()[0]
            
            cur.execute('''INSERT OR REPLACE INTO dataframe
                (x, y, time_from_release, days_from_release,
                 startdate_utc, enddate_utc, price_duration_utc, price_duration_days,
                 price_discount, game_id, shop_id ) 
                VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? )''', 
                ( row['x'], row['y'],
                 row['Time From Release'], row['Days From Release'],
                 int(row['startdate_utc']), int(row['enddate_utc']),
                 int(row['price_duration_utc']), int(row['price_duration_days']),
                 row['Price Discount %'],
                 game_id, shop_id ) )
    

    #----------------------
    #----------------------
    #----------------------

    df_total = pd.concat([df_total, df_official], axis=0)


    cur.execute('''INSERT OR REPLACE INTO scrape_list (link, completed) 
    VALUES ( ?, ? )''', ( url_piece, 1) )

    scrape_list = soup.find_all("a", {"class":"main-image"})
    for row in scrape_list:
        row = row.get('href')
        if re.search(r'(/game/)', row):
            if not re.search(row, url):
                url_piece_add = re.findall(r'/game(.*)', row)[0]
                cur.execute('''INSERT OR IGNORE INTO scrape_list (link, completed) 
                VALUES ( ?, ? )''', ( url_piece_add, 0) )

    url_list.append(url_piece)
    url_scraped.append(url_piece)
    
    conn.commit()

conn.close()

# =============================================================================
# Print run time duration and average time per game upload to the database
# =============================================================================

print('---------------')
print("Total Batch Count:", games_counted)

if round(time() - start_time) // 60 != 1:
    if (round(time()) - start_time) % 60 != 1:
        print("Ran for", (round(time()) - start_time) // 60, "minutes,", (round(time()) - start_time) % 60, 'seconds.')
        if games_counted > 1:
            print("Average time per game:", (round(((time()) - start_time) / games_counted)) // 60, "minutes,", (round(((time()) - start_time) / games_counted)) % 60, 'seconds.')
    else:
        print("Ran for", (round(time()) - start_time) // 60, "minutes,", (round(time()) - start_time) % 60, 'second.')
        if games_counted > 1:
            print("Average time per game:", (round(((time()) - start_time) / games_counted)) // 60, "minutes,", (round(((time()) - start_time) / games_counted)) % 60, 'second.')
else:
    if (round(time()) - start_time) % 60 != 1:
        print("Ran for", (round(time()) - start_time) // 60, "minute,", (round(time()) - start_time) % 60, 'seconds.')
        if games_counted > 1:
            print("Average time per game:", (round(((time()) - start_time) / games_counted)) // 60, "minute,", (round(((time()) - start_time) / games_counted)) % 60, 'seconds.')
    else:
        print("Ran for", (round(time()) - start_time) // 60, "minute,", (round(time()) - start_time) % 60, 'second.')
        if games_counted > 1:
            print("Average time per game:", (round(((time()) - start_time) / games_counted)) // 60, "minute,", (round(((time()) - start_time) / games_counted)) % 60, 'second.')



for _variable in ['chartdata', 'dataUrl', 'df_prices', 'frames', 'i',
                 'largest', 'List', 'num', 'official_stores', 'pattern', 'x', 'row',
                 'percentage_score', 'prices', 'r', 'regex', 'result', 'title', 'url',
                 'appid', 'ATH_players', 'games_to_count', 'developer_id', 'game_id',
                 'steam_review_count','steam_review_rating', 'url2', 'appid_id',
                 'List2', 'List2_mode', 'max_val', 'url_completed', 'Original_price', 'publisher_id',
                 'Ratings_Category', 'ratings_category_id', 'release_date_id',
                 'shop_id', 'steaminfo', 'url_piece', 'url_list', 'url_scraped',
                 'index', 'cur', 'scrape_list', 'df_total']:
    try:    
        exec(f'del {_variable}')   
    except:
        pass

if not free_or_missing: del(free_or_missing)
if not No_Scheduled_Release: del(No_Scheduled_Release)
if not Upcoming_Release: del(Upcoming_Release)
if not missing_appid: del(missing_appid)


