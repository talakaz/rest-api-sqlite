import requests
import pandas as pd
import sqlite3
import time
from datetime import datetime
from datetime import timedelta

year = '2019'
binSize = '1d'
db = {'path': r'C:\Users\Talak\BitMEX_BackTest\SQLite3\HistoricalData.db', 'table': 'BitMEX' + '_' + year + '_' + binSize}
count = '750'
inserts = [0]


def get_last_date():
	conn = sqlite3.connect(db['path'])
	last_date = pd.read_sql('SELECT * FROM %s ORDER BY timestamp DESC LIMIT 1' % (db['table']), conn)['timestamp'][0]
	last_date = datetime.strptime(last_date, '%Y-%m-%d %H:%M:%S')
	time_dif = timedelta(minutes=1)
	next_date = last_date + time_dif
	return next_date


def pull_data(nextDate):
	nextDate = nextDate
	baseURL = "https://www.bitmex.com/api/v1/trade/bucketed?"
	params = {'binSize': binSize, 'symbol': 'XBTUSD', 'count': count, 'startTime': nextDate}
	rawData = requests.get(baseURL, params=params)
	rawData = rawData.json()
	df = pd.DataFrame(rawData)
	df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]
	df['timestamp'] = pd.to_datetime(df['timestamp'])
	df = df.set_index('timestamp')
	return df
	# df.to_sql(name='BitMEX_' + binSize[0], if_exists='replace', con=conn)


def update_data():
	inserts[0] += 750
	tableName = db['table']
	df = pull_data(get_last_date())
	conn = sqlite3.connect(db['path'])
	df.to_sql(name=tableName, if_exists='append', con=conn)
	last_date = pd.read_sql('SELECT * FROM %s ORDER BY timestamp DESC LIMIT 1' % (db['table']), conn)['timestamp'][0]
	print('正在更新 %s 資料庫 | 最新資料為 %s | 累計寫入 %d 筆資料' % (tableName, last_date, inserts[0]))


while True:
	update_data()

