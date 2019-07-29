# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 15:43:26 2019

@author: dimitri.sifouakemgan
"""

#%%
#import numpy as np
import pandas as pd
import datetime

#%%
datetime_parser_func = lambda date: datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S.%f')

#%%
stock_AAPL = pd.read_csv('data/work/stock_AAPL.csv', parse_dates=['datetime'], date_parser=datetime_parser_func)
rawtweets_AAPL = pd.read_csv('data/work/rawtweets_AAPL.csv', parse_dates=['datetime'], date_parser=datetime_parser_func)
rawtweets_AAPL_TREND = pd.read_csv('data/work/rawtweets_AAPL_TREND.csv', parse_dates=['datetime'], date_parser=datetime_parser_func)

#%%
try:
    stock_AAPL = stock_AAPL.drop(['key'], axis=1)
    rawtweets_AAPL = rawtweets_AAPL.drop([
            'category', 'createdate', 'idtweetos', 
            'nbLike', 'retweetcount', 'symbols',
            'tweetosname', 'userFavorites', 'userFollowers',
            'key'
    ], axis=1)
    rawtweets_AAPL_TREND = rawtweets_AAPL_TREND.drop([
            'category', 'createdate', 'idtweetos', 
            'nbLike', 'retweetcount', 'symbols',
            'tweetosname', 'userFavorites', 'userFollowers',
            'key'
    ], axis=1)
except:
    pass

#%%
rawtweets_AAPL = rawtweets_AAPL.loc[rawtweets_AAPL['datetime'] <= datetime.datetime(2018, 10, 17, 23, 59, 59)]
rawtweets_AAPL_TREND = rawtweets_AAPL_TREND.loc[rawtweets_AAPL_TREND['datetime'] <= datetime.datetime(2018, 10, 17, 23, 59, 59)]

#%%
def to_datetime(timestamp):
    return pd.to_datetime(timestamp, unit='ms').tz_localize('UTC').tz_convert('US/Eastern')

def nearest(items, pivot):
    return min(items, key=lambda x: abs(x - pivot))

def match_timestamp(stock, x):
    row = stock[stock['timestamp'] == nearest(stock['timestamp'].values, x['timestamp'])][['price', 'timestamp']].reset_index(drop=True)
    if row.shape[0] > 1:
        print('aze')
    v = (row.loc[0, 'price'], row.loc[0, 'timestamp'])
    return v

#%%
rawtweets_AAPL['price'] = rawtweets_AAPL.apply(lambda x: match_timestamp(stock_AAPL, x), axis=1)
rawtweets_AAPL['price_timestamp'] = rawtweets_AAPL['price'].apply(lambda x: x[1])
rawtweets_AAPL['price'] = rawtweets_AAPL['price'].apply(lambda x: x[0])
rawtweets_AAPL['diff_datetime'] = rawtweets_AAPL.apply(lambda x: abs(to_datetime(x['timestamp']) - to_datetime(x['price_timestamp'])), axis=1)

#%%
rawtweets_AAPL_TREND['price'] = rawtweets_AAPL_TREND.apply(lambda x: match_timestamp(stock_AAPL, x), axis=1)
rawtweets_AAPL_TREND['price_timestamp'] = rawtweets_AAPL_TREND['price'].apply(lambda x: x[1])
rawtweets_AAPL_TREND['price'] = rawtweets_AAPL_TREND['price'].apply(lambda x: x[0])
rawtweets_AAPL_TREND['diff_datetime'] = rawtweets_AAPL_TREND.apply(lambda x: abs(to_datetime(x['timestamp']) - to_datetime(x['price_timestamp'])), axis=1)

#%%
