# -*- coding: utf-8 -*-
"""
Created on Mon Jul 29 11:40:19 2019

@author: Dimitri K. Sifoua <kemgangdimitri@gmail.com>
"""

#%%
import pandas as pd
from src.data_extraction.hbase_api import HBaseRestAPI
from src.data_extraction.utils import to_timestamp, to_datetime, build_filter, build_xml, get_batch, extract_json, encode
from src.data_extraction.types import NbCol, FilterType, Operator, Comparator, Action, Category

#%%
api = HBaseRestAPI(address='192.168.0.160', port=8080)
api.connect()

#%%
nb_rows = 10
symbol = 'AAPL'
start = to_timestamp('2018-10-15 00:00:00.000000-0400')
end = to_timestamp('2018-10-17 23:59:00.000000-0400')

#%%
table = 'stock'
xml = build_xml(
    batch=get_batch(nb_rows, NbCol.STOCK),
    filters=[
        build_filter(
            filter_type=FilterType.ROW_FILTER,
            operator=Operator.EQUAL,
            comparator_type=Comparator.BINARY_COMPARATOR,
            comparator_value=Action.__dict__[symbol],
        )
    ],
     start_time=start,
     end_time=end
)
endpoint = api.put_table_scanner(table_name=table, xml=xml)
data_batchs = api.get_table_content(endpoint)

dfs = []
while True:
    try:
        json_data = data_batchs.__next__()
        df = extract_json(json_data)
        dfs.append(df)
    except:
        break
        
stock_AAPL = pd.concat(dfs, sort=False, ignore_index=True).sort_values(by=['timestamp'], ascending=False).reset_index(drop=True)
stock_AAPL['datetime'] = stock_AAPL['timestamp'].apply(to_datetime).apply(lambda x: x.to_datetime64())
stock_AAPL = stock_AAPL.dropna()
stock_AAPL

#%%
table = 'rawtweets'
category = 'TREND'
xml = build_xml(
    batch=get_batch(nb_rows, NbCol.RAWTWEETS),
    filters=[
        build_filter(
            filter_type=FilterType.SINGLE_COLUMN_VALUE_FILTER,
            operator=Operator.EQUAL,
            comparator_type=Comparator.BINARY_COMPARATOR,
            comparator_value=Category.__dict__[category],
            family=encode('tweetsData'.encode('utf-8')),
            qualifier=encode('category'.encode('utf-8'))
        )
    ],
    start_time=start,
    end_time=end
)
endpoint = api.put_table_scanner(table_name=table, xml=xml)
data_batches = api.get_table_content(endpoint)

dfs = []
while True:
    try:
        json_data = data_batches.__next__()
        df = extract_json(json_data)
        dfs.append(df)
    except:
        break

rawtweets_AAPL_TREND = pd.concat(dfs, sort=False, ignore_index=True).sort_values(by=['timestamp'], ascending=False).reset_index(drop=True)
rawtweets_AAPL_TREND['datetime'] = rawtweets_AAPL_TREND['timestamp'].apply(to_datetime).apply(lambda x: x.to_datetime64())
rawtweets_AAPL_TREND = rawtweets_AAPL_TREND.dropna()
rawtweets_AAPL_TREND

#%%
table = 'rawtweets'
xml = build_xml(
    batch=get_batch(nb_rows, NbCol.RAWTWEETS),
    filters=[
        build_filter(
            filter_type=FilterType.SINGLE_COLUMN_VALUE_FILTER,
            operator=Operator.EQUAL,
            comparator_type=Comparator.BINARY_COMPARATOR,
            comparator_value=Action.__dict__[symbol],
            family=encode('tweetsData'.encode('utf-8')),
            qualifier=encode('symbols'.encode('utf-8'))
        )
    ],
    start_time=start,
    end_time=end
)
endpoint = api.put_table_scanner(table_name=table, xml=xml)
data_batches = api.get_table_content(endpoint)

dfs = []
while True:
    try:
        json_data = data_batches.__next__()
        df = extract_json(json_data)
        dfs.append(df)
    except:
        break
    
rawtweets_AAPL = pd.concat(dfs, sort=False, ignore_index=True).sort_values(by=['timestamp'], ascending=False).reset_index(drop=True)
rawtweets_AAPL['datetime'] = rawtweets_AAPL['timestamp'].apply(to_datetime).apply(lambda x: x.to_datetime64())
rawtweets_AAPL = rawtweets_AAPL.dropna()
rawtweets_AAPL

#%%
stock_AAPL.to_csv('data/work/stock_AAPL.csv', index=False)
rawtweets_AAPL.to_csv('data/work/rawtweets_AAPL.csv', index=False)
rawtweets_AAPL_TREND.to_csv('data/work/rawtweets_AAPL_TREND.csv', index=False)