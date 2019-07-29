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

#%%11