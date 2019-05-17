# -*- coding:utf-8 -*-
import os

shfe_symbols = ["CU", "AL", "ZN", "PB", "NI", "SN", "AU", "AG", "RB", "WR", "HC",  \
               "FU", "BU", \
               "RU"]    #'SC' removed


shfe_dtypes = {'symbol' : 'object',
            'pre_close': 'float64',
            'pre_settlement': 'float64',
            'open':'float64',
            'high':'float64',
            'low':'float64',
            'close':'float64',
            'settlement':'float64',
            'd1':'float64',
            'd2':'float64',
            'volume':'int64',
            'turnover':'float64',
            'oi':'int64',
         }

shfe_headers = ['symbol', 'date', 'pre_close', 'pre_settlement',  \
               'open', 'high', 'low', 'close', 'settlement', \
               'd1', 'd2', 'volume','turnover', 'oi']

HOME_PATH = '/home/sean/PycharmProjects/cn_ex_sync/shfe'
SRC_PATH = os.path.join(HOME_PATH, 'shfe_src_data')
DATA_PATH = os.path.join(HOME_PATH, 'shfe_data', 'shfe.hdf5')
TEST_DATA_PATH = os.path.join(HOME_PATH, 'shfe_data', 'shfe_bak.hdf5')

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']