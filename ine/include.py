# -*- coding:utf-8 -*-
import os

ine_symbols = ["SC", "NR"]

ine_headers = ['symbol', 'date', 'pre_close', 'pre_settlement',  \
               'open', 'high', 'low', 'close', 'settlement', \
               'd1', 'd2', 'volume','turnover', 'oi']

HOME_PATH = '/home/sean/PycharmProjects/cn_ex_sync/ine'
INE_DATA_PATH = os.path.join(HOME_PATH, 'ine_data', 'ine.hdf5')

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']