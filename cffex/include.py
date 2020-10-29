# -*- coding:utf-8 -*-
import os

cffex_symbols = ["IF", "IH", "IC",  \
                 "TF", "T", "TS"]   # 五债， 十债， 二债

cffex_headers = ['symbol', 'date', 'pre_close', 'pre_settlement',  \
               'open', 'high', 'low', 'close', 'settlement', \
               'd1', 'd2', 'volume','turnover', 'oi']

HOME_PATH = '/home/sean/PycharmProjects/cn_ex_sync/cffex'
CFFEX_DATA_PATH = os.path.join(HOME_PATH, 'cffex_data', 'cffex.hdf5')

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']