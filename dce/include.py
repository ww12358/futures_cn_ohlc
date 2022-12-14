# -*- coding:utf-8 -*-
import os

dce_symbols = ["A", "B", "M", "Y", "P", "C", "CS", "RR",
               "JD", "LH",
               "BB", "FB",  # 胶合板， 纤维板
               "JM", "J", "I",
               "PP", "L", "V", "EG", "EB", "PG"]  # 聚丙烯， 聚乙烯， 聚氯乙烯， 乙二醇， 苯乙烯， 液化石油气

dce_symbols_2300pm = ['I', 'J', 'JM', 'A', 'B', 'M', 'P', 'Y', 'C', 'CS', 'PP', 'V', 'EB', 'EG', 'PG', 'RR', 'L']

dce_dtypes = {'symbol': 'object',
              'pre_close': 'float64',
              'pre_settlement': 'float64',
              'open': 'float64',
              'high': 'float64',
              'low': 'float64',
              'close': 'float64',
              'settlement': 'float64',
              'd1': 'float64',
              'd2': 'float64',
              'volume': 'int64',
              'turnover': 'float64',
              'oi': 'int64',
              }

dce_headers = ['symbol', 'date', 'pre_close', 'pre_settlement',
               'open', 'high', 'low', 'close', 'settlement',
               'd1', 'd2', 'volume', 'turnover', 'oi']

HOME_PATH = '/home/sean/PycharmProjects/cn_ex_sync/dce'
SRC_PATH = os.path.join(HOME_PATH, 'dce_src_data')
# DCE_DATA_PATH = os.path.join(HOME_PATH, 'dce_data', 'dce.hdf5')
TEST_DATA_PATH = os.path.join(HOME_PATH, 'dce_data', 'dce_clean.hdf5')

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
