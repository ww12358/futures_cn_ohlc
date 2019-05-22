# -*- coding:utf-8 -*-
import os

cze_symbols = ['PM', 'WH', 'CF', 'SR', 'PTA', 'OI', 'RI', 'ME', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP']

cze_all_symbols = ['CF','PM', 'WH', 'SR', 'PTA', 'OI', 'RI', 'ME', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP',
               'WT', 'WS', 'TA', 'RO', 'ER']

months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

year_str = ['10', '11', '12', '13', '14', '15', '16', '17', '18', '19']

cze_headers = ['date', 'symbol', 'pre_settlement', 'open', 'high', 'low', 'close', 'settlement', 'd1', 'd2', 'volume',
               'oi', 'd_oi', 'turnover', 'EDSP', 'DEL']

cze_dtypes = {'symbol' : 'object',
            'pre_settlement': 'float64',
            'open':'float64',
            'high':'float64',
            'low':'float64',
            'close':'float64',
            'settlement':'float64',
            'd1':'float64',
            'd2':'float64',
            'volume':'int64',
            'oi':'int64',
            'd_oi':'int64',
            'turnover':'float64',
            'EDSP':'float64'
         }

HOME_PATH = '/home/sean/PycharmProjects/chn_future_ex_data/zce'
DATA_SRC_PATH = os.path.join(HOME_PATH,'zce_src_data')
CZE_DATA_PATH = os.path.join(HOME_PATH, 'zce_data/zce.hdf5')
TEST_DATA_PATH = os.path.join(HOME_PATH,'zce_data/zce_test.hdf5')