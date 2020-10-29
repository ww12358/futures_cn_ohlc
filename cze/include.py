# -*- coding:utf-8 -*-
import os

cze_symbols = ['PM', 'WH', 'CF', 'SR', 'AP', 'CJ', # 红枣
               'RI', 'LR', "JR",  #早籼稻， 晚籼稻, 粳稻
                'CY', 'TA', 'PF',     #棉纱，PTA， 短纤
                'OI', 'RS', 'RM',
                'SF', 'SM',  #硅铁， 锰硅
                'MA', 'FG', 'ZC',
                'UR',    #尿素
                'SA',   #纯碱
               ]

cze_all_symbols = ['CF','PM', 'WH', 'SR', 'PTA', 'OI', 'RI', 'MA', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP',
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

HOME_PATH = '/home/sean/PycharmProjects/cn_ex_sync/cze'
DATA_SRC_PATH = os.path.join(HOME_PATH,'cze_src_data')
CZE_DATA_PATH = os.path.join(HOME_PATH, 'cze_data/cze.hdf5')
TEST_DATA_PATH = os.path.join(HOME_PATH,'cze_data/cze_test.hdf5')