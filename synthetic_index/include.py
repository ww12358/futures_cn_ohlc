# -*- coding:utf-8 -*-
import os

HOME_PATH = '/home/sean/PycharmProjects/chn_future_ex_data'
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

##configuration for shfe
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

SHFE_SRC_DATA_PATH = os.path.join(HOME_PATH, 'shfe','shfe_src_data')
SHFE_DATA_PATH = os.path.join(HOME_PATH, 'shfe','shfe_data', 'shfe.hdf5')
SHFE_TEST_DATA_PATH = os.path.join(HOME_PATH, 'shfe', 'shfe_data', 'shfe_bak.hdf5')


##configuration for dce
dce_symbols = ["A", "B", "M", "Y", "P", "C", "CS", "JD",   \
               "BB", "FB",  \
               "JM", "J", "I",  \
               "PP", "L", "V" ]

dce_dtypes = {'symbol' : 'object',
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

dce_headers = ['symbol', 'date', 'pre_close', 'pre_settlement',  \
               'open', 'high', 'low', 'close', 'settlement', \
               'd1', 'd2', 'volume','turnover', 'oi']

DCE_SRC_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_src_data')
DCE_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_data', 'dce.hdf5')
DCE_TEST_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_data', 'dce_clean.hdf5')



##configuration for zce
zce_symbols = ['PM', 'WH', 'CF', 'SR', 'PTA', 'OI', 'RI', 'ME', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP']

zce_all_symbols = ['CF','PM', 'WH', 'SR', 'PTA', 'OI', 'RI', 'ME', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP',
               'WT', 'WS', 'TA', 'RO', 'ER']

zce_headers = ['date', 'symbol', 'pre_settlement', 'open', 'high', 'low', 'close', 'settlement', 'd1', 'd2', 'volume',
               'oi', 'd_oi', 'turnover', 'EDSP']

zce_dtypes = {'symbol' : 'object',
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

ZCE_DATA_PATH = os.path.join(HOME_PATH, "zce", 'zce_data/zce.hdf5')
ZCE_SRC_DATA_PATH = os.path.join(HOME_PATH, "zce", 'zce_src_data')
ZCE_TEST_DATA_PATH = os.path.join(HOME_PATH, "zce", 'zce_data/zce_test.hdf5')

ex_config = {"SHFE":{"symbols":shfe_symbols, "headers":shfe_headers,"dtypes":shfe_dtypes, "SRC_DATA_PATH":SHFE_SRC_DATA_PATH, "DATA_PATH":SHFE_DATA_PATH, "TEST_DATA_PATH":SHFE_TEST_DATA_PATH },
            "DCE":{"symbols":dce_symbols, "headers":dce_headers, "dtypes":dce_dtypes, "SRC_DATA_PATH":DCE_SRC_DATA_PATH, "DATA_PATH":DCE_DATA_PATH, "TEST_DATA_PATH":DCE_TEST_DATA_PATH},
            "ZCE":{"symbols":zce_symbols, "headers":zce_headers, "dtypes":zce_dtypes, "SRC_DATA_PATH":ZCE_DATA_PATH, "DATA_PATH":ZCE_DATA_PATH, "TEST_DATA_PATH":ZCE_TEST_DATA_PATH}
            }