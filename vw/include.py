# -*- coding:utf-8 -*-
import os
from dce.include import dce_symbols, dce_dtypes, dce_headers
from cze.include import cze_symbols, cze_dtypes, cze_headers
from shfe.include import shfe_symbols, shfe_dtypes, shfe_headers

HOME_PATH = '/home/sean/PycharmProjects/cn_ex_sync'
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
from shfe.include import SHFE_DATA_PATH

##configuration for shfe
# shfe_symbols = ["CU", "AL", "ZN", "PB", "NI", "SN", "AU", "AG", "RB", "WR", "HC",  \
#                "FU", "BU", \
#                "RU"]    #'SC' removed

# shfe_dtypes = {'symbol' : 'object',
#             'pre_close': 'float64',
#             'pre_settlement': 'float64',
#             'open':'float64',
#             'high':'float64',
#             'low':'float64',
#             'close':'float64',
#             'settlement':'float64',
#             'd1':'float64',
#             'd2':'float64',
#             'volume':'int64',
#             'turnover':'float64',
#             'oi':'int64',
#          }

# shfe_headers = ['symbol', 'date', 'pre_close', 'pre_settlement',  \
#                'open', 'high', 'low', 'close', 'settlement', \
#                'd1', 'd2', 'volume','turnover', 'oi']

SHFE_SRC_DATA_PATH = os.path.join(HOME_PATH, 'shfe','shfe_src_data')
# SHFE_DATA_PATH = os.path.join(HOME_PATH, 'shfe','shfe_data', 'shfe.hdf5')
SHFE_TEST_DATA_PATH = os.path.join(HOME_PATH, 'shfe', 'shfe_data', 'shfe_bak.hdf5')


##configuration for dce
# dce_symbols = ["A", "B", "M", "Y", "P", "C", "CS", "JD",   \
#                "BB", "FB",  \
#                "JM", "J", "I",  \
#                "PP", "L", "V" ]

# dce_dtypes = {'symbol' : 'object',
#             'pre_close': 'float64',
#             'pre_settlement': 'float64',
#             'open':'float64',
#             'high':'float64',
#             'low':'float64',
#             'close':'float64',
#             'settlement':'float64',
#             'd1':'float64',
#             'd2':'float64',
#             'volume':'int64',
#             'turnover':'float64',
#             'oi':'int64',
#          }
#
# dce_headers = ['symbol', 'date', 'pre_close', 'pre_settlement',  \
#                'open', 'high', 'low', 'close', 'settlement', \
#                'd1', 'd2', 'volume','turnover', 'oi']
from dce.include import DCE_DATA_PATH
DCE_SRC_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_src_data')
# DCE_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_data', 'dce.hdf5')
DCE_TEST_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_data', 'dce_clean.hdf5')

###  CZE  ###
from cze.include import cze_symbols, cze_headers, cze_dtypes, CZE_DATA_PATH
##configuration for zce
#zce_symbols = ['PM', 'WH', 'CF', 'SR', 'PTA', 'OI', 'RI', 'ME', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP']

#zce_all_symbols = ['CF','PM', 'WH', 'SR', 'PTA', 'OI', 'RI', 'ME', 'FG', 'RS', 'RM', 'ZC', 'JR', 'LR', 'SM', 'CY', 'AP',
#               'WT', 'WS', 'TA', 'RO', 'ER']

# cze_headers = ['date', 'symbol', 'pre_settlement', 'open', 'high', 'low', 'close', 'settlement', 'd1', 'd2', 'volume',
#                'oi', 'd_oi', 'turnover', 'EDSP']
"""
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
"""

# CZCE_DATA_PATH = os.path.join(HOME_PATH, "cze", 'cze_data/cze.hdf5')
CZCE_DATA_PATH = CZE_DATA_PATH
CZCE_SRC_DATA_PATH = os.path.join(HOME_PATH, "cze", 'cze_src_data')
# from cze.include import CZE_DATA_PATH
CZCE_TEST_DATA_PATH = os.path.join(HOME_PATH, "cze", 'cze_data/cze_test.hdf5')


# INE #
from ine.include import ine_symbols, ine_headers, INE_DATA_PATH
ine_dtypes = shfe_dtypes

# CFFEX  #
from cffex.include import cffex_symbols, cffex_headers, CFFEX_DATA_PATH
cffex_dtypes = shfe_symbols

ex_config = {"SHFE":{"symbols":shfe_symbols, "headers":shfe_headers,"dtypes":shfe_dtypes, "SRC_DATA_PATH":SHFE_SRC_DATA_PATH, "DATA_PATH":SHFE_DATA_PATH, "TEST_DATA_PATH":SHFE_TEST_DATA_PATH },
            "DCE":{"symbols":dce_symbols, "headers":dce_headers, "dtypes":dce_dtypes, "SRC_DATA_PATH":DCE_SRC_DATA_PATH, "DATA_PATH":DCE_DATA_PATH, "TEST_DATA_PATH":DCE_TEST_DATA_PATH},
            "CZCE":{"symbols":cze_symbols, "headers":cze_headers, "dtypes":cze_dtypes, "SRC_DATA_PATH":CZCE_DATA_PATH, "DATA_PATH":CZCE_DATA_PATH, "TEST_DATA_PATH":CZCE_TEST_DATA_PATH},
            "INE":{"symbols":ine_symbols, "headers":ine_headers, "dtypes":ine_dtypes, "SRC_DATA_PATH":INE_DATA_PATH, "DATA_PATH":INE_DATA_PATH},
            "CFFEX":{"symbols":cffex_symbols, "headers":cffex_headers, "dtypes":cffex_dtypes, "DATA_PATH":CFFEX_DATA_PATH},
            }

idx_headers = ['symbol', 'date',    \
               'open', 'high', 'low', 'close', 'settlement', \
               'volume','turnover', 'oi']

idx_dtypes = {'symbol' : 'object',
            'date': 'datetime64[ns]',
            'open':'float64',
            'high':'float64',
            'low':'float64',
            'close':'float64',
            'settlement':'float64',
            'volume':'int64',
            'turnover':'float64',
            'oi':'int64',
         }