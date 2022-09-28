# -*- coding:utf-8 -*-
import os

from cn.config import DCE_DATA_PATH, CZE_DATA_PATH, SHFE_DATA_PATH, INE_DATA_PATH, CFFEX_DATA_PATH
from dce.include import dce_symbols, dce_dtypes, dce_headers
from shfe.include import shfe_symbols, shfe_dtypes, shfe_headers
from cze.include import cze_symbols, cze_headers, cze_dtypes
from ine.include import ine_symbols, ine_headers
from cffex.include import cffex_symbols, cffex_headers

HOME_PATH = '/home/sean/PycharmProjects/cn_ex_sync'
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']

# SHFE
SHFE_SRC_DATA_PATH = os.path.join(HOME_PATH, 'shfe', 'shfe_src_data')
# SHFE_DATA_PATH = os.path.join(HOME_PATH, 'shfe','shfe_data', 'shfe.hdf5')
SHFE_TEST_DATA_PATH = os.path.join(HOME_PATH, 'shfe', 'shfe_data', 'shfe_bak.hdf5')

# DCE
DCE_SRC_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_src_data')
# DCE_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_data', 'dce.hdf5')
DCE_TEST_DATA_PATH = os.path.join(HOME_PATH, 'dce', 'dce_data', 'dce_clean.hdf5')

# CZCE
# CZCE_DATA_PATH = os.path.join(HOME_PATH, "cze", 'cze_data/cze.hdf5')
CZCE_DATA_PATH = CZE_DATA_PATH
CZCE_SRC_DATA_PATH = os.path.join(HOME_PATH, "cze", 'cze_src_data')
# from cze.include import CZE_DATA_PATH
CZCE_TEST_DATA_PATH = os.path.join(HOME_PATH, "cze", 'cze_data/cze_test.hdf5')

# INE
ine_dtypes = shfe_dtypes

# CFFEX
cffex_dtypes = shfe_symbols

ex_config = {"SHFE": {"symbols": shfe_symbols, "headers": shfe_headers, "dtypes": shfe_dtypes,
                      "SRC_DATA_PATH": SHFE_SRC_DATA_PATH, "DATA_PATH": SHFE_DATA_PATH,
                      "TEST_DATA_PATH": SHFE_TEST_DATA_PATH},
             "DCE": {"symbols": dce_symbols, "headers": dce_headers, "dtypes": dce_dtypes,
                     "SRC_DATA_PATH": DCE_SRC_DATA_PATH, "DATA_PATH": DCE_DATA_PATH,
                     "TEST_DATA_PATH": DCE_TEST_DATA_PATH},
             "CZCE": {"symbols": cze_symbols, "headers": cze_headers, "dtypes": cze_dtypes,
                      "SRC_DATA_PATH": CZCE_DATA_PATH, "DATA_PATH": CZCE_DATA_PATH,
                      "TEST_DATA_PATH": CZCE_TEST_DATA_PATH},
             "INE": {"symbols": ine_symbols, "headers": ine_headers, "dtypes": ine_dtypes,
                     "SRC_DATA_PATH": INE_DATA_PATH, "DATA_PATH": INE_DATA_PATH},
             "CFFEX": {"symbols": cffex_symbols, "headers": cffex_headers, "dtypes": cffex_dtypes,
                       "DATA_PATH": CFFEX_DATA_PATH},
             }

idx_headers = ['symbol', 'date',
               'open', 'high', 'low', 'close', 'settlement',
               'volume', 'turnover', 'oi']

idx_dtypes = {'symbol': 'object',
              'date': 'datetime64[ns]',
              'open': 'float64',
              'high': 'float64',
              'low': 'float64',
              'close': 'float64',
              'settlement': 'float64',
              'volume': 'int64',
              'turnover': 'float64',
              'oi': 'int64',
              }
