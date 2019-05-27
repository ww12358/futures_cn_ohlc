# -*- coding:utf-8 -*-
from shfe.include import SHFE_DATA_PATH, shfe_symbols
from dce.include import DCE_DATA_PATH, dce_symbols
from cze.include import CZE_DATA_PATH, cze_symbols

cn_headers = ['pre_close', 'pre_settlement',  \
               'open', 'high', 'low', 'close', 'settlement', \
               'd1', 'd2', 'volume','turnover', 'oi']

DATA_PATH_DICT = {i : SHFE_DATA_PATH for i in shfe_symbols}
temp_dict = { i : DCE_DATA_PATH for i in dce_symbols}
DATA_PATH_DICT.update(temp_dict)
temp_dict = { i : CZE_DATA_PATH for i in cze_symbols}
DATA_PATH_DICT.update(temp_dict)

all_symbols = shfe_symbols + dce_symbols + cze_symbols

exchange_symbols_map = {"SHFE": shfe_symbols,
                        "DCE": dce_symbols,
                        "CZCE": cze_symbols}
all_exchanges = ["SHFE", "DCE", "CZCE"]
symLists = [shfe_symbols, dce_symbols, cze_symbols]

ex_symList_map = dict(zip(all_exchanges, symLists))
symbol_exchange_map = {}
for ex in all_exchanges:
    symbol_exchange_map.update({i: ex for i in ex_symList_map[ex]})


ts_exchanges = ["SHF", "DCE", "ZCE"]

ql_exchanges = ["SHFE", "DCE", "ZCE"]

local_ts_ex_map = dict(zip(all_exchanges, ts_exchanges))

