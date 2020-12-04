import sys
import os
import pandas as pd
import tushare as ts
import json
from functools import reduce

module_path = os.path.abspath(os.path.join('/home/sean/code/play_with_data/utils'))
cn_path = os.path.abspath(os.path.join('/home/sean/code/cn_ex_sync'))
if module_path not in sys.path:
    sys.path.append(module_path)
    sys.path.append(cn_path)

from cn.tsData import tsData
from cn.updateCN import update_cn_latest
from cn.updateCN import get_start_end_date
from cn.include import all_symbols, symbol_exchange_map

ts.set_token('d0d22ccf30dfceef565c7d36d8d6cefd43fe4f35200575a198124ba5')
pro = ts.pro_api()



def getContractDict(symbol):
    if symbol in all_symbols:
        exchange = symbol_exchange_map[symbol]
    #     print("Exchange:{ex}, Symbol:{smbl}".format(ex=exchange, smbl=symbol))
        basics_df = pro.fut_basic(exchange=exchange, fut_type='1', fields='ts_code,symbol,list_date,delist_date')

    basics_df["list_date"] = pd.to_datetime(basics_df["list_date"], format="%Y%m%d")
    basics_df["delist_date"] = pd.to_datetime(basics_df["delist_date"], format="%Y%m%d")

    from datetime import datetime
    active_contracts = basics_df.loc[(basics_df.list_date <= datetime.now()) & (basics_df.delist_date >= datetime.now())]
    cur_contracts = active_contracts[active_contracts.ts_code.str.contains(symbol)]

    contracts = [x[0:6] for x in cur_contracts["ts_code"].values]
    # print(contracts)
    contract_dict = {}
    for contract in contracts:
        contract_dict[contract[4:]] = contract

    return contract_dict