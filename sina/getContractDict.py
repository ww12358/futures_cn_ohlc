import sys
import os
import pandas as pd
import tushare as ts
import re
from datetime import datetime
from cn.config import TUSHARE_TOKEN

module_path = os.path.abspath(os.path.join('/home/sean/code/play_with_data/utils'))
cn_path = os.path.abspath(os.path.join('/home/sean/code/cn_ex_sync'))
if module_path not in sys.path:
    sys.path.append(module_path)
    sys.path.append(cn_path)

from cn.include import all_symbols, all_exchanges, symbol_exchange_map, exchange_symbols_map

ts.set_token(TUSHARE_TOKEN)
pro = ts.pro_api()

def getContractDict(symbol):
    if symbol in all_symbols:
        exchange = symbol_exchange_map[symbol]
    #     print("Exchange:{ex}, Symbol:{smbl}".format(ex=exchange, smbl=symbol))
        basics_df = pro.fut_basic(exchange=exchange, fut_type='1', fields='ts_code,symbol,list_date,delist_date')

    basics_df["list_date"] = pd.to_datetime(basics_df["list_date"], format="%Y%m%d")
    basics_df["delist_date"] = pd.to_datetime(basics_df["delist_date"], format="%Y%m%d")


    active_contracts = basics_df.loc[(basics_df.list_date <= datetime.now()) & (basics_df.delist_date >= datetime.now())]
    cur_contracts = active_contracts[active_contracts.ts_code.str.contains(symbol)]

    contracts = [x[0:6] for x in cur_contracts["ts_code"].values]
    # print(contracts)
    contract_dict = {}
    for contract in contracts:
        contract_dict[contract[4:]] = contract

    return contract_dict


def getAllContractDict(debug, dt = datetime.now()):

    all_contracts = {}  # {'CU':{'05':'CU2105', '08':'CU2108'...},
    #                       'A':{'09":''A2109', '12': ' A2112'}
    #                     }
    for exchange in all_exchanges:
        #     print(exchange)
        basics_df = pro.fut_basic(exchange=exchange, fut_type='1', fields='ts_code,symbol,list_date,delist_date')
        basics_df["list_date"] = pd.to_datetime(basics_df["list_date"], format="%Y%m%d")
        basics_df["delist_date"] = pd.to_datetime(basics_df["delist_date"], format="%Y%m%d")
        active_contracts = basics_df.loc[
            (basics_df.list_date <= dt) & (basics_df.delist_date >= dt)]
        # print(active_contracts)

        try:
            for symbol in exchange_symbols_map[exchange]:
                # print(symbol)
                # cur_contracts = active_contracts[active_contracts.ts_code.str.contains(symbol)]
                # print(cur_contracts)
                mask = active_contracts.ts_code.apply(lambda x: re.search("^\D+", x).group() == symbol)
                current_contracts = active_contracts[mask]
                length = len(symbol) + 4
                contracts = [x[0:length] for x in current_contracts["ts_code"].values]
                #         print(contracts)
                contract_dict = {}
                for contract in contracts:
                    month = contract[(len(symbol) + 2):]
                    #             print(month)
                    contract_dict[month] = contract

                all_contracts[symbol] = contract_dict

        except KeyError:
            continue
        except Exception as e:
            print(str(e))

    return all_contracts
