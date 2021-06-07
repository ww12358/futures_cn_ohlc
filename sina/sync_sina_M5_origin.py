import os
import sys
import pandas as pd
import numpy as np
import datetime
import asyncio
# import nest_asyncio
import functools
from concurrent.futures import ThreadPoolExecutor
import concurrent
import time
import random
import click

module_path = os.path.abspath(os.path.join('/home/sean/code/cn_ex_sync'))
if module_path not in sys.path:
    sys.path.append(module_path)

from sina.sina_M5 import sina_M5
from sina.sina_M5_origin import sina_M5_origin
from sina.sina_H1 import sina_H1
from sina.sina_H3 import sina_H3
from sina.download_sina import download_sina_data
from sina.getContractDict import getContractDict, getAllContractDict
from sina.include import watch_list
from cn.include import symbol_exchange_map, all_symbols, dce_symbols

def update_sina_origin(symbol, group):
    with sina_M5_origin(symbol, "M5") as data:
        for month, contract, df in group:
            # df.drop('contract', axis=1, inplace=True)
            # print(df)
            # print(data.get_contract_by_month(month))
            local_data = data.get_contract_by_month(month)
            # print("local", local_data)
            if 'contract' in local_data.columns:
                data.append_data(df, month)
            else:
                local_data['contract'] = contract[2:]
                df_new = pd.concat([local_data, df], axis=0, join='outer')
                df_new.drop_duplicates(keep="first", inplace=True)
                data.overwrite(df_new, month)
                # print(df_new)

            # print(month, contract)
            # data.append_data(df, month)
            # duplicated_count = df.duplicated(keep='first').sum()
            # if duplicated_count:
            #     print("WARNING!!! %d Duplicated data in %s found. please check!" % (duplicated_count, contract))

async def store_sina_origin(loop, results):
    try:
        return await asyncio.gather(*(update_sina_origin(contract, df) for contract, df in results),  return_exceptions=True)
    except Exception as e:
        print("Error %s", str(e))

async def get_sina_contracts(month, contract):
#     print(contract)
    time.sleep(random.randint(0, 3))
    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        try:
            df = await loop.run_in_executor(executor, functools.partial(download_sina_data, contract=contract))
        except:
            print("something wrong")
        #     print(df)
        return (month, contract, df)

def get_sina5m(contract_dict, l):

    for symbol in l:
        try:
            contract_d = contract_dict[symbol]
            print(symbol)
            loop = asyncio.get_event_loop()
            group = asyncio.gather(*[get_sina_contracts(month, contract) for month, contract in contract_d.items()])
            results = loop.run_until_complete(group)
#             print(results)
            update_sina_origin(symbol, results)
#             _, dfs = map(list, zip(*results))

        except Exception as e:
            print("error", str(e))
            pass

    return

@click.command()
@click.option("--all", "-A", is_flag=True, help="download all data")
@click.option("--major", "-M", is_flag=True, help="download important data only")

def main(all, major):

    # watch_list = ["RB"]
    try:
        contract_dict = getAllContractDict()
        # contract = {"RB":{'05':'RB2105'}}

        if all:
            print("Full download starting...")
            get_sina5m(contract_dict, all_symbols)

        if major:
            print("Starting major contracts download...")
            get_sina5m(contract_dict, watch_list)

    except Exception as e:
        print("Error...%s" % str(e))

if __name__ == "__main__":
    main()
