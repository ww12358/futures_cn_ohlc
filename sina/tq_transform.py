import pandas as pd
import datetime
import click
from concurrent import futures
import asyncio
import aioredis
import functools
import json

from cn.include import all_symbols

from sina.sina_M5_origin import sina_M5_origin
from sina.sina_H1 import sina_H1
from sina.sina_H3 import sina_H3
from sina.sina_M15 import sina_M15
from sina.sina_M30 import sina_M30
from sina.include import CONTRACT_INFO_PATH
from sina.kMem import kMem
from cn.localData import localData
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning)

# watch_list = ["CU", "RB", "I", "A", "M", "Y", "TA", "SR", "CF", "AL", "ZC"]
watch_list = ["CU", "P", "ZC", "SC"]

def ohlcsum(data):
    if data.empty:
        return data
    #     df = df.sort_index()
    else:
        return pd.DataFrame({
            'open': data['open'].iloc[0],
            'high': data['high'].max(),
            'low': data['low'].min(),
            'close': data['close'].iloc[-1],
            'volume': data['volume'].sum(),
            'contract': data['contract']
        }, index=data.index)

def freq_map(self):
    freq_dic = {"M5": "5min", "H1":"1h", "H3":"3h", "D":"1d"}
    print(freq_dic[self.freq])

    return freq_dic[self.freq]

def convert_sinaM5origin(symbol, freq, data, rebuild):
    # tm = data.iloc[-2].name
    with sina_M5_origin(symbol, "M5") as data_m5:
        months = data_m5.get_symbol_months()

        if months is None:      #sina_M5_origin data does not exist
            return

        for month in months:
            df_m5 = data_m5.get_contract_by_month(month)
            df_m5.drop_duplicates(keep=False, inplace=True)
            print(df_m5)
            # df_m5 = df_m5.loc[df_m5.index > tm]
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_m5)
            g = df_m5.groupby(pd.Grouper(freq=data.freq_map(), offset='21h', closed='right', label='left'))
            # g = g.filter(lambda x : len(x) > 3)
            # g = g.transform('size')>1
            # g.apply(print)
            df_trans = g.apply(ohlcsum)
            # print(df_trans)
            df_result = df_trans.groupby(pd.Grouper(freq=freq, offset='21h', closed='right', label='left')).agg('last')
            # df_result = df_result.iloc[:-1, :]      #save data till last whole period, delete the last row, which is the error current period
            df_result.dropna(inplace=True)
            print(month, df_result)
            if not rebuild:
                data.append_data(df_result, month)
            else:
                data.overwrite(df_result, month)
        return

def trans_freq(symbol, freq, rebuild):
    print(symbol)
    if freq in ["5mim", "15min", "30min", "1h", "3h", "1d"]:
        pass
    else:
        print("Invalid frequency. Quit.")
        return

    try:
        if freq == "15min":
            with sina_M15(symbol, "M15") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

        elif freq == "30min":
            with sina_M30(symbol, "M30") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

        elif freq == "1h":
            with sina_H1(symbol, "H1") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

        elif freq == "3h":
            with sina_H3(symbol, "H3") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

    except Exception as e:
        print(str(e))
        pass

async def load_symbol(symbol_li):
    try:

        # r = await aioredis.create_redis_pool(
        #     "redis://localhost", minsize=5, maxsize=10, db=1
        # )

        with open(CONTRACT_INFO_PATH) as f:
            cInfo_j = json.load(f)

        loop = asyncio.get_event_loop()
        for smbl in symbol_li:
            with futures.ThreadPoolExecutor() as executor:
                cInfo = cInfo_j[smbl]
                # print(cInfo)
                loop.run_in_executor(executor, functools.partial(load_contracts, symbol=smbl, cInfo=cInfo, loop=loop))
        # group = await asyncio.gather(*[load_contracts(symbol, r) for symbol in watch_list])
        # results = loop.run_until_complete(group)
        # for smbl in watch_list:
        #     print(smbl)
        #     with futures.ThreadPoolExecutor() as executor:
        #         try:
        #             # df = await loop.run_in_executor(executor, functools.partial(download_sina_data_hq, contract=contract))
        #             grp = await asyncio.gather(*[loop.run_in_executor(executor, functools.partial(load_all, symbol=smbl, r=r))])
        #             result = asyncio.run_until
        #
        #         # except ValueError as e:
        #         #     if str(e) == "BUSTERED":
        #         #         raise ValueError("F!!!")
        #         #     else:
        #         #         print(str(e))
        #         except:
        #             print("something wrong when running get_sina_contracts")
        #         #     print(df)

        return

    finally:
        print("all done")

    return

# async def load_redis(symbol, cInfo, r):
#     contract = kMem(symbol, cInfo, r)
#     await contract._init()
def load_contracts(symbol, cInfo, loop):
    # print(symbol)
    # contract = kMem(symbol, cInfo, loop)
    # await contract._init()
    # print(contract.get_1st_contract())
    # await r.set(symbol+'_done', 'yes')
    with kMem(symbol, cInfo, loop) as contract:
        main_contract = contract.get_1st_contract()
    print(symbol, contract.get_all_contracts())
    return main_contract

@click.command()
@click.option("--symbol", "-S", type=click.STRING, help="contract symbol")
@click.option("--rebuild", "-R", is_flag=True, help="rebuild all data rows")
@click.option("--all", "-A", is_flag=True, help="download all data")
@click.option("--major", "-M", is_flag=True, help="download important data only")
@click.option("--freq", "-F",  type=click.STRING, help="freqency inlcude 5min, 15min, 30min, 1h, 3h, 1d")

def main(all, major, symbol, freq, rebuild=False):
    print(all_symbols)

    if all:
        asyncio.run(load_symbol(all_symbols))
        return

    elif major:
        asyncio.run(load_symbol(watch_list))
        return

    # else:
        symbol = symbol.strip().upper()
        # for smbl in all_symbols:
        #     # trans_freq(smbl, freq, rebuild)
        #     print(smbl)
        # if symbol in all_symbols:
        #     # trans_freq(symbol, freq, rebuild)
        #     print(symbol)
        #     return
        # else:
        #     print("not a valid symbol")
        # return




if __name__ == "__main__":
    main()
    # asyncio.run(main())
    # loop = asyncio.get_event_loop()
    # task = loop.create_task(main())
    # loop.run_until_complete(task)
