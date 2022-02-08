import asyncio
import json
import pyarrow as pa
import aioredis
import redis
import pandas as pd
import numpy as np
from concurrent import futures
import functools
from sina.redis_buffer import update_redis

def ohlcsum(data):
    # print(data.columns)
    if data.empty:
        return data
    else:
        return pd.DataFrame({
            'open': data['open'].iloc[0],
            'high': data['high'].max(),
            'low': data['low'].min(),
            'close': data['close'].iloc[-1],
            'volume': data['volume'].sum(),
            'oi': data['oi'].iloc[-1]
        }, index=data.index)

async def gen_idx(symbol, cInfo, freq, r, loop):
    km = kMem(symbol, cInfo)
    if freq == '1min':
        await load_hfreq(km, r)
        with futures.ProcessPoolExecutor() as executor:
        # await loop.run_in_executor(executor, functools.partial(load_hfreq, km=km, r=r))
            df = await loop.run_in_executor(executor, functools.partial(km.to_idx, freq))
            # print(df)
            await update_redis(r, symbol+"00_"+freq, df)
        # try:
        #     with futures.ThreadPoolExecutor() as executor:
        #         await loop.run_in_executor(executor, functools.partial(load_hfreq, km=km, r=r))
        # except Exception as e:
        #     print(str(e)
    elif freq in ['5min', '15min', '30min', '1h', '4h']:
        # with futures.ThreadPoolExecutor() as executor:
        df = await load_1min(km, r)
        # df = df.iloc[:-1, :]
        # print(df)
        g = df.groupby(pd.Grouper(freq=freq, offset='21h', closed='left', label='left'))
        # g.apply(print)
        df_result = g.apply(ohlcsum)
        df_result = df_result.groupby(pd.Grouper(freq=freq, offset='21h', closed='left', label='left')).agg('last')
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print("df_result", df_result)
        df_result = df_result.dropna()
        df_result = df_result.iloc[:-1, :]  # delete last row which is obviously not correct
        # print(df_result)
        # gv = df_result.groupby(pd.Grouper(freq='24h', offset='21h', closed='left', label='left'))
        # # gv.apply(print)
        # df_result_offset = gv.apply(trans_volume)
        await update_redis(r, symbol + "00_" + freq, df_result)
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print("df_result", df_result.tail(40))
        # df = load_1min()
    return

async def load_1min(km, r):
   try:
       ptn = km.symbol + "00_1min"
       k = await r.keys(ptn)
       buf = await r.get(k[0])
       km.kandles['1min'] = pa.deserialize(buf)
   except Exception as e:
       print("Error occured while fetching {0} 1min".format(km.symbol), ptn, '\t', str(e))
       return None

   return km.kandles['1min']

async def load_hfreq(km, r):
    try:
        for c in km.all_contracts:
            ptn = km.symbol + '??' + c
            # print(ptn)
            k = await r.keys(ptn)
            buf = await r.get(k[0])
            raw_df = pa.deserialize(buf)
            # g = raw_df.groupby(raw_df.index, sort=True)
            km.dfs[c] = raw_df.groupby(raw_df.index).apply(lambda g:g.iloc[-1])     #use only last row of each group
            # print(km.dfs[c])
    except IndexError:
        print("Data of {1} exist on redis. Pass...".format(ptn))
        pass
    except Exception as e:
        print("Error occured while loading hfreq contracts from redis...\t", str(e))

async def clean_hfreq(km, r):
    try:
        for c in km.all_contracts:
            ptn = km.symbol + '??' + c
            k = await r.keys(ptn)
            buf = await r.get(k[0])
            raw_df = pa.deserialize(buf)
            df = raw_df.groupby(raw_df.index).apply(lambda g:g.iloc[-1])    #delete all redundant data
            await update_redis(r, ptn, df, force=True)
    except IndexError:
        print("Data of exist on redis. Pass...")
        pass
    except Exception as e:
        print("Error occured while cleaning hfreq contracts...\t", str(e))

class kMem:
    # async def _init(self):
    #     self._r = await aioredis.create_redis_pool(
    #         "redis://localhost", minsize=5, maxsize=10, db=1
    #     )

    def __init__(self, symbol, cInfo):
        self.symbol = symbol
        # self._r = redis.StrictRedis(host='127.0.0.1', port=6379, db=1)
        self.cInfo = cInfo
        # self.loop = loop
        self.all_contracts = self.get_all_contracts()
        self.contract_1st = self.all_contracts[0]
        self.dfs = {}
        self.kandles = {}
        # self.load_hfreq(loop)

        return

    # def __del__(self):
    #     self._r.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, tb):
        self._r.close()

        return True

    def get_all_contracts(self):
        # print(self.cInfo)
        return self.cInfo['all']

    def get_1st_contract(self):
        return self.contract_1st

    def to_idx(self, freq):
        if len(self.dfs) > 0:
            dfs = self.dfs.values()
            df_concat = pd.concat(dfs, axis=0)
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_concat)

            df_concat = df_concat[df_concat["volume"] > 1]      # avoid divide by 0 when wavg
            # print(self.symbol, df_concat)
            g = df_concat.groupby(df_concat.index, sort=True)
            # g.apply(print)
            # for date, group in g:
            #     print(date)
            #     print(group)

            df_00 = pd.concat([g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
                               g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
                               g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
                               g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
                               g.apply(lambda x: np.sum(x['volume'])),
                               g.apply(lambda x: np.sum(x['oi'])),
                               ],
                              axis=1, keys=['open', 'high', 'low', 'close', 'volume', 'oi'])

            # print(self.symbol, df_00)
            if freq == "1min":
                return df_00
        else:
            print("{} contracts not loaded. Skip generating index.", self.symbol,)
            return None

    def getMainContract(self):
        with open("/home/sean/code/utils/main_contracts.json", "r") as f:
            m_con = json.load(f)

        self.mainContract = m_con[self.symbol]['1st']

        return self.mainContract