import aioredis
import asyncio
import redis
import pyarrow as pa
import pandas as pd
from sina.include import REDIS_SVR_ADDR, REDIS_DB, REDIS_PORT
from datetime import datetime
from sina.include import SINA_M5_PATH

# def put(contract, df):
#     r = redis.StrictRedis(host='localhost', port=6379, db=0)
#     ser = pyarrow.serialize(df).to_buffer()
#     comp = pyarrow.compress(ser, asbytes=True)
#     size = len(ser)
#     r.set(contract, comp)
#     r.set(contract + "size", size)
async def flush_redis(r, contract, df):
    try:
        await r.set(contract, pa.serialize(df).to_buffer().to_pybytes())
        print(contract , "set to ", df)
    except:
        print("Error ocurred while flushing {0} to redis.".format(contract))

async def update_redis(r, contract, df):
    if df.empty:
        return
    # print(contract, "idx", df)
    print("Buffering : ", contract, datetime.now())
    # df = df.dropna()
    try:
        ser = await r.get(contract)

        if not ser is None:     #redis buffer exists, append data
            df_origin = pa.deserialize(ser)
            # print(df_origin)
            if df_origin is None:
                df_latest = df
            else:
                # df_latest = df_origin.append(df)
                df_latest = pd.concat([df_origin, df], axis=0)
                df_latest.drop_duplicates(keep='last', inplace=True)
                df_latest.sort_index(ascending=True, inplace=True)
                df_latest = df_latest.groupby(df_latest.index).last()
                # df_origin = df_origin.iloc[:-1, :]  # delete last row which is obviously not correct
                # mmu = df_latest.memory_usage(deep=True).sum()
                # if mmu > 1024000:
                #     print(contract, mmu)
                #     print(df_latest.info(), df_latest.tail(10))
                # print("df_latest", df_latest)
            # print(df_latest)
            await r.set(contract, pa.serialize(df_latest).to_buffer().to_pybytes())
        else:       #initialize redis buffer
            print("Error ocurred while buffering {0} to redis.".format(contract))
            print(contract, "is None", df)
            await r.set(contract, pa.serialize(df).to_buffer().to_pybytes())

    except Exception as e:
        print(contract, "not exist", df.info(), df)
        # await r.set(contract, pa.serialize(df).to_buffer().to_pybytes())
        print(str(e))

    print("end buffering {0}, at {1}".format(contract, datetime.now()))

async def store_redis(loop, results):
    try:
        r = await aioredis.Redis.from_url(
            "redis://" + REDIS_SVR_ADDR, max_connections=len(results), db=REDIS_DB, decode_responses=False
        )
        # r = await aioredis.create_redis_pool(
        #     "redis://localhost", minsize=5, maxsize=10, loop=loop, db=1
        # )
        return await asyncio.gather(*(update_redis(r, contract, df) for contract, df in results),  return_exceptions=True, )

    finally:
        await r.close()

async def store_redis_tq(r, contract, quote):
    try:
        if not quote.datetime.isnull().values.any():
            # print(contract, quote)
            quote.index = pd.to_datetime(quote.datetime)
            # print(quote)
            quote = quote.shift(8, freq="H")
            # print(quote)
            quote.loc[quote.volume == 0, 'volume'] = 1
            quote = quote.loc[:, ['open', 'high', 'low', 'close', 'volume', 'close_oi']]
            quote.rename(columns={"close_oi": "oi"}, inplace=True)
            # print(contract, quote.tail(10))

            return await update_redis(r, contract, quote)

        else:
            return

    except Exception as e:
        print("Error occured while store_redis_tq", '\t', str(e))

class buffer():
    def __init__(self, symbol, month, freq, ip_addr, port=6379, db=1, no_print=False):
        # print(ip_addr, port, symbol, month)
        self.df_buf = None
        try:
            self.r = redis.StrictRedis(host=ip_addr, port=port, db=db)
            if month == '00':
                ptn = symbol + '00_' + freq
            else:
                ptn = ''.join([symbol, '??', month])

            # print(ptn)
            k = self.r.keys(pattern=ptn)
            # print(k)
            buf = self.r.get(k[0])
            self.df_buf = pa.deserialize(buf)
            # print(self.df_buf)
        except Exception as e:
            if not no_print:
                print("Error ocurred when retrieving data from redis server.", str(e))
            pass

    def get_df(self):
        return self.df_buf

    def get_latest(self, tm):
        if not self.df_buf is None:
            return self.df_buf.loc[self.df_buf.index > tm]
        else:
            return None



    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self.r

