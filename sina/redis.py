import aioredis
import asyncio
# import redis
import pyarrow as pa
import pandas as pd

from sina.include import SINA_5M_PATH

# def put(contract, df):
#     r = redis.StrictRedis(host='localhost', port=6379, db=0)
#     ser = pyarrow.serialize(df).to_buffer()
#     comp = pyarrow.compress(ser, asbytes=True)
#     size = len(ser)
#     r.set(contract, comp)
#     r.set(contract + "size", size)
def get_redis_buff(r, contract):
    bytes_origin = r.get(contract)
    print(bytes_origin)
    size_origin = int(r.get(contract + "size"))
    dec = pyarrow.decompress(bytes_origin, size_origin)

    return pyarrow.deserialize((dec))

async def update_redis(r, contract, df):
    # try:
    #     print(contract)
    #
    #     df_origin = get_redis_buff(r, contract)
    #     print(df_origin)
    #     df_latest = df_origin.append(df)
    #     print(df_latest)
    # except Exception as e:
    #     print(str(e))
    #     pass

    # ser = pyarrow.serialize(df).to_buffer()
    # comp = pyarrow.compress(ser, asbytes=True)
    # size = str(len(ser))
    # await r.set(contract, comp)
    # await r.set(contract+"size", size)
    # context = pa.default_serialization_context()
    try:
        ser = await r.get((contract))
        df_origin = pa.deserialize(ser)
        df_latest = df_origin.append(df)
    except Exception as e:
        print(str(e))
        pass

    await r.set(contract, pa.serialize(df_latest).to_buffer().to_pybytes())

    # val = await r.get(key)
    # print(f"Got {key} -> {val}")

async def store_redis(loop, results):
    try:
        r = await aioredis.create_redis_pool(
            "redis://localhost", minsize=5, maxsize=10, loop=loop, db=0
        )
        return await asyncio.gather(*(update_redis(r, contract, df) for contract, df in results),  return_exceptions=True, )

    finally:
        r.close()
        await r.wait_closed()