import aioredis
import asyncio
import redis
import pyarrow

# def put(contract, df):
#     r = redis.StrictRedis(host='localhost', port=6379, db=0)
#     ser = pyarrow.serialize(df).to_buffer()
#     comp = pyarrow.compress(ser, asbytes=True)
#     size = len(ser)
#     r.set(contract, comp)
#     r.set(contract + "size", size)

async def update_redis(r, contract, df):
    ser = pyarrow.serialize(df).to_buffer()
    comp = pyarrow.compress(ser, asbytes=True)
    size = str(len(ser))
    await r.set(contract, comp)
    await r.set(contract+"size", size)

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