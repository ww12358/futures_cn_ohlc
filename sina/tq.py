import aioredis
import asyncio
import functools
import datetime
from cn.include import symbol_exchange_map
from sina.redis_buffer import store_redis_tq
from sina.include import REDIS_SVR_ADDR, REDIS_DB, REDIS_PORT
import nest_asyncio
nest_asyncio.apply()
# def exec_async(func, *args, **kwargs):
#     with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
#         future = executor.submit(func, *args, **kwargs)
#     return future.result()

# async def get_quote(api, contract_tq, contract, q):
#     # quote = api.get_quote(contract)
#     try:
#         quote = await api.get_kline_serial(contract_tq, 60)
#         print(quote)
#         async with api.register_update_notify(quote) as channel:
#             async for _ in channel:
#                 # print(contract, quote.datetime, quote.last_price)
#                 if api.is_changing(quote):
#                     # await store_redis_tq(r, contract, quote)
#                     q.append((contract, quote))
#                     # print(q)
#                     print(datetime.datetime.now())
#                     # q.put((contract, quote))
#                 # await asyncio.sleep(60)
#     except Exception as e:
#         print("Error in get_quote()", str(e))
#         pass

async def update_quote(r, api, contract_tq, contract):
    try:
        quote = await api.get_kline_serial(contract_tq, 60)
        # print(quote)
        async with api.register_update_notify(quote) as channel:
            async for _ in channel:
                if api.is_changing(quote):
                    await store_redis_tq(r, contract, quote)
                    # print(datetime.datetime.now())
    except Exception as e:
        print("Error in get_quote()", str(e))

async def update_symbol(r, api, loop, contract_tq_d, contract_d):

    group = await asyncio.gather(
        *[update_quote(r, api, contract_tq_d[k], contract) for k, contract in contract_d.items()])
    results = loop.run_until_complete(group)

async def get_quote(api, smb_li, tq_contract_dict, contract_dict):

    try:
        loop = asyncio.get_event_loop()
        r = await aioredis.Redis.from_url(
            "redis://" + REDIS_SVR_ADDR, max_connections=10 * len(smb_li), db=REDIS_DB, decode_responses=False
        )
        # r = await aioredis.create_redis_pool(
        #     "redis://localhost", minsize=5, maxsize=10, loop=loop, db=1
        # )
        # for symbol in t_symbols:
        #     contract_tq_d = tq_contract_dict[symbol]
        #     contract_d = contract_dict[symbol]
        #     loop.call_soon_threadsafe(
        #         functools.partial(update_symbol, r, api, loop, contract_tq_d, contract_d))
        #             # print(contract_tq_d[k])

        grp = await asyncio.gather(
            *[update_symbol(r, api, loop, tq_contract_dict[symbol], contract_dict[symbol]) for symbol in smb_li]
        )
        result = loop.run_until_complete(grp)

        # quote = api.get_quote(contract)

            #     contract_tq = exchange+'.'+contract_tq_d[k]
            #     print(k, contract, contract_tq)
            #     try:
            #         quote = await api.get_kline_serial(contract_tq, 60)
            #         # print(quote)
            #         async with api.register_update_notify(quote) as channel:
            #             async for _ in channel:
            #                 if api.is_changing(quote):
            #                     await store_redis_tq(r, contract, quote)
            #                     # print(datetime.datetime.now())
            #                     # q.put((contract, quote))
            #                 # await asyncio.sleep(60)
            #     except Exception as e:
            #         print("Error in get_quote()", str(e))
            #         pass
    except Exception as e:
        print("Error in get_quote()", str(e))
    finally:
        r.close()
        await r.wait_closed()



