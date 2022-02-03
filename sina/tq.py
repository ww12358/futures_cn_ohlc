
# import asyncio
import concurrent.futures
import asyncio
# import nest_asyncio
# nest_asyncio.apply()
import pandas as pd
from sina.redis_buffer import store_redis_tq
import time



# def exec_async(func, *args, **kwargs):
#     with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
#         future = executor.submit(func, *args, **kwargs)
#     return future.result()

async def get_quote(api, contract_tq, contract):
    # quote = api.get_quote(contract)
    quote = await api.get_kline_serial(contract_tq, 60)
    # print(quote)
    async with api.register_update_notify(quote) as channel:
        async for _ in channel:
            # print(contract, quote.datetime, quote.last_price)
            if api.is_changing(quote):
                await store_redis_tq(contract, quote)
            # await asyncio.sleep(60)


# def rg_chnl(api, contract_tq, contract):
#     api.create_task(get_quote(api, contract_tq, contract))

