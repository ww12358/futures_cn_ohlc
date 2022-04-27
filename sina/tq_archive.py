import asyncio
import nest_asyncio
import aioredis
import pyarrow as pa
import datetime
from sina.include import watch_list, REDIS_SVR_ADDR, REDIS_DB, REDIS_PORT, all_freq
from sina.tq_mh import tq_mh
from cn.include import symbol_exchange_map, all_exchanges
from cn.updateCN import get_last_trading_day
from sina.redis_buffer import update_redis
from sina.getContractDict import getContractDict, getAllContractDict
import nest_asyncio
import pandas as pd

import tushare as ts
ts.set_token('d0d22ccf30dfceef565c7d36d8d6cefd43fe4f35200575a198124ba5')
pro = ts.pro_api()
nest_asyncio.apply()

DEBUG = 0

async def get_redis_buffer(r, month, ptn, no_print=True):
    try:
        buf = await r.get(ptn)
        df = pa.deserialize(buf)
        if month != '00':
            df['contract'] = ptn

        df.dropna(inplace=True)
        if not no_print:
            print(ptn, '\n', df)
    except Exception as e:
        print("Error while saving ".format(ptn), str(e))
        if month == '00':
            df = pd.DataFrame(columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        else:
            df = pd.DataFrame(columns=['datetime', 'open', 'high', 'low', 'close', 'volume', 'oi', 'contract'])

        df.set_index('datetime', inplace=True)

    return (month, df, ptn)



async def archive_tq(watch_list):

    if DEBUG:
        watch_list = ["CU"]
        # watch_list = ["CU", "FG", "SC", "B"]
        # REDIS_SVR_ADDR = "127.0.0.1"

    contract_dict = getAllContractDict(debug=DEBUG)
    contract_dict = {key: contract_dict[key] for key in contract_dict.keys() & watch_list}

    tm = datetime.datetime.now()
    dt = tm.replace(hour=15, minute=30, second=0)     # setup local time as last trading day in case tushare is not available
    dt_dict = {}
    for ex in all_exchanges:
        try:
            dt = get_last_trading_day(ex, tm, pro)
            dt = dt.replace(hour=15, minute=30, second=0)
            dt_dict[ex] = dt
        except:
            print("tushare is currrently not available. use local time.", ex, dt)
            pass
    # print(dt_dict)

    loop = asyncio.get_event_loop()
    # r = await aioredis.create_redis_pool(
    #     "redis://" + REDIS_SVR_ADDR, minsize=5, maxsize=20, loop=loop, db=REDIS_DB
    # )
    r = await aioredis.Redis.from_url(
        "redis://" + REDIS_SVR_ADDR, max_connections=10 * len(watch_list), db=REDIS_DB, decode_responses=False
    )
    for symbol in watch_list:
        print(symbol)
        ex = symbol_exchange_map[symbol]
        dt = dt_dict[ex]
        idx_ptn_li = []
        for freq in all_freq:
            print(freq)
            with tq_mh(symbol, freq) as h5:
                if freq == "1min":
                    contracts_d = contract_dict[symbol]

                    grp = asyncio.gather(
                        *[get_redis_buffer(r, k, v) for k, v in contracts_d.items()]
                    )
                    result = loop.run_until_complete(grp)

                    for month, df, contract in result:
                        print(month)
                        df_archive = df.loc[df.index <= dt]  # dataframe to save to local hdf5
                        # df_local = h5.get_contract_by_month(month)
                        #     print(month, '\n', 'local:', df_local.info(), '\n', 'append:', df.info(), '\n', df)
                        h5.append_data(df_archive, month, debug=DEBUG)
                        df_buf = df.loc[df.index > dt]  # dataframe shrinked to save to redis buffer
                        df_buf = df_buf.drop(['contract'], axis=1, inplace=False)
                        # print(df_buf)
                        task1 = loop.create_task(update_redis(r, contract, df_buf))
                        await task1

                # index_grp = await
                idx_ptn = symbol + '00_' + freq
                idx_ptn_li.append(idx_ptn)
                task2 = loop.create_task(get_redis_buffer(r, '00', idx_ptn, no_print=True))
                _, df, contract = await task2
                #     df = await get_redis_buffer(r, '00', ptn, no_print=False)
                df_archive = df.loc[df.index <= dt]
                h5.append_data(df_archive, '00', debug=DEBUG)
                df_buf = df.loc[df.index > dt]
                # print(contract, df_archive, df_buf)
                task3 = loop.create_task(update_redis(r, contract, df_buf))
                await task3

    await r.close()

    # archive_tq()

if __name__ == "__main__":
    asyncio.run(archive_tq(watch_list))