# from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import os
import time
import logging
import pandas as pd
import functools
from concurrent.futures import ThreadPoolExecutor
import concurrent
import datetime
from sina.getContractDict import getContractDict, getAllContractDict
from sina.redis_buffer import store_redis
from sina.download_sina import download_sina_data, download_sina_data_hq
import nest_asyncio
import numpy as np
from sina.include import trading_symbols
from sina.sina_M5_archive import archive_sina_M5

nest_asyncio.apply()

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

DEBUG = 0

def job_function():
    # print("Hello World")
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # date = datetime.now()
    # t = date.time()
    contract_dict = getAllContractDict()
    # print(contract_dict)
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        # asyncio.get_event_loop()\
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        sched_background = AsyncIOScheduler()
        sched_background.add_job(get_sina5m, "interval", minutes=5, next_run_time=datetime.datetime.now(), args=[contract_dict])
        sched_background.add_job(archive_sina_M5, "cron", hour='0-2, 9-11, 13-15, 21-23', minute="2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57", args=[contract_dict])
        # sched_background.add_job(get_sina5m, "interval", minutes=5,
                                 # args=[contract_dict, datetime.datetime.now().time()])
        sched_background.start()
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass


# asyncio def interval_function():
async def get_sina_contracts(contract):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # futures = []
        # for symbol in contract_dict.keys():
        #     futures.append(executor.submit(get_wiki_page_existence, wiki_page_url=url))
        # for future in concurrent.futures.as_completed(futures):
        #     print(future.result())
        loop = asyncio.get_event_loop()
        #     fv_dic = {}
        # for key,symbol in contract_dict.items():
        #print(key, symbol)
        try:
            df = await loop.run_in_executor(executor, functools.partial(download_sina_data_hq, contract=contract))
        # except ValueError as e:
        #     if str(e) == "BUSTERED":
        #         raise ValueError("F!!!")
        #     else:
        #         print(str(e))
        except:
            print("something wrong")
        #     print(df)
        return contract, df

async def get_sina5m(contract_dict):

    # print("sina!" + datetime.now().strftime("%H:%M:%S"))
    # print(t)
    # with trading_symbols(t) as ts:
    # t_symbols = ts.get_t_range()

    t = datetime.datetime.now().time()
    t_symbols = trading_symbols(t)

    if t_symbols is None:
        return

    # print("Downloading below contracts: ", t_symbols)
    # for symbol in contract_dict.keys():
    for symbol in t_symbols:
        try:
            contract_d = contract_dict[symbol]
            print(symbol)
            loop = asyncio.get_event_loop()
            group = asyncio.gather(*[get_sina_contracts(contract) for contract in contract_d.values()])
            results = loop.run_until_complete(group)
            print(results)
            _, dfs = map(list, zip(*results))

            if all(item is None for item in dfs):    #if all dfs items are None
                continue

            df_concat = pd.concat(dfs, axis=0)
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_concat)

            df_concat = df_concat[df_concat["volume"] > 1]
            # print(df_concat)
            # g = df_concat.groupby(df_concat.index.minute, sort=True)
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
                                g.apply(lambda x: np.sum(x['oi']))],
                               axis=1, keys=['open', 'high', 'low', 'close', 'volume', 'oi'])

            # df_00["pct"] = df_00["close"].pct_change(axis='rows')
            # df_00 = df_00.loc[(df_00.pct < 0.09) & (df_00.pct > -0.09)]

            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_00)

            # print(results)
            results.append(tuple(((symbol + '00'), df_00)))
            # print(results)
            res = loop.run_until_complete(store_redis(loop, results))
            # print(res)
            loop.close

        except Exception as e:
            print("error", str(e))
            pass

    return



def main():

    if not DEBUG:

        sched_main = BackgroundScheduler()

        def disable_job_function():
            print("job_disabled")
            sched_main.remove_job('SINA_RETRIEVE_JOB')

        # def enable_interval():
        #     sched_main.add_job(job_function, 'cron', day_of_week='mon-fri', hour=9, minutes=0, second=2, id='SINA_RETRIEVE_JOB')

        # # Runs from Monday to Friday at 5:30 (am) until
        sched_main.add_job(job_function, 'cron', day_of_week='mon-fri', hour=9, minute=0, second=15, id='SINA_RETRIEVE_JOB')
        sched_main.start()
        # sched_main.add_job(disable_job_function, 'cron', day_of_week='tue-sat', hour=2, minute=35, second=0)

        while True:
            time.sleep(10)

        sched.shutdown()

    else:   #debug mode
        job_function()

    return


if __name__ == "__main__":
    # schedule.every().day.at("09:00").do(main)
    #
    # while True:
    #     # Checks whether a scheduled task
    #     # is pending to run or not
    #     schedule.run_pending()
    #     time.sleep(1)
    main()
