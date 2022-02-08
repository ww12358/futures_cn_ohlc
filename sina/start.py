# from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import aioredis
import os
import time
import logging
import pandas as pd
import functools
from concurrent.futures import ThreadPoolExecutor
import concurrent
from multiprocessing import Process
import multiprocessing as mp
import datetime
from tqsdk import TqApi, TqAuth
from sina.getContractDict import getContractDict, getAllContractDict
from sina.redis_buffer import store_redis
from sina.download_sina import download_sina_data, download_sina_data_hq
from sina.tq import get_quote
import nest_asyncio
import numpy as np
from sina.include import trading_symbols, DEBUG, RUN_NOW
from sina.redis_buffer import store_redis
from sina.sina_M5_archive import archive_sina_M5
from cn.include import symbol_exchange_map

nest_asyncio.apply()

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

def job_function():
    # print("Hello World")
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # date = datetime.now()
    # t = date.time()
    contract_dict = getAllContractDict(debug=DEBUG)
    # print(contract_dict)
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        # asyncio.get_event_loop()
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        # sched_background = AsyncIOScheduler()
        # sched_background.add_job(get_tq_data, "interval", minutes=5, next_run_time=datetime.datetime.now(), args=[contract_dict])
        # asyncio.run(get_tq_data(contract_dict))
        # await get_tq_data(contract_dict)

        # get_tq_data(contract_dict, api, new_loop)
        # asyncio.run(get_tq_data(contract_dict, api))
        try:
            # get_tq_data(contract_dict, new_loop)
            new_loop.run_until_complete(get_tq_data(contract_dict, new_loop))
            # asyncio.run(get_tq_data(contract_dict, new_loop))
            # pending = asyncio.Task.all_tasks()
            # new_loop.run_until_complete(asyncio.gather(*pending))
        except:
            new_loop.close()


        # p = Process(target=get_tq_data, args=(contract_dict,))
        # p.daemon=True
        # p.start()
        # p.join()

        # pool = mp.Pool(mp.cpu_count())
        # for i in range(0, 3):
        #     pool.apply_async(get_tq_data, args=(contract_dict))
        #
        # pool.close()
        # pool.join()

        # sched_background.add_job(archive_sina_M5, "cron", hour='0-2,  9-11, 13-15, 21-23', minute="2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57", args=[contract_dict])
        # sched_background.add_job(archive_sina_M5, "cron", hour='0-2, 9-11, 13-15, 21-23',
        #                          minute="52", args=[contract_dict])
        # sched_background.add_job(get_sina5m, "interval", minutes=5,
                                 # args=[contract_dict, datetime.datetime.now().time()])
        # sched_background.start()
        # asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

# async def run_update(loop, results, event):
#     while True:
#         asyncio.sleep(1)
#         if event.is_set():
#             store_redis(loop, results)

# async def get_tq_data(contract_dict):
def get_tq_data(contract_dict, loop):
    t = datetime.datetime.now().time()
    t_symbols = trading_symbols(DEBUG, t)

    if t_symbols is None:
        return
    # print("Downloading below contracts: ", t_symbols)
    # for symbol in contract_dict.keys():
    tq_contract_dict = {}
    for symbol in t_symbols:
        contract_tq_d = {}
        contract_d = contract_dict[symbol]
        print(symbol)
        exchange = symbol_exchange_map[symbol]

        if exchange in ['SHFE', 'DCE', 'INE']:
            for k, v in contract_d.items():
                contract_tq_d[k] = exchange + '.' + v.lower()
        elif exchange == 'CZCE':
            for k, v in contract_d.items():
                contract_tq_d[k] = exchange + '.' + v[0:2] + v[3:]

        tq_contract_dict[symbol] = contract_tq_d

    try:
        api = TqApi(auth=TqAuth("15381188725", "mancan@07"))
        # results = {}
        results = asyncio.Queue()
        event = asyncio.Event()
        # for symbol in t_symbols:
        #     try:
        #         contract_tq = {}
        #         contract_d = contract_dict[symbol]
        #         print(symbol)
        #         exchange = symbol_exchange_map[symbol]
        #
        #         if exchange in ['SHFE', 'DCE', 'INE']:
        #             for k, v in contract_d.items():
        #                 contract_tq[k] = v.lower()
        #         elif exchange == 'CZCE':
        #             for k, v in contract_d.items():
        #                 contract_tq[k] = v[0:2] + v[3:]
        #                 print(contract_tq[k])
        #
        #         print(exchange)
        #         print(contract_d)
        #         # q = asyncio.Queue()
        #
        #         for k, contract in contract_d.items():
        #             api.create_task(get_quote(api, exchange+'.'+contract_tq[k], contract, q))
        #         results[symbol] = q
                # pending = asyncio.all_tasks()
                # print(pending)
                # group = asyncio.gather(*[api.create_task(get_quote(api, exchange+'.'+contract_tq[k], contract))for k, contract in contract_d.items()])
                # tasks[symbol] = group
                # loop = asyncio.get_event_loop()
                # results = api._loop.run_until_complete(group)
                # print(results)

        api.create_task(get_quote(api, t_symbols, tq_contract_dict, contract_dict))

        # try:
        #     for symbol in t_symbols:
        #         result = results[symbol]
        #         # print(result)
        #         if len(result) == 0:
        #             pass
        #         else:
        #             loop.run_until_complete(store_redis(loop, result))
        #             results[symbol].clear()
        # except Exception as e:
        #     print("Error after api.wait_update(), for loop. ", str(e))
        # loop.close()
    except Exception as e:
        print("Error in get_tq_data()", str(e))

    # finally:
    #     r.close()
        # await r.wait_closed()

    while True:
        api.wait_update()
        # print("hello")
        # print(results['CU'])
        # loop = asyncio.get_event_loop()
        # try:
        #     for symbol in t_symbols:
        #         result = results[symbol]
        #         # print(result)
        #         if len(result) == 0:
        #             pass
        #         else:
        #             loop.run_until_complete(store_redis(loop, result))
        #             results[symbol].clear()
        # except Exception as e:
        #     print("Error after api.wait_update(), for loop. ", str(e))

    return

def main():

    if not RUN_NOW:

        sched_main = BackgroundScheduler()
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
