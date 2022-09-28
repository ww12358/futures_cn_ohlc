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
from sina.include import trading_symbols, DEBUG, RUN_NOW, watch_list
from sina.redis_buffer import store_redis
from sina.sina_M5_archive import archive_sina_M5
from cn.include import symbol_exchange_map, all_symbols
from sina.include import watch_list
from cn.config import TQ_USER, TQ_PASS

nest_asyncio.apply()

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

def job_function():
    # print("Hello World")
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # date = datetime.now()
    # t = date.time()
    contract_dict = getAllContractDict(debug=DEBUG)
    contract_dict = {key: contract_dict[key] for key in contract_dict.keys() & watch_list}
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
            new_loop.run_until_complete(get_tq_data(contract_dict, new_loop))
            # asyncio.run(get_tq_data(contract_dict, new_loop))
        except:
            new_loop.close()
    except (KeyboardInterrupt, SystemExit):
        pass

def get_tq_data(contract_dict, loop):
    # t = datetime.datetime.now().time()
    # t_symbols = []
    # trading_symbols(DEBUG, t, t_symbols)
    #
    # t_symbols = t_symbols & contract_dict.keys()
    t_symbols = watch_list

    # if t_symbols is None:
    #     return
    print("Downloading below contracts: ", t_symbols)

    tq_contract_dict = {}
    for symbol in t_symbols:
        contract_tq_d = {}
        # if symbol in contract_dict.keys():
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
        # print(tq_contract_dict[symbol])

    # print(tq_contract_dict)

    try:
        api = TqApi(auth=TqAuth(TQ_USER, TQ_PASS))
        api.create_task(get_quote(api, t_symbols, tq_contract_dict, contract_dict))
    except Exception as e:
        print("Error in get_tq_data()", str(e))

    while True:
        api.wait_update()

    return

def main():

    # if not RUN_NOW:
    #
    #     sched_main = BackgroundScheduler()
    #     # def enable_interval():
    #     #     sched_main.add_job(job_function, 'cron', day_of_week='mon-fri', hour=9, minutes=0, second=2, id='SINA_RETRIEVE_JOB')
    #
    #     # # Runs from Monday to Friday at 5:30 (am) until
    #     sched_main.add_job(job_function, 'cron', day_of_week='mon-fri', hour=9, minute=0, second=15, id='SINA_RETRIEVE_JOB')
    #     sched_main.start()
    #     # sched_main.add_job(disable_job_function, 'cron', day_of_week='tue-sat', hour=2, minute=35, second=0)
    #
    #     while True:
    #         time.sleep(10)
    #
    #     sched.shutdown()
    #
    # else:   #debug mode
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
