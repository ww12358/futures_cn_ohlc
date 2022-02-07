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
from multiprocessing import Process
import datetime
from tqsdk import TqApi, TqAuth
from sina.getContractDict import getContractDict, getAllContractDict
from sina.redis_buffer import store_redis
from sina.download_sina import download_sina_data, download_sina_data_hq
from sina.tq import get_quote
import nest_asyncio
import numpy as np
from sina.include import trading_symbols, DEBUG, RUN_NOW
from sina.sina_M5_archive import archive_sina_M5
from cn.include import symbol_exchange_map

nest_asyncio.apply()

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)

def updt_Kandle(contract_dict):
    return

def job_function():
    # print("Hello World")
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    # date = datetime.now()
    # t = date.time()
    contract_dict = getAllContractDict(debug=DEBUG)
    # print(contract_dict)
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        # asyncio.get_event_loop()\
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        sched_background = AsyncIOScheduler()
        # sched_background.add_job(get_tq_data, "interval", minutes=5, next_run_time=datetime.datetime.now(), args=[contract_dict])
        # asyncio.run(get_tq_data(contract_dict))
        # get_tq_data(contract_dict)
        p = Process(target=get_tq_data, args=(contract_dict,))
        p.daemon=True
        p.start()
        p.join()


        # sched_background.add_job(archive_sina_M5, "cron", hour='0-2,  9-11, 13-15, 21-23', minute="2, 7, 12, 17, 22, 27, 32, 37, 42, 47, 52, 57", args=[contract_dict])
        # sched_background.add_job(archive_sina_M5, "cron", hour='0-2, 9-11, 13-15, 21-23',
        #                          minute="52", args=[contract_dict])
        # sched_background.add_job(get_sina5m, "interval", minutes=5,
                                 # args=[contract_dict, datetime.datetime.now().time()])
        sched_background.start()
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

# async def get_tq_data(contract_dict):
def get_tq_data(contract_dict):
    t = datetime.datetime.now().time()
    t_symbols = trading_symbols(DEBUG, t)

    if t_symbols is None:
        return

    api = TqApi(auth=TqAuth("15381188725", "mancan@07"))
    # print("Downloading below contracts: ", t_symbols)
    # for symbol in contract_dict.keys():
    for symbol in t_symbols:
        try:
            contract_tq = {}
            contract_d = contract_dict[symbol]
            print(symbol)
            exchange = symbol_exchange_map[symbol]

            if exchange in ['SHFE', 'DCE', 'INE']:
                for k, v in contract_d.items():
                    contract_tq[k] = v.lower()
            elif exchange == 'CZCE':
                for k, v in contract_d.items():
                    contract_tq[k] = v[0:2] + v[3:]
                    print(contract_tq[k])

            print(exchange)
            print(contract_d)

            # contract_l = contract_d.values()
            # print(contract_l)
            # contract_l_tq = [exchange+'.'+contract.lower() for contract in contract_l]
            # print(contract_l_tq)

            # get_quote_l(api, contract_l_tq, contract_l)
            # rg_multi_chnls(api, contract_l_tq, contract_l)
            # loop = asyncio.get_event_loop()
            # res = loop.run_until_complete(get_quote_l(api, contract_l_tq, contract_l))
            # group = asyncio.gather(*[rg_chnl(api, exchange+'.'+contract, contract) for contract in contract_d.values()])

            # results = loop.run_until_complete(group)
            # print(results)

            for k, contract in contract_d.items():
                api.create_task(get_quote(api, exchange+'.'+contract_tq[k], contract))

        except Exception as e:
            print("Error in get_tq_data()", str(e))
            pass

    while True:
        api.wait_update()
            # print(res)

            # results = loop.run_until_complete(group)
            # # print(results)
            # _, dfs = map(list, zip(*results))
            #
            # if all(item is None for item in dfs):    #if all dfs items are None
            #     continue
            #
            # df_concat = pd.concat(dfs, axis=0)
            # # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            # #     print(df_concat)
            #
            # df_concat = df_concat[df_concat["volume"] > 1]
            # # print(df_concat)
            # # g = df_concat.groupby(df_concat.index.minute, sort=True)
            # g = df_concat.groupby(df_concat.index, sort=True)
            # # g.apply(print)
            # # for date, group in g:
            # #     print(date)
            # #     print(group)
            #
            # df_00 = pd.concat([g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
            #                     g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
            #                     g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
            #                     g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
            #                     g.apply(lambda x: np.sum(x['volume'])),
            #                     # g.apply(lambda x: np.sum(x['oi'])),
            #                    ],
            #                    axis=1, keys=['open', 'high', 'low', 'close', 'volume',
            #                                  # 'oi'
            #                                  ])
            # df_00['symbol'] = symbol + '0000'
            # results.append(tuple(((symbol + '00'), df_00)))
            # res = loop.run_until_complete(store_redis(loop, results))
            # loop.close

    return

# asyncio def interval_function():
async def get_sina_contracts(contract):
    url_switch = 0

    with concurrent.futures.ThreadPoolExecutor() as executor:
        loop = asyncio.get_event_loop()
        try:
            # df = await loop.run_in_executor(executor, functools.partial(download_sina_data_hq, contract=contract))
            df, switch = await loop.run_in_executor(executor, functools.partial(download_sina_data, contract=contract, url_switch=url_switch))
            url_switch = switch
        # except ValueError as e:
        #     if str(e) == "BUSTERED":
        #         raise ValueError("F!!!")
        #     else:
        #         print(str(e))
        except:
            print("something wrong when running get_sina_contracts")
        #     print(df)
        return contract, df

async def get_sina5m(contract_dict):

    # print("sina!" + datetime.now().strftime("%H:%M:%S"))
    # print(t)
    # with trading_symbols(t) as ts:
    # t_symbols = ts.get_t_range()

    t = datetime.datetime.now().time()
    t_symbols = trading_symbols(DEBUG, t)

    if t_symbols is None:
        return

    # print("Downloading below contracts: ", t_symbols)
    # for symbol in contract_dict.keys():
    for symbol in t_symbols:
        try:
            contract_d = contract_dict[symbol]
            # print(symbol)
            loop = asyncio.get_event_loop()
            group = asyncio.gather(*[get_sina_contracts(contract) for contract in contract_d.values()])
            results = loop.run_until_complete(group)
            # print(results)
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
                                # g.apply(lambda x: np.sum(x['oi'])),
                               ],
                               axis=1, keys=['open', 'high', 'low', 'close', 'volume',
                                             # 'oi'
                                             ])
            df_00['symbol'] = symbol + '0000'
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

    if not RUN_NOW:

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
