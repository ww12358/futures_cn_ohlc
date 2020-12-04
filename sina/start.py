from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import os
import time
import logging
import requests
import pandas as pd
import json
import functools
from concurrent.futures import ThreadPoolExecutor
import concurrent
from datetime import datetime
from sina.getContractDict import getContractDict, getAllContractDict
import nest_asyncio
import numpy as np
from cn.include import all_symbols

nest_asyncio.apply()

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


def download_sina_data(contract):
    try:
        print(contract)
        url = "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract
        data = requests.get(url).content
        data = json.loads(data)
        data = pd.DataFrame(data)
        # print(data.tail(1))
        data = data.sort_values(0)
        data.columns = "date open high low close volume".split()
        data = data.set_index('date')
        data.index = pd.to_datetime(data.index)
        data[["open", "high", "low", "close", "volume"]] = data[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric)

    except Exception as e:
        print("error", str(e))

    return data




def job_function():
    print("Hello World")
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    contract_dict = getAllContractDict()
    # print(contract_dict)
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        # asyncio.get_event_loop()\
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        sched_background = AsyncIOScheduler()
        sched_background.add_job(get_sina5m, "interval", minutes=5, next_run_time=datetime.now(), args=[contract_dict])
        sched_background.start()
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

# asyncio def interval_function():
async def get_sina_contracts(month, contract):
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
        df = await loop.run_in_executor(executor, functools.partial(download_sina_data, contract=contract))
        #     print(df)
        return month, df

async def get_sina5m(contract_dict):
    print("sina!" + datetime.now().strftime("%H:%M:%S"))

    for symbol, contract_d in contract_dict.items():
        try:
            # print(symbol)
            loop = asyncio.get_event_loop()
            group = asyncio.gather(*[get_sina_contracts(month, contract) for month, contract in contract_d.items()])
            results = loop.run_until_complete(group)
            # print(results)
            loop.close

            _, dfs = map(list, zip(*results))
            df_concat = pd.concat(dfs, axis=0)
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_concat)

            df_concat = df_concat[df_concat["volume"] > 12]
            # g = df_concat.groupby(df_concat.index.minute, sort=True)
            g = df_concat.groupby(df_concat.index, sort=True)
            # for date, group in g:
            #     print(date)
            #     print(group)

            df_00 = pd.concat([g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
                                g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
                                g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
                                g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
                                g.apply(lambda x: np.sum(x['volume']))],
                               axis=1, keys=['open', 'high', 'low', 'close', 'volume'])

            df_00["pct"] = df_00["close"].pct_change(axis='rows')
            df_00 = df_00.loc[(df_00.pct < 0.09) & (df_00.pct > -0.09)]
            with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                print(df_00.tail(1))

        except Exception as e:
            print("error", str(e))
            pass

    return

def main():

    # sched_main = BlockingScheduler()
    sched_main = BackgroundScheduler()

    # # Runs from Monday to Friday at 5:30 (am) until
    sched_main.add_job(job_function, 'cron', day_of_week='mon-sun', hour=22, minute=28)
    sched_main.start()

    while True:
        time.sleep(10)


    sched.shutdown()

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

