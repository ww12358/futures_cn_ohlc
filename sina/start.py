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
from datetime import datetime
from sina.getContractDict import getContractDict
import nest_asyncio
from cn.include import all_symbols

nest_asyncio.apply()

logging.basicConfig()
logging.getLogger('apscheduler').setLevel(logging.DEBUG)


def download_sina_data(symbol):
    url = "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + symbol
    data = requests.get(url).content
    data = json.loads(data)
    data = pd.DataFrame(data)
    data = data.sort_values(0)
    data.columns = "date open high low close volume".split()
    data = data.set_index('date')
    data.index = pd.to_datetime(data.index)
    data[["open", "high", "low", "close", "volume"]] = data[["open", "high", "low", "close", "volume"]].apply(
        pd.to_numeric)

    return data




def job_function(symbol):
    print("Hello World")
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    contract_dict = getContractDict(symbol)
    # Execution will block here until Ctrl+C (Ctrl+Break on Windows) is pressed.
    try:
        # asyncio.get_event_loop()\
        new_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(new_loop)
        sched_background = AsyncIOScheduler()
        sched_background.add_job(get_sina5m, "interval", seconds=30, next_run_time=datetime.now(), args=[contract_dict])
        sched_background.start()
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        pass

# asyncio def interval_function():


async def get_sina5m(contract_dict):
    print("sina!"+datetime.now().strftime("%H:%M:%S"))
    _executor = ThreadPoolExecutor(1)

    async def get_sina_contracts(key, symbol):
        loop = asyncio.get_event_loop()
        #     fv_dic = {}
        # for key,symbol in contract_dict.items():
        print(key, symbol)
        df = await loop.run_in_executor(_executor, functools.partial(download_sina_data, symbol=symbol))
        #     print(df)
        #     fv_dic[key] = df

        return key, df

    loop = asyncio.get_event_loop()
    group = asyncio.gather(*[get_sina_contracts(key, smb) for key, smb in contract_dict.items()])
    results = loop.run_until_complete(group)
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

    import numpy as np
    df_new = pd.concat([g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
                        g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
                        g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
                        g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
                        g.apply(lambda x: np.sum(x['volume']))],
                       axis=1, keys=['open', 'high', 'low', 'close', 'volume'])

    df_new["pct"] = df_new["close"].pct_change(axis='rows')
    df_new = df_new.loc[(df_new.pct < 0.09) & (df_new.pct > -0.09)]
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        print(df_new)


    return

def main():

    symbol = "pp"
    symbol = symbol.strip().upper()
    # sched_main = BlockingScheduler()
    sched_main = BackgroundScheduler()

    # # Runs from Monday to Friday at 5:30 (am) until
    sched_main.add_job(job_function, 'cron', day_of_week='mon-sun', hour=9, minute=45, args=[symbol])
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

