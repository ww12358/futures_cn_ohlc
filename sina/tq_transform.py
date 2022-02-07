import pandas as pd
import click
import asyncio
import aioredis
import json
from datetime import datetime, timedelta
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from cn.include import all_symbols

from sina.include import CONTRACT_INFO_PATH
from sina.kMem import gen_idx
import warnings
import nest_asyncio
nest_asyncio.apply()

warnings.filterwarnings("ignore", category=DeprecationWarning)

# watch_list = ["CU", "RB", "I", "A", "M", "Y", "TA", "SR", "CF", "AL", "ZC"]
# watch_list = ["CU", "P", "ZC", "SC"]
watch_list = ["SC", 'CU', 'P', 'SR', 'RU']

def round_by_five(time):
    if time.second == 0 and time.microsecond == 0 and time.minute % 5 == 0:
        return time
    minutes_by_five = time.minute // 5
    # get the difference in times
    diff = (minutes_by_five + 1) * 5 - time.minute
    time = (time + timedelta(minutes=diff)).replace(second=0, microsecond=0)
    return time

def ohlcsum(data):
    if data.empty:
        return data
    #     df = df.sort_index()
    else:
        return pd.DataFrame({
            'open': data['open'].iloc[0],
            'high': data['high'].max(),
            'low': data['low'].min(),
            'close': data['close'].iloc[-1],
            'volume': data['volume'].sum(),
            'contract': data['contract']
        }, index=data.index)

async def load_symbol(symbol_li, freq):
    print(datetime.datetime.now())
    try:
        with open(CONTRACT_INFO_PATH) as f:
            cInfo_j = json.load(f)

        loop = asyncio.get_event_loop()
        r = await aioredis.create_redis_pool(
            "redis://127.0.0.1", minsize=5, maxsize=10, loop=loop, db=1
        )
        group = asyncio.gather(*[gen_idx(sym, cInfo_j[sym], freq, r, loop) for sym in symbol_li])
        results = loop.run_until_complete(group)
        # loop.close()
    except Exception as e:
        print(str(e))
        r.close()
        await r.wait_closed()
    finally:
        r.close()
        await r.wait_closed()
        # loop.close()

@click.command()
@click.option("--symbol", "-S", type=click.STRING, help="contract symbol")
@click.option("--rebuild", "-R", is_flag=True, help="rebuild all data rows")
@click.option("--all", "-A", is_flag=True, help="download all data")
@click.option("--major", "-M", is_flag=True, help="download important data only")
@click.option("--freq", "-F",  type=click.STRING, help="freqency inlcude 5min, 15min, 30min, 1h, 3h, 1d")

def main(all, major, symbol, freq, rebuild=False):
    # print(all_symbols)
    schdlr = AsyncIOScheduler()

    if all:
        smb_li = all_symbols
        # asyncio.run(load_symbol(all_symbols))

    elif major:
        smb_li = watch_list
        # asyncio.run(load_symbol(watch_list, freq))

    schdlr.add_job(load_symbol, "interval", minutes=5, next_run_time=round_by_five(datetime.now()), args=[smb_li, '1min'])

    schdlr.add_job(load_symbol, "cron",
                   hour='0-2,  9-11, 13-15, 21-23',
                   minute="0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55",
                   second="35",
                   args=[smb_li, '5min'])

    schdlr.add_job(load_symbol, "cron",
                   hour='0-2,  9-11, 13-15, 21-23',
                   minute="0, 15, 30, 45",
                   second="45",
                   args=[smb_li, '15min'])

    schdlr.add_job(load_symbol, "cron",
                   hour='0-2,  9-11, 13-15, 21-23',
                   minute="0, 30",
                   second="50",
                   args=[smb_li, '30min'])

    schdlr.add_job(load_symbol, "cron",
                   hour='0-2, 9-11, 13-15, 21-23',
                   minute="0",
                   second="55",
                   args=[smb_li, '1h'])

    schdlr.add_job(load_symbol, "cron",
                   hour='1, 9, 13, 17',
                   minute="3",
                   second="42",
                   args=[smb_li, '4h'])

    schdlr.start()
    asyncio.get_event_loop().run_forever()
    # else:
    #     symbol = symbol.strip().upper()
        # for smbl in all_symbols:
        #     # trans_freq(smbl, freq, rebuild)
        #     print(smbl)
        # if symbol in all_symbols:
        #     # trans_freq(symbol, freq, rebuild)
        #     print(symbol)
        #     return
        # else:
        #     print("not a valid symbol")
        # return




if __name__ == "__main__":
    main()
    # asyncio.run(main())
    # loop = asyncio.get_event_loop()
    # task = loop.create_task(main())
    # loop.run_until_complete(task)
