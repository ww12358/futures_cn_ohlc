# -*- coding:utf-8 -*-

from __future__ import unicode_literals
from shfe.include import SHFE_DATA_PATH, TEST_DATA_PATH, shfe_headers, shfe_dtypes, months, shfe_symbols
import pandas as pd
import tushare as ts
import click
from datetime import datetime, timedelta
import re
import numpy as np

ts.set_token('d0d22ccf30dfceef565c7d36d8d6cefd43fe4f35200575a198124ba5')
pro = ts.pro_api()

def get_local_data(symbol, year, month):
    if not year is None:
        year_short = year[2:]
        query_str = symbol + year_short + month
        print(query_str)

    try:
        with pd.HDFStore(SHFE_DATA_PATH) as f:
            df = pd.read_hdf(f, '/'+symbol+'/D/_'+month)
            if not year is None:
                df = df.loc[df.index.get_level_values('symbol')==query_str]
                df.reset_index(level="symbol", inplace=True)

    except Exception as e:
        print("Local data not exsist. skip")
        return None

    return df


def get_start_end_date(df_basics, symbol, year, month):
#    print(symbol, year, month)
    year_short = year[2:]
    month_str = year_short + month


    if df_basics.empty:
        return None

    else:
        df_info = df_basics.loc[df_basics['symbol'] == symbol + month_str]
        if df_info.empty:
            return None
        else:
            start_date = df_info.list_date.values[0]
            end_date = df_info.delist_date.values[0]

            return (start_date, end_date)


def remove_item(li, item):
    r = list(li)
    r.remove(item)

    return r


def normalize_ts_raw(df_ts, symbol_str, month_str):
    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(df_ts.head(5))
    df_ts.fillna(0, inplace=True)
#    print(df_ts['pre_settle'])

    df_ts.reset_index(inplace=True, drop=True)

    columns = {
        "ts_code": "symbol",
        "trade_date": "date",
        "pre_settle": "pre_settlement",
        "settle": "settlement",
        "change1": "d1",
        "change2": "d2",
        "vol": "volume",
        "amount": "turnover"
    }

    df_ts.rename(columns=columns, inplace=True)
    df_ts.date = pd.to_datetime(df_ts.date, format="%Y%m%d")
    df_ts = df_ts.astype(shfe_dtypes)
    #    df_ts.drop("symbol", axis=1, inplace=True)
    df_ts.symbol = symbol_str + month_str
#    print(df_ts.symbol.dtype)
    df_ts.set_index("date", drop=True, inplace=True)
    df_ts.sort_index(ascending=True, inplace=True)

    df_ts.loc[df_ts["volume"] == 0, ["open", "high", "low"]] = df_ts.loc[df_ts["volume"] == 0, "close"]
    df_ts.loc[df_ts["pre_close"] == 0, "pre_close"] = df_ts.loc[df_ts["pre_close"] == 0, "pre_settlement"]

    ts_headers = remove_item(shfe_headers, "date")
    df_ts = df_ts[ts_headers]

    with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
        print(df_ts.head(5))

    return df_ts




def get_data_ts(symbol, exchange, month_str, start_date, end_date):
    #    print(symbol, exchange, month_str, start_date, end_date)
    #    example of month_str: "1901"
    ts_code = symbol + month_str + '.' + exchange
    #    print(ts_code)
    df_ts = pro.fut_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

#    print(df_ts)

    if not df_ts.empty:
        return normalize_ts_raw(df_ts, symbol, month_str)
    else:
        print("No data from remote")
        return None

def normalize_new_data(df):
    df.set_index("symbol", append=True, inplace=True)
#    df.loc[df["volume"] == 0, ["open", "high", "low"]] = df.loc[df["volume"] == 0, ["close"]]
#    df.loc[df["pre_close"] == 0, "pre_close"] = df.loc[df["pre_close"] == 0, "pre_settlement"]

#    print(df)

    return df


def append_ts_data(df, exchange, symbol, month, start_date, end_date):
    #    print(symbol, exchange, month, start_date, end_date)
    month_short = month[2:]
    df_ts = get_data_ts(symbol, exchange, month, start_date, end_date)
#    print(df_ts)

    i = df_ts.index.size - df.index.size  #
    if i == 0:  # df_open.index.size equals with df.index.size in length
        idx = df_ts.index.difference(df.index)
        if not idx.empty:
            print(idx.values)
            # print(df_open[idx])
        else:
            print("data correct!")
    elif i < 0:  # df.index.size > df_open.size:
        idx = df.index.difference(df_ts.index)
        print("%d row(s) of redundant data found." % abs(i))
 #       print(idx.values)

    else:  # df.index.size < df_open.size
        print("%d row(s) of data missing." % i)
 #       idx = df_ts.index.difference(df.index)
 #       print(idx.values)

#        df_append = df_ts[~df_ts.index.isin(df.index)]
        df_append = normalize_new_data(df_ts[~df_ts.index.isin(df.index)])
#        df.set_index("symbol", append=True, inplace=True)
        print(df_append, "\nAbove data is going to be appended to local data.\n")

        if not df_append.empty:
            with pd.HDFStore(SHFE_DATA_PATH) as f:
                df_old = pd.read_hdf(f, '/' + symbol + '/D/' + '_' + month_short, mode='r')
                df_new = df_old.append(df_append)
                df_new.sort_index(inplace=True)
                df_new.to_hdf(f, '/' + symbol + '/D/' + '_' + month_short, format='table', append=False,
                              data_columns=True, mode='a', endcoding="utf-8")

            return

"""
#        cmd_input("Are you sure to update local data?('Y/yes' or 'N/no')   ")
        timeout = time.time() + 1

        while True:
            answer = input("Are you sure you want to update local data?('Y/yes' or 'N/no', default: 'Y')   ").strip().lower()
            #       print("answer is ", answer)
            if answer in ("yes", "y"):
                if not df_append.empty:
#                    df_new = df.append(df_append)
#                    df_new.sort_index(inplace=True)
#                    print(df_new)
                    with pd.HDFStore(SHFE_DATA_PATH) as f:
                        df_old = pd.read_hdf(f, '/' + symbol + '/D/' + '_' + month_short, mode='r')
                        df_new = df_old.append(df_append)
                        df_new.sort_index(inplace=True)
                        df_new.to_hdf(f, '/' + symbol + '/D/' + '_' + month_short, format='table', append=False,
                                      data_columns=True, mode='a', endcoding="utf-8")
                    break
                else:
                    print("Empty data. Quit...")
                    break
                            if answer in ("no", 'n'):
                print("Data updating aborted.")
                break

            if time.time() > timeout:
                print("Update data automatically...")
 #               print("Time out waiting for prompt. Quit...")
                with pd.HDFStore(SHFE_DATA_PATH) as f:
                    df_old = pd.read_hdf(f, '/' + symbol + '/D/' + '_' + month_short, mode='r')
                    df_new = df_old.append(df_append)
                    df_new.to_hdf(f, '/' + symbol + '/D/' + '_' + month_short, format='table', append=False,
                                  data_columns=True, mode='a', endcoding="utf-8")
                
            time.sleep(1)
"""


def update_shfe(symbol, year, month, df_basic):
    year_short = year[2:]
    ts_month_str = year_short+month

    df_local = get_local_data(symbol, year, month)
    if df_local is None:
        return

    try:
        start_date ,end_date = get_start_end_date(df_basic, symbol, year, month)
        append_ts_data(df_local, "SHF", symbol, ts_month_str, start_date, end_date)

        return

    except Exception as e:
        print("Date not exsist. Quit.")
#        print(str(e))

        return



def year_to_string(y):
    if 1 <= y <=9:
        return "0"+str(y)
    elif 10 <= y <= 99:
        return str(y)
    else:
        print("Error year specified")
        return None


def monthlist_till_today(start, end):
#    start = datetime.strptime(start_date, "%Y-%m-%d")
#    end = datetime.now()
    total_months = lambda dt: dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start)-1, total_months(end)+12):
        y, m = divmod(tot_m, 12)
#        mlist.append(datetime(y, m+1, 1).strftime("%y%m"))
        if m in range(0, 9):
            mlist.append((str(y), '0'+str(m+1)))
        else:
            mlist.append((str(y), str(m+1)))

    #could be improved with delist_date, avoid last month try. to be continued...
    return mlist


def delist_date(symbol, exchange, year, month, df_basics):
    smbl_str = symbol + year + month + '.' + exchange
    print(smbl_str)
    try:
        delist_date = df_basics.loc[df_basics["ts_code"] == smbl_str, "delist_date"].values[0]
        return delist_date

    except IndexError:
        print("Contract not listed yet")
        return None


def first_last_trd_day(symbol, df_basics):
    pat = re.escape(symbol) + r'\d{4}'
    dfa = df_basics.loc[df_basics["symbol"].str.match(pat)].copy()
    dfa.loc[:, "list_date"] = pd.to_datetime(dfa["list_date"].str.strip(), format="%Y%m%d")
    dfa.loc[:, "delist_date"] = pd.to_datetime(dfa["delist_date"].str.strip(), format="%Y%m%d")
    dfa = dfa.sort_values(by="list_date")
#    print(dfa)
    first_day = dfa["list_date"].min()
    last_day = dfa["delist_date"].max()

    return first_day, last_day


def get_last_trading_day(exchange, tm):

    end_date = tm.strftime("%Y%m%d")
#    print(end_date)

    df = pro.trade_cal(exchange=exchange, start_date='20080101', end_date=end_date,
                       fields='exchange,cal_date,is_open,pretrade_date')
    df = df.loc[df['is_open'] == 1]

    if end_date in df["cal_date"].values:
        hr = tm.hour
#        print(hr)
        if hr < 16:  # during trading hour, remote data might not be updated
            # this day is trading day
            tm = tm.replace(hour=0, minute=0, second=0, microsecond=0)  # round down to 0:00 AM
            tm = tm - timedelta(days=1)
#            print(tm)
    else:
        # market is closed on
        # do nothing
        print("Market is closed.")

    df['cal_date'] = pd.to_datetime(df['cal_date'])

 #   print(df)
    s = tm - df['cal_date']
#    print(s)
    closest = s[s >= pd.to_timedelta(0)].idxmin()
#    print("closest", closest)
    last_trd_day = df.loc[[closest]]["cal_date"].values[0]

    dt = (last_trd_day - np.datetime64('1970-01-01T00:00:00Z')) / np.timedelta64(1, 's')

    return datetime.utcfromtimestamp(dt)



def is_up_to_date(a, b):
    return a == b

def get_symbol_months(symbol, f):
    months = []
    #find months list from local hdf5
    for item in f.walk("/" + symbol + "/D/"):
        months_raw = list(item[2])
        months = [(lambda x: x.strip('_'))(x) for x in months_raw]

    return months


def update_shfe_latest(symbol, df_basics):
    months = []
    today = datetime.today()
    last_tdate = get_last_trading_day("SHFE", datetime.now())        #last trading day
    print("last trading date", last_tdate)
    latest_local_date_dic = {}
    this_year = today.year

    #find dates of latest data from local hdf5 groups
    with pd.HDFStore(SHFE_DATA_PATH) as f:
        months = get_symbol_months(symbol, f)
        for month in months:
            df = pd.read_hdf(f, '/' + symbol + "/D/_" + month)
            latest_local_date_dic[month] = df.index.get_level_values("date").max()
#        print(latest_local_date_dic)

    m_li = []
    for month in latest_local_date_dic.keys():
        print("-----------------------")
        start_date = latest_local_date_dic[month]
        start_year = start_date.year

        for year in range(start_year, this_year+2):
            d_date_str = delist_date(symbol, "SHF", str(year)[2:], month, df_basics)
            if d_date_str is None:
                continue
            else:
                d_date = datetime.strptime(d_date_str, "%Y%m%d")
                latest_local_date = latest_local_date_dic[month]
                print("delist date", d_date)
                print("latest local data:", latest_local_date)

                if d_date == latest_local_date and d_date <= last_tdate:
                    print("History data", symbol+str(year)+month, "is updated. Skip")
                elif latest_local_date == last_tdate and last_tdate <= d_date:
                    print("listing contract", symbol+str(year)+month, "is updated. Skip")
                elif latest_local_date < d_date:
#                    update_shfe(symbol, str(year), month, df_basics)
                    print(symbol, str(year), month, "should be updated!")
                    m_li.append((str(year), month))
                elif d_date < latest_local_date:
                    print("History data is updated. Skip")
                else:
                    print("Any thing else????")

    if not m_li:
        #emtpy list, no job
        print("All data is updated. Skip job!")
        return
    else:
        print(m_li)
        for (y, m) in m_li:
            update_shfe(symbol, y, m, df_basics)

        return


def clean_local_data(df):
    df.fillna(0, inplace=True)
    df.loc[df["volume"] == 0, ["open", "high", "low"]] = df.loc[df["volume"] == 0, "close"]
    df.loc[df["pre_close"] == 0, "pre_close"] = df.loc[df["pre_close"] == 0, "pre_settlement"]

    return


def cleanse_shfe_data(symbol):
    with pd.HDFStore(SHFE_DATA_PATH) as f:

        months = get_symbol_months(symbol, f)
        if "00" in months:
            months.remove("00")

        for month in months:
#            print('/'+symbol+"/D/_"+month)
            df = pd.read_hdf(f, '/'+symbol+"/D/_"+month)
            clean_local_data(df)
            df.to_hdf(f, '/' + symbol + '/D/_' + month, format='table', append=False,
              data_columns=True, mode='a', endcoding="utf-8")

    return


@click.command()
@click.option("--symbol", "-s",
              type=click.STRING,
              help='The symbol of a trading contract, or "all" is required for command')
@click.option("--year", "-y", type=click.INT, help="Year")
@click.option("--month", "-m", type=click.STRING, help="month")
@click.option("--latest", "-u", is_flag=True, help="up-to-date")
@click.option("--clean", "-c", is_flag=True, help="Clean data")
def main(symbol, year, month, latest, clean):
    start_date = "2008-01-01"
    df_basics = pro.fut_basic(exchange='SHFE', fut_type='1', fields='ts_code,symbol,list_date,delist_date')
#    print(symbol, year, month)
    this_year = datetime.now().year
    this_month = datetime.now().month
    now = datetime.now()
    if symbol:
        symbol = symbol.upper()
        if symbol in shfe_symbols:
#        print("good symbol")
            if latest:
                print("Updating %s to latest..." % symbol)
                update_shfe_latest(symbol, df_basics)
                return

            if clean:
                print("Cleaning %s data..." % symbol)
                cleanse_shfe_data(symbol)
                print("Data cleaning finished with success! Done.")
                return

            if year and 8 <= year <= (this_year - 1999):
                year_s = str(year + 2000)
                #        print("good year")
                #        print(year_s)
                if month in months:
                    update_shfe(symbol, year_s, month, df_basics)
                    return

                elif month is None:
                    for m in months:
                        update_shfe(symbol, year_s, m, df_basics)
                    return

            elif year is None:
                print("all years")
                if month in months:
                    for y in range(2000 + year, this_year + 1):
                        update_shfe((symbol, str(y), month), df_basics)
                    return

                elif month is None:  # update all data till today
                    while True:
                        answer = input(
                            "Are you sure you want to update ALL local data?('Y/yes' or 'N/no')   ").strip().lower()
                        #       print("answer is ", answer)
                        if answer in ("yes", "y"):
                            first_list_day, last_delist_day = first_last_trd_day(symbol, df_basics)
                            mlist = monthlist_till_today(first_list_day)
                            #                    print(mlist)
                            for ele in mlist:
                                #                        print(ele[0], ele[1])
                                update_shfe(symbol, ele[0], ele[1], df_basics)
                            return

                        else:
                            return


        if symbol == "ALL":
            if latest:
                for smbl in shfe_symbols:
                    update_shfe_latest(smbl, df_basics)
                return

            if clean:
                for smbl in shfe_symbols:
                    cleanse_shfe_data(smbl)

                print("Data cleaned with success.")
                return

        else:
            print("Wrong symbol. Quit")
            return




if __name__ == '__main__':
    main()