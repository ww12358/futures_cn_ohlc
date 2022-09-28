# -*- coding:utf-8 -*-
from .include import local_ts_ex_map, symbol_exchange_map
from .contracts import get_list_delist_dates
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from itertools import product
from bisect import bisect_left, bisect_right

def get_start_end_date(df_basics, exchange, symbol, year, month):
#    print(symbol, year, month)
    month_str = year + month

    query_str = symbol + month_str + '.' + local_ts_ex_map[exchange]
#    print(query_str)

    if df_basics.empty:
        return None

    else:
        df_info = df_basics.loc[df_basics['ts_code'] == query_str]
        if df_info.empty:
            return None
        else:
            start_date = df_info.list_date.values[0]
            end_date = df_info.delist_date.values[0]

            return (start_date, end_date)

def get_last_trading_day(exchange, tm, pro):

#    print(exchange)
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


def delist_date(symbol, exchange, year, month, df_basics):
    smbl_str = symbol + year + month + '.' + exchange
#    print(smbl_str)
    try:
        delist_date = df_basics.loc[df_basics["ts_code"] == smbl_str, "delist_date"].values[0]
        return delist_date

    except IndexError:
        print("Contract not listed yet")
        return None


def first_last_contract(list_dates, delist_dates, date, today):
    # print(list_dates, "\n", delist_dates)
    first_delist_date= min(i for i in delist_dates.keys() if (i - date) > pd.to_timedelta(0))   #find the first delist date
    last_list_date=max(i for i in list_dates.keys() if (i - today) < pd.to_timedelta(0))
    first_contract = delist_dates[first_delist_date]
    last_contract = list_dates[last_list_date]
#    print(first_contract, "\t", last_contract)
    return [first_contract[0:2], first_contract[2:]], [last_contract[0:2], last_contract[2:]]

def get_contract_range(contract_combs, list_dates, delist_dates, date, today):
    from bisect import bisect_left
#    print(first_contract)
    first_contract_li, last_contract_li = first_last_contract(list_dates, delist_dates, date, today)
    contract_range = contract_combs[bisect_left(contract_combs, first_contract_li):bisect_right(contract_combs, last_contract_li)]
#    print(contract_list)
#    print(contract_range)
    return contract_range

def normalize_new_data(df):
#    print(df)
    df.reset_index(inplace=True)
    df.set_index(["date", "symbol"], inplace=True)
#    print(df)
#    df.loc[df["volume"] == 0, ["open", "high", "low"]] = df.loc[df["volume"] == 0, ["close"]]
#    df.loc[df["pre_close"] == 0, "pre_close"] = df.loc[df["pre_close"] == 0, "pre_settlement"]
#    print(df)

    return df

def append_data(exchange, symbol, freq, year, month, start_date, end_date, ldata, rdata):
    df = ldata.get_data(year, month)
    ts_month_str = year + month
    # print("local data", df)
    # print(symbol, exchange, start_date, end_date)

#    month_short = yymm[2:]
    df_ts = rdata.get_data(symbol, exchange, freq, ts_month_str, start_date, end_date)
    # print("df_ts", df_ts)
    # print(df.shape)

    i = df_ts.index.size - df.index.size  #
    if i == 0:  # df_open.index.size equals with df.index.size in length
        idx = df_ts.index.difference(df.index)
        if not idx.empty:
            print(idx.values)
            # print(df_open[idx])
        else:
            print("Local data is same with remote, correct!")
    elif i < 0:  # df.index.size > df_open.size:
        idx = df.index.difference(df_ts.index)
        print("%d row(s) of redundant data found." % abs(i))
 #       print(idx.values)

    else:  # df.index.size < df_open.size
        print("%d row(s) of data missing." % i)
        df_append = df_ts[~df_ts.index.isin(df.index)]
#        df.set_index("symbol", append=True, inplace=True)
        print(df_append, "\nAbove data is going to be appended to local data.\n")
        if not df_append.empty:
            ldata.append_data(df_append, month)

    return

def new_data(exchange, symbol, freq, ts_code, start_date, end_date, ldata, rdata):
#    month_short = yymm[2:]
    df_ts = rdata.get_all_data(symbol, exchange, freq, ts_code, start_date, end_date)
#    print(df_ts)

    print(df_ts, "\nAbove data is going to be save to local file.\n")
    # if not df_ts.empty:
    # df_ts.to_hdf(self.__h5Store, '/' + symbol + '/' + freq + '/_' + month, mode='a', format='table', append=False,
    #               data_columns=True, complevel=9, complib='blosc:snappy', endcoding="utf-8")

    return

def update_cn(exchange, symbol, freq, year, month, ldata, rdata, basics_df):
#    year = str(int(year)+2000)
#    year_short = year[2:]
#    ts_month_str = year+month

#    year_4d = str(int(year)+2000)
#    print(year, month)
#    df_local = ldata.get_data(year, month)
#    print(df_local)
#    df_local = get_local_data(symbol, year, month)

    try:
#        print(rdata.basics)
        start_date ,end_date = get_start_end_date(basics_df, exchange, symbol, year, month)
#        print(start_date, end_date)
        append_data(exchange, symbol, freq, year, month, start_date, end_date, ldata, rdata)
#        print(df_local)

        return

    except Exception as e:
#        print("Date not exist. Quit.")
        print(str(e))

        return

def update_cn_latest(exchange, symbol, freq, ldata, rdata, basics_df ):

    last_trading_day = get_last_trading_day(exchange, datetime.now(), rdata.feed)        #last trading day
    year_2, month_2, day_2 = last_trading_day.year, last_trading_day.month, last_trading_day.day
    print("last trading date", last_trading_day)

    months_str = ldata.get_symbol_months()
    # print(months_str)
    latest_local_date_dic = ldata.get_latest_date()
#    print(latest_local_date_dic)
    last_row_date = latest_local_date_dic[min(latest_local_date_dic, key=latest_local_date_dic.get)]
#    print(last_row_date)
    year_1, month_1, day_1 = last_row_date.year, last_trading_day.month, last_row_date.day

    years_str = []
    for i in range(year_1, year_2+2):
        years_str.append(str(i)[2:])

#    print(years_str)
    combs = product(years_str, months_str)
    contract_combs = list(map(list, combs))
#    print(contract_combs)

    list_dates, delist_dates = get_list_delist_dates(symbol, local_ts_ex_map[exchange], contract_combs, basics_df)
#    print(list_dates, "\n")
#    print(delist_dates)

    contracts = get_contract_range(contract_combs, list_dates, delist_dates, last_row_date, last_trading_day)
#    print(contracts)

    if not contracts:
        #emtpy list, no job
        print("All data is updated. Skip job!")
        return
    else:
        for contract in contracts:
            update_cn(exchange, symbol, freq, contract[0], contract[1], ldata, rdata, basics_df)

        return

def newContracts(exchange, symbol, freq, ldata, rdata, basics_df ):
#    print("New Contracts")
#    print(basics_df)
    df_info = basics_df.loc[basics_df['symbol'].str.contains(symbol)]
    # print(df_info)
    for index, row in df_info.iterrows():
#        print(row["ts_code"], row['symbol'], row['list_date'], row['delist_date'])
#         ym = row['symbol'][2:]
        ym = row['ts_code'][2:6]
        # print(ym)
        month = ym[2:]
        # print("contract {}, month {}".format(ym, month))
        df_new = rdata.get_all_data(row["ts_code"], row['list_date'], row['delist_date'], ym)
        print("Below data is going to save to local file...")
        print(df_new)
        ldata.save_contract(df_new, exchange, symbol, freq, month)

    # ldata.print_all()

    return
