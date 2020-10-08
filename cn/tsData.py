# -*- coding:utf-8 -*-
import tushare as ts
import pandas as pd
import time
from shfe.include import shfe_dtypes, shfe_headers
from .include import local_ts_ex_map



def remove_item(li, item):
    r = list(li)
    r.remove(item)

    return r


class tsData:
    def __init__(self, ts_pro, exchange, symbol, freq):
        self.name = "tushare"
        self.symbol = symbol.strip().upper()
        self.feed = ts_pro
        self.exchange = exchange
        self.freq = freq
        return

    def normalize_ts_raw(self, df_ts, exchange, symbol_str, month_str):
#        with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#            print(df_ts.head(5))
        if exchange in ["SHFE", "DCE", "CZCE", "INE", "CFFEX"]:
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
            df_ts = df_ts[shfe_headers]
            df_ts = df_ts.astype(shfe_dtypes)
            #    df_ts.drop("symbol", axis=1, inplace=True)
            df_ts.symbol = symbol_str + month_str
#            print(df_ts.symbol)
            df_ts.set_index(["date", "symbol"], drop=True, inplace=True)
            df_ts.sort_index(ascending=True, inplace=True)

            df_ts.loc[df_ts["close"] == 0, "close"] = df_ts.loc[df_ts["close"] == 0, "pre_settlement"]
            df_ts.loc[df_ts["volume"] == 0, ["open", "high", "low"]] = df_ts.loc[df_ts["volume"] == 0, "close"]
            df_ts.loc[df_ts["pre_close"] == 0, "pre_close"] = df_ts.loc[df_ts["pre_close"] == 0, "pre_settlement"]

#            with pd.option_context('display.max_rows', None, 'display.max_columns', None):  # more options can be specified also
#                print(df_ts.head(5))

#            ts_headers = remove_item(shfe_headers, "date")

        return df_ts


    def get_data(self, symbol, exchange, freq, month_str, start_date, end_date):
#       print(self.symbol, exchange, month_str, start_date, end_date)
        #    example of month_str: "1901"
        ts_code = self.symbol + month_str + '.' + local_ts_ex_map[self.exchange]
#        print(ts_code)
        df_ts = self.feed.fut_daily(ts_code=ts_code, start_date=start_date, end_date=end_date)

        time.sleep(1)
#        print(df_ts)

        if not df_ts.empty:
            return self.normalize_ts_raw(df_ts, self.exchange, self.symbol, month_str)
        else:
            print("No data from remote")
            return None

    def get_all_data(self, ts_code, start_date, end_date, month_str):

        df_ts = self.feed.fut_daily(ts_code=ts_code,  start_date=start_date, end_date=end_date)
        if not df_ts.empty:
            return self.normalize_ts_raw(df_ts, self.exchange, self.symbol, month_str)
        else:
            print("No data from remote")
            return None