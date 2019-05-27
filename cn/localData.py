# -*- coding:utf-8 -*-

import pandas as pd
from .include import DATA_PATH_DICT, all_symbols, cn_headers
import re


class localData:
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol
        self.freq = freq
        self.exchange = exchange
#        self.__h5key = "/" + self.symbol + "/" + self.freq + "/_" + month_seq
        self.__df = {}
        try:
            self.__h5Store = pd.HDFStore(DATA_PATH_DICT[self.symbol])
            self.months = self.get_symbol_months()
            for month in self.months:
                key = ''.join(["/", self.symbol, "/", self.freq, "/_", month])
                self.__df[month] = pd.read_hdf(self.__h5Store, key)
#                print(self.__df[month])
#                if self.exchange == "CZCE":
#                    self.__df[month].drop(["d_oi", "EDSP"], axis=1, inplace=True)
#                    self.__df[month]["pre_close"] = 0
#                    self.__df[month] = self.__df[month][cn_headers]

#                    if self.symbol == "TA":
#                        self.__df[month].reset_index(inplace=True)
#                        self.__df[month]["symbol"] = self.__df[month]["symbol"].str.replace("PTA", "TA")
#                        self.__df[month].set_index(["date", "symbol"], inplace=True)

#                self.__df[month].reset_index(inplace=True)
#                self.__df[month].set_index(["date", "symbol"], inplace=True)
 #               print(self.__df[month])
            print("Local Data loaded successfully! Continue...")

        except Exception as e:
            print("Some error occured during access data file", str(e))
            return

    def __del__(self):
        print("Saving data to disk drive... Please wait.")
        if self.__h5Store:
            self.__h5Store.flush()
            self.__h5Store.close()
            print("Success! Update exit.")
            return
        else:
            return

    def get_data(self, year, month):
        if not year is None:
#            print(year)
#            year_short = year[2:]
            query_str = self.symbol + year + month
            print(query_str)

        try:
            df = self.__df[month]
#            print(df)
            df = df.loc[df.index.get_level_values('symbol') == query_str]
#            df.reset_index(level="symbol", inplace=True)
        except ValueError:
            return None
        except Exception as e:
            print(str(e))

        return df

    def get_symbol_months(self):
        months = []
        # find months list from local hdf5
        for item in self.__h5Store.walk("/" + self.symbol + "/" + self.freq + "/"):
            months_raw = list(item[2])
            months = [(lambda x: x.strip('_'))(x) for x in months_raw]
            if("00" in months):
                months.remove("00")

        return months

    def get_latest_date(self):
        latest_local_date_dic = {}
        months = self.get_symbol_months()
        for month in months:
            df = pd.read_hdf(self.__h5Store, '/' + self.symbol + "/" + self.freq + "/_" + month)
            latest_local_date_dic[month] = df.index.get_level_values("date").max()

        return latest_local_date_dic

    def append_data(self, df_append, exchange, symbol, freq, month):
#        print(df_append)
#        print(self.__df[month])
#        self.__df[month].reset_index(inplace=True)
#        self.__df[month].set_index(["date", "symbol"], inplace=True)
        df_new = self.__df[month].append(df_append, sort=True)
#        df_new.sort_index(level=["date","symbol"], ascending=True, inplace=True)
#        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#            print(df_new.head(10))
#            print(df_new.tail(10))
        df_new.to_hdf(self.__h5Store, '/' + symbol + '/' + freq + '/_' + month, format='table', append=False,
                                  data_columns=True, complevel=9, complib='blosc:snappy', endcoding="utf-8")

        return





