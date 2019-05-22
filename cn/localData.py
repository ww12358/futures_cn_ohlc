# -*- coding:utf-8 -*-

import pandas as pd
from .include import DATA_PATH_DICT, all_symbols



class localData:
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol
        self.freq = freq
        self.exchange = exchange
#        self.__h5key = "/" + self.symbol + "/" + self.freq + "/_" + month_seq
        try:
            if self.symbol in all_symbols:
                self.__h5Store = pd.HDFStore(DATA_PATH_DICT[self.symbol])
#                self.__df = pd.read_hdf(self.__h5Store, self.__h5key)
                print("Data read successfully.")
            else:
                print(symbol, "\tNot a valid symbol. Quit")
                return

        except Exception as e:
            print("Some error occured during access data file", str(e))
            return

    def __del__(self):
        print("Destructor called.")
        if self.__h5Store:
            self.__h5Store.flush()
            self.__h5Store.close()
            return
        else:
            return

    def get_data(self, year, month):
        if not year is None:
#            year_short = year[2:]
            query_str = self.symbol + year + month
            print(query_str)

        try:
            df = pd.read_hdf(self.__h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month)
            print(df)
            df = df.loc[df.index.get_level_values('symbol') == query_str]
            df.reset_index(level="symbol", inplace=True)
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
        df_old = pd.read_hdf(self.__h5Store, '/' + symbol + '/D/' + '_' + month, mode='r')
        df_new = df_old.append(df_append)
        df_new.sort_index(inplace=True)
        with pd.option_context('display.max_rows', 100, 'display.max_columns', None):
            print(df_new)
#        df_new.to_hdf(self.__h5Store, '/' + symbol + '/' + freq + '/_' + month, format='table', append=True,
#                                  data_columns=True, mode='a', endcoding="utf-8")

        return

"""
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

    def get_symbol_months(self):
        months = []
        # find months list from local hdf5
        for item in self.__h5Store.walk("/" + self.symbol + "/D/"):
            months_raw = list(item[2])
            months = [(lambda x: x.strip('_'))(x) for x in months_raw]

        return months

class scrnOutput(localData):

"""





