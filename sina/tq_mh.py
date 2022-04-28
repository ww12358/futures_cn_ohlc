from sina.include import TQ_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map
import pandas as pd
import numpy as np
import os

class tq_mh(h5_store):
    def __init__(self, symbol, freq):
        self.symbol = symbol.upper()
        # self.exchange = exchange.upper()
        self.exchange = symbol_exchange_map[symbol]
        self.h5_path = "".join([TQ_PATH, freq, "/", self.exchange, "/", symbol, ".hdf5"])

        # super(tq_mh, self).__init__(symbol, freq)
        # self.symbol = symbol
        self.freq = freq
        # self.exchange = exchange
        # self.exchange = symbol_exchange_map[symbol]
        self.df = {}
        self.__isempty = True
        # print(self.h5_path)
        try:
            #            print(DATA_PATH_DICT[self.symbol])
            self.h5Store = pd.HDFStore(self.h5_path)
            #            print(DATA_PATH_DICT[self.symbol])
            if not os.path.exists(self.h5_path):
                print("file not exists, will create new file")

            if not symbol in self.h5Store:
                print("symbol not found in local file, will save new contracts to local.\n")
                return

            else:  # load local file storage to instance
                self.months = self.get_symbol_months_with_idx()

                for month in self.months:
                    key = ''.join(["/", self.symbol, "/_", self.freq, "/_", month])
                    #                print(key)
                    if (d := pd.read_hdf(self.h5Store, key)) is not None:
                        self.df[month] = d
                self.set_notempty()
        except Exception as e:
            print("Some error occured during access data file:\t", str(e))
            return

    def get_data(self, year, month):
        if not year is None:
            #            print(year)
            #            year_short = year[2:]
            query_str = self.symbol + year + month
            # print(query_str)

        try:
            df = self.df[month]
            # print(df)
            df = df.loc[df.contract == query_str]
        #            df.reset_index(level="symbol", inplace=True)
        except ValueError:
            return None
        except Exception as e:
            print(str(e))

        return df

    def get_symbol_months(self):
        try:
            months = []
            # find months list from local hdf5
            for item in self.h5Store.walk("/" + self.symbol + "/_" + self.freq + "/"):
                #               print(item)
                months_raw = list(item[2])
                months = [(lambda x: x.strip('_'))(x) for x in months_raw]
                if ("00" in months):  # do not update vw_idx
                    months.remove("00")
            return months

        except Exception as e:
            print(str(e))
            return None

    def get_symbol_months_with_idx(self):
        try:
            months = []
            # find months list from local hdf5
            for item in self.h5Store.walk("/" + self.symbol + "/_" + self.freq + "/"):
                # print(item)
                months_raw = list(item[2])
                months = [(lambda x: x.strip('_'))(x) for x in months_raw]

        except Exception as e:
            print(str(e))

        return months

    def append_data(self, df_append, month, debug=False):
        try:
            self.df[month]
            # df_append = df_append.loc[df_append.index.get_level_values("date") > self.df[month].index.get_level_values("date")[-1]]
            df_append = df_append.loc[df_append.index > self.df[month].iloc[-1].name]
            df_append.sort_index(ascending=True, inplace=True)
            if debug:
                with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                    print(df_append.head(10))
                    print(df_append.tail(50))
            else:
                df_append.to_hdf(self.h5Store, '/' + self.symbol + '/_' + self.freq + '/_' + month, mode='a', format='table', append=True,
                                 data_columns=True, complevel=9, complib='blosc:snappy')
        except KeyError:
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_append.head(10))
            #     print(df_append.tail(50))
            df_append.sort_index(ascending=True, inplace=True)
            df_append.to_hdf(self.h5Store, '/' + self.symbol + '/_' + self.freq + '/_' + month, mode='a', format='table',
                             append=False, data_columns=True, complevel=9, complib='blosc:snappy')
        except Exception as e:
            print("Error while appending data...", str(e))

        self.df[month] = df_append

        return

    def insert_data(self, df_insert, month, trustNew=True):
        try:
            self.df[month]
            if trustNew:
                print(self.df[month])
                df_origin = self.df[month].loc[self.df[month].index.get_level_values("date") > df_insert.index.get_level_values("date")[-1]]
                df_new = pd.concat([df_insert, df_origin], axis=0, join='inner')
            else:
                df_insert = df_insert.loc[df_insert.index.get_level_values("date") < self.df[month].index.get_level_values("date")[0]]
                df_new = pd.concat([df_insert, self.df[month]], axis=0, join='inner')
            # df_append = df_append.loc[df_append.index > self.df[month].iloc[-1].name]
            df_new.sort_index(ascending=True, inplace=True)
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_new.head(10))
            #     print(df_new.tail(100))
            # print(df_new.info())
            df_new.to_hdf(self.h5Store, '/' + self.symbol + '/_' + self.freq + '/_' + month, mode='a', format='table', append=False,
                             data_columns=True, complevel=9, complib='blosc:snappy')
            self.df[month] = df_new
        except KeyError:
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_insert.head(10))
            #     print(df_insert.tail(50))
            df_insert.sort_index(ascending=True, inplace=True)
            df_insert.to_hdf(self.h5Store, '/' + self.symbol + '/_' + self.freq + '/_' + month, mode='a', format='table',
                             append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            self.df[month] = df_insert
        except Exception as e:
            print(self.symbol, self.freq, str(e))

        return

    def clease_data(self, month):
        try:
            self.df[month]
            self.df[month].drop_duplicates(keep='first', inplace=True)
            self.df[month].to_hdf(self.h5Store, '/' + self.symbol + '/_' + self.freq + '/_' + month, mode='a', format='table',
                             append=False, data_columns=True, complevel=9, complib='blosc:snappy')
        except KeyError:
            print("Data does not exist. Quit...")
        except Exception as e:
            print(self.symbol, self.freq, str(e))

    def save_contract(self, df_new, exchange, symbol, freq, month):
        try:
            self.df[month]
            # print(self.df[month])
            df_append = self.df[month].append(df_new, sort=False)
            df_append.sort_index(ascending=True, inplace=True)
            df_append.to_hdf(self.h5Store, '/' + symbol + '/_' + freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            self.df[month] = df_append
        except KeyError:
            df_new.sort_index(ascending=True, inplace=True)
            # print(df_new)
            df_new.to_hdf(self.h5Store, '/' + symbol + '/_' + freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            self.df[month] = df_new
        except Exception as e:
            print(str(e))

        return

    def overwrite(self, df_new, month):
        # confirm =  input("Are your sure you want to overwrite contract data? (\"Y or Yes\" to confirm or \"N or No\" to quit:\t")
        confirm = "y"

        if confirm in ["Y", "Yes", "y", "yes"]:
            print("Overwrite confirmed. Saving data...")
            try:
                df_new.to_hdf(self.h5Store, '/' + self.symbol + '/_' + self.freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            except Exception as e:
                print(str(e))
            return

        elif confirm in ["N", "No", "n", "no"]:
            print("Quit saving data. Exit.")
            return

        else:
            print("Command not accepted. Pass...")
            return

    def print_all(self):
        months = self.get_symbol_months()
        for month in months:
            print(month)
            print(self.df[month])

        return


    def aggr_contracts(self, dfs, start_date):
        # dfs = list(self.get_contract_data().values())
        # print(dfs)

        df_concat = pd.concat(dfs, axis=0)
        df_concat.dropna(inplace=True)
        df_concat = df_concat[(df_concat["volume"] > 0) & (df_concat.index.get_level_values("date") > start_date)]
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(df_concat)
        # print(df_concat.loc[df_concat['volume'] == 0])
        g = df_concat.groupby(df_concat.index, sort=True)
        # g = df_concat.groupby(pd.Grouper(freq=self.freq_map(), offset='21h', closed='right', label='left'))
        # g = g.filter(lambda x: x if not x.empty)
        # g.apply(print)

        df_trans = pd.concat([
            g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
            g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
            g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
            g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
            g.apply(lambda x: np.sum(x['volume'])),
            # g.apply(lambda x: np.sum(x['oi'])),
        ],
            axis=1, keys=['open', 'high', 'low', 'close', 'volume',    \
                          # 'oi'  \
                          ])
        df_trans['symbol'] = ''.join([self.symbol, "0000"])
        df_trans = df_trans[['symbol', 'open', 'high', 'low', 'close', 'volume',    \
                             # 'oi'   \
                             ]]
        # print(df_trans)

        return df_trans