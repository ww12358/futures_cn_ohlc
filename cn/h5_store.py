import pandas as pd
import numpy as np
import os
from datetime import datetime
from cn.include import symbol_exchange_map

class h5_store:
    # h5_path = ""
    def __init__(self, symbol, freq):

        self.symbol = symbol
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

            else:   #load local file storage to instance
                self.months = self.get_symbol_months_with_idx()

                for month in self.months:
                    key = ''.join(["/", self.symbol, "/", self.freq, "/_", month])
    #                print(key)
                    if (d := pd.read_hdf(self.h5Store, key)) is not None:
                        self.df[month] = d
                    # self.df[month] = d if (d:=pd.read_hdf(self.h5Store, key)) is not None
                #   print(month, '\n', self.df[month])
                # self.df = {k:v for k,v in self.df.items() if v is not None}     #filter None

                # self.__isemtpy = False
                self.set_notempty()
                # print(self.__isempty)
    #                if self.exchange == "DCE":
    #                    self.__df[month].reset_index(inplace=True)
    #                    self.__df[month].set_index(["date", "symbol"], inplace=True)
    #                 if self.exchange == "CZCE":
    #                     if "d_oi" in self.__df[month].columns:
    #                         self.__df[month].drop(["d_oi", "EDSP"], axis=1, inplace=True)
    #                         self.__df[month]["pre_close"] = 0
    #                         self.__df[month].reset_index(inplace=True)
    #                         self.__df[month].set_index(["date", "symbol"], inplace=True)
    #                    if self.symbol == "TA":
    #                        self.__df[month].reset_index(inplace=True)
    #                        self.__df[month]["symbol"] = self.__df[month]["symbol"].str.replace("PTA", "TA")
    #                        self.__df[month].set_index(["date", "symbol"], inplace=True)
    #             print("Local Data loaded successfully! Continue...")

        except Exception as e:
            print("Some error occured during access data file:\t", str(e))
            return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.h5Store:
            #            self.__h5Store.flush()
            self.h5Store.close()

    def close(self):
        if self.h5Store:
            #            self.__h5Store.flush()
            self.h5Store.close()

    def isempty(self):
        return self.__isempty

    def set_empty(self):
        self.__isempty = True

    def set_notempty(self):
        self.__isempty = False

    def get_symbol(self):
        return self.symbol

    def get_data(self, year, month):
        if not year is None:
#            print(year)
#            year_short = year[2:]
            query_str = self.symbol + year + month
            # print(query_str)

        try:
            df = self.df[month]
            # print(df)
            df = df.loc[df.index.get_level_values('symbol') == query_str]
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
            for item in self.h5Store.walk("/" + self.symbol + "/" + self.freq + "/"):
#               print(item)
                months_raw = list(item[2])
                months = [(lambda x: x.strip('_'))(x) for x in months_raw]
                if("00" in months):     #do not update vw_idx
                    months.remove("00")
            return months

        except Exception as e:
            print(str(e))
            return None

    def get_symbol_months_with_idx(self):
        try:
            months = []
            # find months list from local hdf5
            for item in self.h5Store.walk("/" + self.symbol + "/" + self.freq + "/"):
                # print(item)
                months_raw = list(item[2])
                months = [(lambda x: x.strip('_'))(x) for x in months_raw]

        except Exception as e:
            print(str(e))

        return months

    def append_data(self, df_append, month, debug=False):
        try:
            self.df[month]
            df_append = df_append.loc[df_append.index.get_level_values("date") > self.df[month].index.get_level_values("date")[-1]]
            # df_append = df_append.loc[df_append.index > self.df[month].iloc[-1].name]
            df_append.sort_index(ascending=True, inplace=True)
            if debug:
                with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                    print(df_append.head(10))
                    print(df_append.tail(50))
            else:
                df_append.to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a', format='table', append=True,
                                 data_columns=True, complevel=9, complib='blosc:snappy')
        except KeyError:
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_append.head(10))
            #     print(df_append.tail(50))
            df_append.sort_index(ascending=True, inplace=True)
            df_append.to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a', format='table',
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
            df_new.to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a', format='table', append=False,
                             data_columns=True, complevel=9, complib='blosc:snappy')
            self.df[month] = df_new
        except KeyError:
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_insert.head(10))
            #     print(df_insert.tail(50))
            df_insert.sort_index(ascending=True, inplace=True)
            df_insert.to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a', format='table',
                             append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            self.df[month] = df_insert
        except Exception as e:
            print(self.symbol, self.freq, str(e))

        return

    def clease_data(self, month):
        try:
            self.df[month]
            self.df[month].drop_duplicates(keep='first', inplace=True)
            self.df[month].to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a', format='table',
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
            df_append.to_hdf(self.h5Store, '/' + symbol + '/' + freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            self.df[month] = df_append
        except KeyError:
            df_new.sort_index(ascending=True, inplace=True)
            # print(df_new)
            df_new.to_hdf(self.h5Store, '/' + symbol + '/' + freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
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
                df_new.to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
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


    def get_contract_by_month(self, month):
        if self.months is None:
            print("Data not loaded. Check data file.")
            return

        if month in self.months:
            return self.df[month]

    def get_all_data(self):
        if not self.df is None:
            return self.df
        else:
            print("Data not loaded correctly, abort!")
            return

    def get_contract_data(self):   # all data except "00" / index
        # print(self.__df)
        dfs_copy = self.df.copy()
        # print(self.df)
        if not dfs_copy is None:
            if "00" in dfs_copy.keys():
                del dfs_copy["00"]
                return dfs_copy
            else:
                return dfs_copy
        else:
            print("Data not loaded correctly, abort!")
            return

    def get_last_trade_data(self):
        if not self.df is None:
            for key in self.df.keys():
                self.df[key] = self.df[key].iloc[-1]
            return self.df

    def get_idx_data(self):
        try:
            key = ''.join(["/", self.symbol, "/", self.freq, "/_", "00"])
            df = pd.read_hdf(self.h5Store, key)
        except KeyError:
            raise KeyError("NO_INDEX_ERROR")
        except Exception as e:
            print(str(e))

        return df

    def get_max_contract_date(self):

        dfs = list(self.get_contract_data().values())
        dates = [df.index.get_level_values('date').max() for df in dfs]
        # print(dates)

        # check if each month data table has the same data length and ends with same date
        dates_set = set(dates)
        if len(dates_set) == 1:
            latest_date = dates_set.pop()
            print("Finished checking contracts. All dates are same. Continue...")
        else:
            print("Warning!!! Last dates not equal. \nPlease update contract data first. \nIgnore this if this is special contracts like \'AU\' or some contract is rolling over.")
            latest_date = max(dates)
        #        return
        # print("latest_date\t", latest_date)
        return latest_date

    # def get_latest_date(self):
    #     latest_local_date_dic = {}
    #     months = self.get_symbol_months()
    #     for month in months:
    #         # df = pd.read_hdf(self.__h5Store, '/' + self.symbol + "/" + self.freq + "/_" + month)
    #         # latest_local_date_dic[month] = df.index.get_level_values("date").max()
    #         latest_local_date_dic[month] = self.get_contract_by_month(month).index.get_level_values("date").max()
    #
    #     return latest_local_date_dic
    # def freq_map(self):
    #     freq_dic = {"M5": "5min", "M15": "15min", "M30": "30min", "H1":"1h", "H3":"3h", "D":"1d"}
    #     print(freq_dic[self.freq])
    #
    #     return freq_dic[self.freq]

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

        if 'oi' in df_concat.columns:

            df_trans = pd.concat([
                g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
                g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
                g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
                g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
                g.apply(lambda x: np.sum(x['volume'])),
                g.apply(lambda x: np.sum(x['oi'])),
            ],
                axis=1, keys=['open', 'high', 'low', 'close', 'volume', 'oi'])
            df_trans['symbol'] = ''.join([self.symbol, "0000"])
            df_trans = df_trans[['symbol', 'open', 'high', 'low', 'close', 'volume', 'oi']]

        else:
            df_trans = pd.concat([
                g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
                g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
                g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
                g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
                g.apply(lambda x: np.sum(x['volume'])),
                # g.apply(lambda x: np.sum(x['oi'])),
            ],
                axis=1, keys=['open', 'high', 'low', 'close', 'volume'])
            df_trans['symbol'] = ''.join([self.symbol, "0000"])
            df_trans = df_trans[['symbol', 'open', 'high', 'low', 'close', 'volume']]
        # print(df_trans)

        return df_trans

    def rebuild_idx(self, f_dry_run):
        dfs = list(self.get_contract_data().values())
        # print(dfs)
        print("Rebuild index price...")
        df_new = self.aggr_contracts(dfs, datetime.strptime("19700101", "%Y%m%d"))
        print(df_new)
        if not f_dry_run:
            self.overwrite(df_new, "00")
            print("not dry run. do overwrite data...")

    def generate_idx(self, f_rebuild=False, f_dry_run=False):

        try:
            if f_rebuild:
                self.rebuild_idx(f_dry_run)

            else:  # rebuild == False
                print("Update/Append index price...")
                latest_contract_date = self.get_max_contract_date()

                mono_idx_df = self.get_idx_data()
                latest_idx_date = mono_idx_df.index.get_level_values("date").max()
                # latest_idx_date = mono_idx_df.index.max()
                print("latest index date",latest_idx_date)
                print("Current price index latest date", latest_contract_date)

                if mono_idx_df.empty:  # in case price index data is empty, set a very early date
                    latest_idx_date = pd.to_datetime("19700101", "%Y%m%d")

                if latest_contract_date == latest_idx_date:
                    print(self.symbol, "Price index \'00\' is Up-to-date. No need to update. Skip!")
                    return

                elif latest_idx_date < latest_contract_date:
                    dfs = list(self.get_contract_data().values())
                    # print(dfs)
                    # print(latest_idx_date, latest_contract_date)
                    df_append = self.aggr_contracts(dfs, latest_idx_date)
                    # print(df_append)
                    if not f_dry_run:
                        # print("not dry run. do append data...")
                        self.append_data(df_append, "00")
                    print("Successfully updated index %s" % self.symbol)

                else:
                    print("Contract data fewer than Index data. Something wrong with data file. Please check. ")

        except KeyError as e:
            print(str(e))
            self.rebuild_idx(f_dry_run)
            return

        except Exception as e:
            print("Error occured accessing %s index data" % self.get_symbol())
            print(str(e))
            return