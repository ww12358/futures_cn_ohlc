import pandas as pd
import os

class h5_store:
    h5_path = ""
    def __init__(self, exchange, symbol, freq):

        self.symbol = symbol
        self.freq = freq
        self.exchange = exchange
        self.__df = {}
        self.__isempty = True
        # print(self.h5_path)
        try:
#            print(DATA_PATH_DICT[self.symbol])
            self.__h5Store = pd.HDFStore(self.h5_path)
#            print(DATA_PATH_DICT[self.symbol])
            if not os.path.exists(self.h5_path):
                print("file not exists, will create new file")

            if not symbol in self.__h5Store:
                print("symbol not found in local file, will save new contracts to local")
                return

            else:   #load local file storage to instance
                self.months = self.get_symbol_months()

                for month in self.months:
                    key = ''.join(["/", self.symbol, "/", self.freq, "/_", month])
    #                print(key)
                    self.__df[month] = pd.read_hdf(self.__h5Store, key)
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
        #
        # except TypeError as e:
        #     print(str(e))
        #     return None
        #
        # except KeyError as e:
        #     raise KeyError
        #     return
        except Exception as e:
            print("Some error occured during access data file:\t", str(e))
            return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__h5Store:
            #            self.__h5St`   ore.flush()
            self.__h5Store.close()
#     def __del__(self):
#         if self.__h5Store:
# #            self.__h5Store.flush()
#             self.__h5Store.close()
#             print("Saving data to disk drive... Please wait.")
#             print("Success! Update exit.")
#             return
#         else:
#             return

    def isempty(self):
        return self.__isempty

    def set_empty(self):
        self.__isempty = True

    def set_notempty(self):
        self.__isempty = False

    def get_data(self, year, month):
        if not year is None:
#            print(year)
#            year_short = year[2:]
            query_str = self.symbol + year + month
#            print(query_str)

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
        try:
            months = []
            # find months list from local hdf5
            for item in self.__h5Store.walk("/" + self.symbol + "/" + self.freq + "/"):
#               print(item)
                months_raw = list(item[2])
                months = [(lambda x: x.strip('_'))(x) for x in months_raw]
                if("00" in months):     #do not update vw_idx
                    months.remove("00")

            return months
        # except ValueError as e:
        #     print(str(e))
        #
        # except TypeError as e:
        #     print(str(e))
        #     return None
        #
        # except KeyError as e:
        #     print(str(e))
        #
        # except IOError as e:
        #     print(str(e))
        #
        # except NotImplementedError as e:
        #     print(str(e))
        #     return None
        # #node does not exist, new contracts
        # except tb.NoSuchNodeError as e:
        #     print(str(e))
        #     return None

        except Exception as e:
            print(str(e))
            return None



    def get_symbol_months_with_idx(self):
        try:
            months = []
            # find months list from local hdf5
            for item in self.__h5Store.walk("/" + self.symbol + "/" + self.freq + "/"):
                #               print(item)
                months_raw = list(item[2])
                months = [(lambda x: x.strip('_'))(x) for x in months_raw]

        except Exception as e:
            print(str(e))

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
#         with pd.option_context('display.max_rows', None, 'display.max_columns', None):
#             print(df_new.head(10))
#             print(df_new.tail(50))

        df_new.to_hdf(self.__h5Store, '/' + symbol + '/' + freq + '/_' + month, mode='a', format='table', append=False,
                                  data_columns=True, complevel=9, complib='blosc:snappy')

        self.__df[month] = df_new
#        self.__h5Store.flush()

        return

    def save_contract(self, df_new, exchange, symbol, freq, month):
        try:
            self.__df[month]
            df_append = self.__df[month].append(df_new, sort=False)
            df_append.sort_index(ascending=True, inplace=True)
            df_append.to_hdf(self.__h5Store, '/' + symbol + '/' + freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            self.__df[month] = df_append
        except KeyError:
            df_new.sort_index(ascending=True, inplace=True)
            # print(df_new)
            df_new.to_hdf(self.__h5Store, '/' + symbol + '/' + freq + '/_' + month, mode='a', format='table', append=False, data_columns=True, complevel=9, complib='blosc:snappy')
            self.__df[month] = df_new
        except Exception as e:
            print(str(e))

        return

    def print_all(self):
        months = self.get_symbol_months()
        for month in months:
            print(month)
            print(self.__df[month])

        return


    def get_contract_by_month(self, month):
        if self.months is None:
            print("Data not loaded. Check data file.")
            return

        if month in self.months:
            return self.__df[month]

    def get_all_data(self):
        if not self.__df is None:
            return self.__df
        else:
            print("Data not loaded correctly, abort!")
            return

    def get_last_trade_data(self):
        if not self.__df is None:
            for key in self.__df.keys():
                self.__df[key] = self.__df[key].iloc[-1]
            return self.__df

    def get_idx_data(self):
        key = ''.join(["/", self.symbol, "/", self.freq, "/_", "00"])
        return pd.read_hdf(self.__h5Store, key)