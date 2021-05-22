# -*- coding:utf-8 -*-

import pandas as pd
from cn.include import DATA_PATH_DICT
from cn.h5_store import h5_store
import numpy as np
from datetime import datetime
from cn.include import symbol_exchange_map

class localData(h5_store):
    def __init__(self, symbol, freq):
        self.symbol = symbol
        self.exchange = symbol_exchange_map[symbol]
        self.h5_path = DATA_PATH_DICT[self.symbol]

        super(localData, self).__init__(symbol, freq)

    def aggr_contracts(self, dfs, start_date):
        # dfs = list(self.get_contract_data().values())
        # print(dfs)

        df_concat = pd.concat(dfs, axis=0)
        df_concat = df_concat[(df_concat["volume"] > 0) & (df_concat.index.get_level_values("date") > start_date)]
        g = df_concat.groupby(level='date', sort=True)

        df_trans = pd.concat([
            g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
            g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
            g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
            g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
            g.apply(lambda x: np.average(x['settlement'], weights=x['volume'])),
            g.apply(lambda x: np.sum(x['volume'])),
            g.apply(lambda x: np.sum(x['turnover'])),
            g.apply(lambda x: np.sum(x['oi'])),
        ],
            axis=1, keys=['open', 'high', 'low', 'close', 'settlement', 'volume', 'turnover', 'oi'])
        df_trans['symbol'] = ''.join([self.symbol, "0000"])
        df_trans = df_trans[['symbol', 'open', 'high', 'low', 'close', 'settlement', 'volume', 'turnover', 'oi']]
        # print(df_trans)

        return df_trans

    def get_latest_date(self):

        latest_local_date_dic = {}
        months = self.get_symbol_months()
        for month in months:
            latest_local_date_dic[month] = self.get_contract_by_month(month).index.get_level_values("date").max()

        return latest_local_date_dic

    # def append_data(self, df_append, month):
    #
    #     try:
    #         self.df[month]
    #         # print(self.df[month].iloc[-1].name)
    #         print(self.df[month].tail(1).index.get_level_values("date"))
    #         print(df_append.index.get_level_values("date"))
    #         # df_append = df_append.loc[df_append.index.get_level_values("date") > self.df[month].iloc[-1].name]
    #         df_append = df_append.loc[df_append.index.get_level_values("date") > self.df[month].tail(1).index.get_level_values("date")]
    #         df_append.sort_index(level=["date", "symbol"], ascending=True, inplace=True)
    #         # print(df_append)
    #
    #         df_append.to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a',
    #                          format='table', append=True,
    #                          data_columns=True, complevel=9, complib='blosc:snappy')
    #
    #     except KeyError:
    #         # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #         #     print(df_append.head(10))
    #         #     print(df_append.tail(50))
    #         df_append.to_hdf(self.h5Store, '/' + self.symbol + '/' + self.freq + '/_' + month, mode='a',
    #                          format='table',
    #                          append=False, data_columns=True, complevel=9, complib='blosc:snappy')
    #
    #     except Exception as e:
    #         print(str(e))
    #
    #         return
    #
    #     self.df[month] = df_append
    #
    #     return

    # def generate_idx(self, f_rebuild=False, f_dry_run=False):
    #
    #     try:
    #
    #         if f_rebuild:
    #             # dfs = list(self.get_contract_data().values())
    #             # print("Rebuild index price...")
    #             # df_new = self.aggr_contracts(dfs, datetime.strptime("19700101", "%Y%m%d"))
    #             # print(df_new)
    #             # if not f_dry_run:
    #             #     # print("not dry run. do overwrite data...")
    #             #     self.overwrite(df_new, self.symbol, "00")
    #             self.rebuild_idx(f_dry_run)
    #
    #         else:  # rebuild == False
    #             print("Update/Append index price...")
    #             latest_contract_date = self.get_max_contract_date()
    #             mono_idx_df = self.get_idx_data()
    #             # the only line override h5_store
    #             latest_idx_date = mono_idx_df.index.get_level_values("date").max()
    #             #end of override
    #             print("Current price index latest date", latest_idx_date)
    #
    #             if mono_idx_df.empty:  # in case price index data is empty, set a very early date
    #                 latest_idx_date = pd.to_datetime("19700101", "%Y%m%d")
    #
    #             if latest_contract_date == latest_idx_date:
    #                 print(self.symbol, "Price index \'00\' is Up-to-date. No need to update. Skip!")
    #                 return
    #             elif latest_idx_date < latest_contract_date:
    #                 dfs = list(self.get_contract_data().values())
    #                 # print(latest_date)
    #                 df_append = self.aggr_contracts(dfs, latest_idx_date)
    #                 print(df_append)
    #                 if not f_dry_run:
    #                     # print("not dry run. do append data...")
    #                     self.append_data(df_append, "00")
    #                 print("Successfully updated index %s" % self.symbol)
    #             else:
    #                 print("Contract data fewer than Index data. Something wrong with data file. Please check. ")
    #
    #     except KeyError as e:
    #         print(str(e))
    #         self.rebuild_idx(f_dry_run)
    #         return
    #
    #     except Exception as e:
    #         print("Error occured accessing %s index data", self.get_symbol())
    #         print(str(e))
    #         return