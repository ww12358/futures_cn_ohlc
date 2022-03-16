from sina.include import TQ_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map
import pandas as pd
import numpy as np

class tq_mh(h5_store):
    def __init__(self, symbol, freq):
        self.symbol = symbol.upper()
        # self.exchange = exchange.upper()
        self.exchange = symbol_exchange_map[symbol]
        self.h5_path = "".join([TQ_PATH, freq, "/", self.exchange, "/", symbol, ".hdf5"])

        super(tq_mh, self).__init__(symbol, freq)

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