from sina.include import SINA_M5_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map
import pandas as pd
import numpy as np

class sina_M5(h5_store):
    def __init__(self, symbol, freq):
        self.symbol = symbol.upper()
        # self.exchange = exchange.upper()
        self.exchange = symbol_exchange_map[symbol]
        self.h5_path = "".join([SINA_M5_PATH, self.exchange, "/", symbol, ".hdf5"])

        super(sina_M5, self).__init__(symbol, freq)
        # print(self.h5_path)

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
