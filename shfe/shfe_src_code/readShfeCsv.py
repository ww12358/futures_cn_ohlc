# -*- coding:utf-8 -*-
from __future__ import print_function
from __future__ import absolute_import
from shfe.include import *
import pandas as pd
import re

def readShfeSrcData():
    f = pd.HDFStore(SHFE_DATA_PATH, "w")

    for item in sorted(os.listdir(SRC_PATH)):
        FILE_PATH = os.path.join(SRC_PATH, item)
        year_str = os.path.basename(item)
        print("dir: %s, year: %s" % (FILE_PATH, year_str))
        df = pd.read_csv(FILE_PATH,  \
                            sep=',', header=None,   \
                            skipinitialspace=True, skiprows=1, \
                             #usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14],   \
                            names=shfe_headers,   \
                            parse_dates=['date'], \
                            dtype=shfe_dtypes, \
                            thousands=','   )
                         #, index_col=[0, 1])
    #    print df.isna().sum()
        symbol = ''
        for index, row in df.iterrows():
            if pd.isnull(row['symbol']):
               df.loc[index, 'symbol'] = symbol
            else:
                symbol = df.loc[index, 'symbol'] = row['symbol'].upper()

    #            matchObj = re.search(r'\b[a-z]{2}\d{4}', row['symbol'])
            if row['volume'] == 0:
                df.loc[index, 'open']=  \
                df.loc[index, 'high']=  \
                df.loc[index, 'close']= \
                df.loc[index, 'low'] = row['pre_close']
                df.loc[index, 'settlement'] = row['pre_settlement']

            if pd.isnull(row['d1']):
                df.loc[index, 'd1'] = 0

            if pd.isnull(row['d2']):
                df.loc[index, 'd2'] = 0
    #            print row
        df.set_index(['date', 'symbol'], inplace=True)
    #    print df, '\n', df.isna().sum()
        for symbol_str in shfe_symbols:
#            print symbol_str
            for month_str in months:
#                print month_str
                pat = re.escape(symbol_str) + r'\d{2}' + re.escape(month_str)
    #                dfa = df.loc[df['symbol'].str.match(pat)]
                dfa = df.loc[df.index.get_level_values('symbol').str.match(pat)]
                if not dfa.empty:
                   dfa.to_hdf(f, '/' + symbol_str + '/D/' + '_' + month_str, format='table', append=True, data_columns=True, mode='a')
  #              print dfa
    f.flush()
    f.close()
    return

