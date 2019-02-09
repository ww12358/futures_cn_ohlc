# -*- coding:utf-8 -*-

import pandas as pd
import os
from include import ex_config, months, shfe_headers

idx_headers = ['symbol', 'date',    \
               'open', 'high', 'low', 'close', 'settlement', \
               'volume','turnover', 'oi']

idx_dtypes = {'symbol' : 'object',
            'date': 'datetime64[ns]',
            'open':'float64',
            'high':'float64',
            'low':'float64',
            'close':'float64',
            'settlement':'float64',
            'volume':'int64',
            'turnover':'float64',
            'oi':'int64',
         }

def wavg(sub_df, avg_col, weight_col):
    d = sub_df[avg_col]
    w = sub_df[weight_col]
    try:
        return (d * w).sum() / w.sum()
    except ZeroDivisionError:
        return d.mean()


def genMonoIdx(ex_name, symbol):
    if ex_name not in ex_config.keys() or symbol not in ex_config[ex_name]["symbols"]:
        print "Exchange or symbol error"
        exit(-1)
    else:
        print "Processing %s\t%s" % (ex_name, symbol)
        next

    DATA_PATH = ex_config[ex_name]["DATA_PATH"]

    d_li=[]
    for month in months:
        try:
            d_li.append(pd.read_hdf(DATA_PATH, '/'+symbol+'/D/'+'_'+month))
        except KeyError:
            continue

    df = pd.concat(d_li)

    mono_index_df = pd.DataFrame(columns=idx_dtypes)
    line = pd.DataFrame(columns=idx_dtypes)
    for date, sub_df in df.groupby(level='date'):
        line.loc[0, "date"] = pd.to_datetime(date)
        line.loc[0, "symbol"] = symbol + "0000"
        for col in ["open", "high", "low", "close", "settlement"]:
            line.loc[0, col] = wavg(sub_df, col, "volume")
        for col in ["volume", "turnover", "oi"]:
            line.loc[0, col] = sub_df[col].sum(axis=0)
        mono_index_df = mono_index_df.append(line, ignore_index=True)
    #    print mono_index_df
    mono_index_df = mono_index_df[idx_headers]
    mono_index_df = mono_index_df.astype(idx_dtypes)
    mono_index_df.set_index("date", inplace=True)
    #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #print mono_index_df
    #    print mono_index_df
    #mono_index_df = mono_index_df.iloc[1:]
    print mono_index_df

    with pd.HDFStore(DATA_PATH) as f:
        mono_index_df.to_hdf(f, '/' + symbol + '/D/' + '_00', format='table', append=True, data_columns=True, mode='a')
        f.close()

    print "%s%s updated." % (symbol, '00')

## main
"""
for ex_name in ex_config.keys():
    for symbol in ex_config[ex_name]["symbols"]:
#        print "Processing %s\t%s" %(ex_name, symbol)
        genMonoIdx(ex_name, symbol)

"""

for symbol in ex_config["SHFE"]["symbols"]:
#        print "Processing %s\t%s" %(ex_name, symbol)
        genMonoIdx("SHFE", symbol)