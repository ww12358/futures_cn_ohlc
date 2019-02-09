# coding: utf-8
import os
import pandas as pd
import re
from include import dce_symbols, dce_dtypes, dce_headers, months, DATA_PATH, SRC_PATH, TEST_DATA_PATH
import h5py

def getYear(f_name):
    matchObj = re.match(r'(.*)_(.*).csv', f_name)
    if matchObj:
#        print matchObj.group(2), matchObj.group(0)
        return matchObj.group(2)

def readSrcData():
    f = pd.HDFStore(DATA_PATH)

    for item in os.listdir(SRC_PATH):
        SUB_DIR = os.path.join(SRC_PATH, item)
        symbol = os.path.basename(item).upper()
        print symbol
        if os.path.isdir(SUB_DIR):
            df = pd.DataFrame()
            for f_name in sorted(os.listdir(SUB_DIR)):
                FILE_PATH = os.path.join(SUB_DIR, f_name)
#                year = getYear(f_name)
#                print year
#                y = year[2:]
#                print y
                if os.path.isfile(FILE_PATH):
                    #                print FILE_PATH
                    df_sub = pd.read_csv(FILE_PATH, sep=',', header=None, skipinitialspace=True, skiprows=1, \
                                         usecols=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14], names=dce_headers,    \
                                         parse_dates=['date'], \
                                         dtype=dce_dtypes, index_col=[0,1])
#                    print df_sub.dtypes
                   # print df_sub
                    if df.empty:        #first time reading csv, dataframe is empty
                        df = df_sub.copy()
                    else:
                        df = pd.concat([df, df_sub])
            #                print df.head(10)
#                print df.shape  #, '\n', df.head(10), '\n', df.tail(10)
#                for month in months:
#                   dfc = df.iloc[df.index.get_level_values(0).str.contains('04')]
#                    reg_str = "".join(['\w\w\d\d'], month)
#                    dfc = df.iloc[df.index.get_level_values(0).str.match(r'\w\w\d\d'+re.escape(month))]
            for contract, df_sel in df.groupby(level=0):
                matchObj = re.search(r'^\w{1,2}?\d{2}(.*)', contract)
                if matchObj:
                    month = matchObj.group(1)
#                    print df_sel.dtypes
                    df_sel.to_hdf(f, '/'+symbol+'/D/'+'_' + month, format='table', append=True, data_columns=True, mode='a')
                else:
                    print 'Data format not match. Abort...'
                    return
            f.flush()
            f.close()

def cleanse_data():
#    pd.options.mode.chained_assignment = None  # default='warn'
#    f = pd.HDFStore(DATA_PATH, "r")
#
#    info = f.keys()
#    print info

    for symbol in dce_symbols:
        with h5py.File(DATA_PATH) as f:
            sub_grp = f["".join(["/", symbol, '/D'])]
            months_str = sub_grp.keys()
        f.close()
#        print months_str
        with pd.HDFStore(DATA_PATH, "r") as f_pandas:
            for month_str in months_str:
                df = pd.read_hdf(f_pandas, "".join(["/",symbol, '/D/', month_str]))
                index_upper(df)     #refresh symbols as uppercase, eg: 'a1901' -> 'A1901'
#                remove_NaN(df)
#                df.fillna(0, inplace=True)
                transit_zero_volumes(df)
#                with pd.option_context('display.max_rows', None, 'display.max_columns', None):
                print "Update Symbol: %s,   subgroup: %s" % (symbol, month_str)
                df.to_hdf(TEST_DATA_PATH, '/'+symbol+'/D/'+month_str, format='table', append=True, data_columns=True, mode='a')
        f_pandas.flush()
        f_pandas.close()
#
##Turn synbol column into Uppercase, eg: a1901 -->  A1901
def index_upper(df):
    df.index.set_levels(df.index.get_level_values('symbol').str.upper().unique(), level='symbol', inplace=True)
    return

##On first trading day of a new contract, pre_close="NaN", make pre_close = pre_settlement
def remove_NaN(df):
    df.loc[df.pre_close.isnull(), "pre_close"] = df.loc[df.pre_close.isnull(), 'pre_settlement']
    return

def transit_zero_volumes(df):
    df.loc[df.volume==0, 'open'] = \
    df.loc[df.volume == 0, 'high'] = \
    df.loc[df.volume == 0, 'low'] = df.loc[df.volume==0, 'close']

    return

