# -*- coding:utf-8 -*-
import quandl
import pandas as pd
from .include import ex_config
import numpy as np
import datetime
quandl.ApiConfig.api_key = 'zoFEDaUaEqsZdsajsp_o'

month_alphabet = ['F', 'G', 'H', 'J', 'K', 'M', 'N', 'Q', 'U', 'V', 'X', 'Z']
months = ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']
month_dict = dict(zip(month_alphabet, months))

start = '2018-10-12'
end = '2018-12-31'

def zero_volume(df):
    df.loc[df.volume == 0, 'open'] = \
    df.loc[df.volume == 0, 'high'] = \
    df.loc[df.volume == 0, 'low'] = df.loc[df.volume == 0, 'close']

    return

def ex_spec_details(ex_name, qdata):
    quandl_col_headers = ['open', 'high', 'low', 'close', 'pre_settlement', 'settlement', 'volume', 'oi', 'turnover', 'symbol']
    quandl_shfe_col_headers = col_headers = ['pre_settlement', 'open', 'high', 'low', 'close', 'settlement', 'd1', 'd2', 'volume', 'oi', 'oi_change', 'symbol']
    if ex_name == "SHFE":
        qdata.columns = quandl_shfe_col_headers
        qdata['turnover'] = -1
        qdata['pre_close'] = -1
        qdata.drop(columns=['oi_change'], inplace=True)

    elif ex_name == "DCE":
        qdata.columns = quandl_col_headers
        qdata['d1'] = -1
        qdata['d2'] = -1
        qdata['pre_close'] = -1

    elif ex_name == "ZCE":
        qdata.columns = quandl_col_headers
        qdata['d1'] = -1
        qdata['d2'] = -1
        qdata['d_oi'] = -1
        qdata['EDSP'] = -1
    return

def update_symbol_till_last_tdd(ex_name, symbol_str):
    TEST_DATA_PATH = ex_config[ex_name]["TEST_DATA_PATH"]
    DATA_PATH = ex_config[ex_name]["DATA_PATH"]
    headers = ex_config[ex_name]["headers"]
    dtypes = ex_config[ex_name]["dtypes"]



    for month in month_alphabet:
        month_digit_str = month_dict[month]
        print "Fetching %s%s" % (symbol_str, month_digit_str)
        query_str_2018 = ''.join([ex_name, "/", symbol_str, month, '2018'])    #eg. DCE/JDK2019
        query_str_2019 = ''.join([ex_name, "/", symbol_str, month, '2019'])

        #  print query_str
        try:
            qdata_2018 = quandl.get(query_str_2018, start_date=start, end_date=end)
            qdata_2018.replace('None', np.nan)
            qdata_2018.fillna(0, inplace=True)
            qdata_2018['symbol'] = ''.join([symbol_str,'18',month_digit_str])
        except:
            print "%s not found" % query_str_2018
            try:
                qdata_2019 = quandl.get(query_str_2019, start_date=start, end_date=end)
                qdata_2019.replace('None', np.nan)
                qdata_2019.fillna(0, inplace=True)
                qdata_2019['symbol'] = ''.join([symbol_str, '19', month_digit_str])
            except:        # if both query throw exception, the symbol does not exist for remote database, skip
                print "%s not found" % query_str_2019
                continue
        try:
            qdata_2019 = quandl.get(query_str_2019, start_date=start, end_date=end)
            qdata_2019.replace('None', np.nan)
            qdata_2019.fillna(0, inplace=True)
            qdata_2019['symbol'] = ''.join([symbol_str, '19', month_digit_str])
        except:
            print "%s not found" % query_str_2019
            qdata_2019 = pd.DataFrame(columns=qdata_2018.columns)

        qdata = qdata_2018.append(qdata_2019)

        ex_spec_details(ex_name, qdata)
        qdata.index.name = 'date'
        qdata.reset_index(inplace=True)
        qdata = qdata[headers]
        try:
            qdata = qdata.astype(dtypes)
#            print qdata.dtypes
        except:
            print "Error occured when converting data type. Aborted..."
            return

        qdata.set_index(['date','symbol'], inplace=True)
        zero_volume(qdata)
#        print qdata, '\n', qdata.dtypes, qdata.index


        with pd.HDFStore(DATA_PATH) as f:
            try:
                df = pd.read_hdf(f, '/'+symbol_str+'/D/_'+month_dict[month], "r")
 #               print df.index
            except KeyError:    #new data from remote database, local file does not contain it
                qdata.to_hdf(f, '/' + symbol_str + '/D/' + '_' + month_digit_str, format='table', append=True,
                                                         data_columns=True, mode='a')
                print       "%s%s inserted.(New data group created)" % (symbol_str, month_digit_str)
            except:
                print "Continue..."

            if qdata.index.isin(df.index).all():
               print "Nothing to do. %s%s data was updated" % (symbol_str, month_digit_str)
            else:
                qdata.to_hdf(f, '/' + symbol_str + '/D/' + '_' + month_digit_str, format='table', append=True,
                             data_columns=True, mode='a')
                print "%s%s updated." % (symbol_str, month_digit_str)

            f.close()
    """
            try:
                #df.append(qdata, verify_integrity=True, sort=True)
                df_all = pd.concat([df, qdata], verify_integrity=True, sort=True, levels=['date'], axis=0)
            except:
                print "Value Error"
            print df_all
    """

def updateExchangeData(ex_name):

    symbols = ex_config[ex_name]["symbols"]
    for sym in symbols:
        update_symbol_till_last_tdd(ex_name, sym)


updateExchangeData("SHFE")