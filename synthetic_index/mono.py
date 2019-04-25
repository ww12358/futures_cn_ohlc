# -*- coding:utf-8 -*-

import pandas as pd
from datetime import datetime
import click
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


def agregate_month_data(symbol, df_all, start_date):
#    print(start_date)
    df_all = df_all.loc[df_all.index.get_level_values("date") > start_date]
#    print(df_all)

    df_append=pd.DataFrame(columns=idx_dtypes)
    df_row = pd.DataFrame(columns=idx_dtypes)
    for date, sub_df in df_all.groupby(level='date'):
#        print(date)
#        print(sub_df)
        df_row.loc[0, "date"] = pd.to_datetime(date)
        df_row.loc[0, "symbol"] = symbol + "0000"
        for col in ["open", "high", "low", "close", "settlement"]:
            df_row.loc[0, col] = wavg(sub_df, col, "volume")
        for col in ["volume", "turnover", "oi"]:
            df_row.loc[0, col] = sub_df[col].sum(axis=0)
#        print(df_row)
        df_append = df_append.append(df_row, ignore_index=True)

    df_append = df_append[idx_headers]
    df_append = df_append.astype(idx_dtypes)
    df_append.set_index("date", inplace=True)

    print(df_append)

    return df_append


def genMonoIdx(ex_name, symbol):
    latest_local_date_dic = {}
    local_data_length_dic ={}
    months = []
    if ex_name not in ex_config.keys() or symbol not in ex_config[ex_name]["symbols"]:
        print("Exchange or symbol error")
        exit(-1)
    else:
        print("Processing %s\t%s" % (ex_name, symbol))
        next

    DATA_PATH = ex_config[ex_name]["DATA_PATH"]

    d_li=[]
    with pd.HDFStore(DATA_PATH) as f:
        for item in f.walk("/" + symbol + "/D/"):
            months_raw = list(item[2])
            months = [(lambda x: x.strip('_'))(x) for x in months_raw]

        if "00" in months:
            months.remove("00")
            print("Months after index removed", months)
            try:
                mono_idx_df = pd.read_hdf(f, '/'+symbol+"/D/_00")
                latest_idx_date = mono_idx_df.index.get_level_values("date").max()
                if mono_idx_df.empty:       #in case price index data is empty, set a very early date
                    latest_idx_date = pd.to_datetime("19700101", "%Y%m%d")
                print("Current price index latest date", latest_idx_date)
            except Exception as e:
                print("Error occured accessing %s index data", symbol)
                print(str(e))
                return

        else:
            print(symbol, "Price Index does not exsist. New dataframe constructed. Continue")
            mono_index_df = pd.DataFrame(columns=idx_dtypes)
            latest_idx_date = datetime.strptime("19700101", "%Y%m%d")

        for month in months:
            try:
                df_month = pd.read_hdf(f, '/'+symbol+'/D/'+'_'+month)
                d_li.append(df_month)
                latest_local_date_dic[month] = df_month.index.get_level_values("date").max()
#                local_data_length_dic[month] = len(df_month.index)
            except KeyError:
                continue
        df = pd.concat(d_li)

    dates = list(latest_local_date_dic.values())
#    data_length = local_data_length_dic
#    print(data_length)
#    print(months)
#    print(dates)

    #check if each month data table has the same data lenth and ends with same date
    dates_set = set(dates)
    if len(dates_set) == 1:
        latest_date = dates_set.pop()
        print("All dates are same.")
    else:
        print("Dates not equal. Please update contract data with 'update_shfe_ts.py' first. Excecution quit")
        return

    print("latest_date")
#    latest_date = dates[0]

    if latest_date == latest_idx_date:
        print(symbol, "price index is up-to-date. Skip.")
        return
    else:
        mono_index_df = agregate_month_data(symbol, df, latest_idx_date)
 #       for a, b in itertools.combinations(idx_latest, 2):
        # # print(up_to_date(a,b))
        #            if not is_up_to_date(a, b):
        #                print("Data not up-to-date!", a, b)
        #            else:
        #                continue

    #    print mono_index_df

    #with pd.option_context('display.max_rows', None, 'display.max_columns', None):
    #print mono_index_df
    #    print mono_index_df
    #mono_index_df = mono_index_df.iloc[1:]
    print(mono_index_df)

    with pd.HDFStore(DATA_PATH) as f:
        mono_index_df.to_hdf(f, '/' + symbol + '/D/' + '_00', format='table', append=True, data_columns=True, mode='a')
        f.close()

    print("%s%s updated." % (symbol, '00'))

## main
"""
for ex_name in ex_config.keys():
    for symbol in ex_config[ex_name]["symbols"]:
#        print "Processing %s\t%s" %(ex_name, symbol)
        genMonoIdx(ex_name, symbol)

"""

@click.command()
@click.option("--symbol", "-s",
              type=click.STRING,
              help="symbol")
@click.option("--exchange", "-e",
              type=click.STRING,
              )
def main(symbol, exchange):

    shfe_symbols = ex_config["SHFE"]["symbols"]

    if symbol:
        symbol = symbol.strip().upper()
        if symbol == "ALL":
            for smbl in shfe_symbols:
                genMonoIdx("SHFE", smbl)

    if exchange:
        exchange = exchange.strip().upper()

    if exchange in ex_config.keys() and symbol in ex_config[exchange]["symbols"]:
        print(exchange, symbol)
        genMonoIdx(exchange, symbol)

if __name__ == "__main__":
    main()

#for symbol in ex_config["SHFE"]["symbols"]:
#        print "Processing %s\t%s" %(ex_name, symbol)
#        genMonoIdx("SHFE", symbol)
