import pandas as pd
import datetime
from cn.include import all_symbols

def ohlcsum(data):
    if data.empty:
        return data
    #     df = df.sort_index()
    else:
        return pd.DataFrame({
            'open': data['open'].iloc[0],
            'high': data['high'].max(),
            'low': data['low'].min(),
            'close': data['close'].iloc[-1],
            'volume': data['volume'].sum(),
            'contract': data['contract']
        }, index=data.index)


def convert_sinaM5origin(symbol, freq, data, rebuild):
    # tm = data.iloc[-2].name
    with sina_M5_origin(symbol, "M5") as data_m5:
        months = data_m5.get_symbol_months()

        if months is None:      #sina_M5_origin data does not exist
            return

        for month in months:
            df_m5 = data_m5.get_contract_by_month(month)
            df_m5.drop_duplicates(keep=False, inplace=True)
            print(df_m5)
            # df_m5 = df_m5.loc[df_m5.index > tm]
            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(df_m5)
            g = df_m5.groupby(pd.Grouper(freq=data.freq_map(), offset='21h', closed='right', label='left'))
            # g = g.filter(lambda x : len(x) > 3)
            # g = g.transform('size')>1
            # g.apply(print)
            df_trans = g.apply(ohlcsum)
            # print(df_trans)
            df_result = df_trans.groupby(pd.Grouper(freq=freq, offset='21h', closed='right', label='left')).agg('last')
            # df_result = df_result.iloc[:-1, :]      #save data till last whole period, delete the last row, which is the error current period
            df_result.dropna(inplace=True)
            print(month, df_result)
            if not rebuild:
                data.append_data(df_result, month)
            else:
                data.overwrite(df_result, month)
        return
