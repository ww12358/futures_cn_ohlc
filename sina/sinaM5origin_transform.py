import pandas as pd
import datetime
import click
from cn.include import all_symbols

from sina.sina_M5_origin import sina_M5_origin
from sina.sina_H1 import sina_H1
from sina.sina_H3 import sina_H3
from sina.sina_M15 import sina_M15
from sina.sina_M30 import sina_M30
from cn.localData import localData

watch_list = ["CU", "RB", "I", "A", "M", "Y", "TA", "SR", "CF"]

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
            # 'contract': data['contract']
        }, index=data.index)

def freq_map(self):
    freq_dic = {"M5": "5min", "H1":"1h", "H3":"3h", "D":"1d"}
    print(freq_dic[self.freq])

    return freq_dic[self.freq]

def convert_sinaM5origin(symbol, freq, data, rebuild):
    with sina_M5_origin(symbol, "M5") as data_m5:
        months = data_m5.get_symbol_months()

        if months is None:      #sina_M5_origin data does not exist
            return

        for month in months:
            df_m5 = data_m5.get_contract_by_month(month)
            # print(df_m5)
            g = df_m5.groupby(pd.Grouper(freq=data.freq_map(), offset='21h', closed='right', label='left'))
            #         g.apply(print)
            df_trans = g.apply(ohlcsum)
            df_result = df_trans.groupby(pd.Grouper(freq=freq, offset='21h', closed='right', label='left')).agg('last')
            df_result.dropna(inplace=True)
            print(month, df_result)
            if not rebuild:
                data.append_data(df_result, month)
            else:
                data.overwrite(df_result, month)
        return

def trans_freq(symbol, freq, rebuild):
    print(symbol)
    if freq in ["5mim", "15min", "30min", "1h", "3h", "1d"]:
        pass
    else:
        print("Invalid frequency. Quit.")
        return

    try:
        if freq == "15min":
            with sina_M15(symbol, "M15") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

        elif freq == "30min":
            with sina_M30(symbol, "M30") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

        elif freq == "1h":
            with sina_H1(symbol, "H1") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

        elif freq == "3h":
            with sina_H3(symbol, "H3") as data:
                convert_sinaM5origin(symbol, freq, data, rebuild)

    except Exception as e:
        print(str(e))
        pass


@click.command()
@click.option("--symbol", "-S", type=click.STRING, help="contract symbol")
@click.option("--rebuild", "-R", is_flag=True, help="rebuild all data rows")
@click.option("--all", "-A", is_flag=True, help="download all data")
@click.option("--major", "-M", is_flag=True, help="download important data only")
@click.option("--freq", "-F",  type=click.STRING, help="freqency inlcude 5min, 15min, 30min, 1h, 3h, 1d")

def main(all, major, symbol, freq, rebuild=False):

    if all:
        for smbl in all_symbols:
            trans_freq(smbl, freq, rebuild)
        return

    elif major:
        for smbl in watch_list:
            trans_freq(smbl, freq, rebuild)
        return

    else:
        symbol = symbol.strip().upper()
        if symbol in all_symbols:
            trans_freq(symbol, freq, rebuild)
            return
        else:
            print("not a valid symbol")
        return




if __name__ == "__main__":
    main()

