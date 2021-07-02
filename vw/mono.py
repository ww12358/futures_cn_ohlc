# -*- coding:utf-8 -*-

import pandas as pd
from datetime import datetime
import click
from vw.include import ex_config, idx_headers, idx_dtypes
from cn.include import all_symbols, all_exchanges, symbol_exchange_map, exchange_symbols_map
from cn.localData import localData
# from sina.sina_M5_archive import sina_M5
from sina.sina_H3 import sina_H3
from sina.sina_H1 import sina_H1
from sina.sina_M15 import sina_M15
from sina.sina_M30 import sina_M30
from sina.sina_M5 import sina_M5
from sina.sina_M5_origin import sina_M5_origin
from sina.include import watch_list
import numpy as np


# def wavg(sub_df, avg_col, weight_col):
#     d = sub_df[avg_col]
#     w = sub_df[weight_col]
#     try:
#         return (d * w).sum() / w.sum()
#     except ZeroDivisionError:
#         return d.mean()
#     except Exception as e:
#         print(str(e))
#         return None


# def agregate_month_data(symbol, df_all, start_date):
# #    print(start_date)
#     df_all = df_all.loc[df_all.index.get_level_values("date") > start_date]
# #    print(df_all)
#
#     df_append=pd.DataFrame(columns=idx_dtypes)
#     df_row = pd.DataFrame(columns=idx_dtypes)
#     for date, sub_df in df_all.groupby(level='date'):
# #        print(date)
# #        print(sub_df)
#         df_row.loc[0, "date"] = pd.to_datetime(date)
#         df_row.loc[0, "symbol"] = symbol + "0000"
#         for col in ["open", "high", "low", "close", "settlement"]:
#             df_row.loc[0, col] = wavg(sub_df, col, "volume")
#         for col in ["volume", "turnover", "oi"]:
#             df_row.loc[0, col] = sub_df[col].sum(axis=0)
# #        print(df_row)
#         df_append = df_append.append(df_row, ignore_index=True)
#
#     df_append = df_append[idx_headers]
#     df_append = df_append.astype(idx_dtypes)
#     df_append.set_index("date", inplace=True)
#
#     print(df_append)
#
#     return df_append

# def aggr_contracts(symbol, dfs, start_date):
#
#     df_concat = pd.concat(dfs, axis=0)
#     df_concat = df_concat[(df_concat["volume"] > 0) & (df_concat.index.get_level_values("date") > start_date)]
#     g = df_concat.groupby(level='date', sort=True)
#     df_append = pd.concat([
#         g.apply(lambda x: np.average(x['open'], weights=x['volume'])),
#         g.apply(lambda x: np.average(x['high'], weights=x['volume'])),
#         g.apply(lambda x: np.average(x['low'], weights=x['volume'])),
#         g.apply(lambda x: np.average(x['close'], weights=x['volume'])),
#         g.apply(lambda x: np.average(x['settlement'], weights=x['volume'])),
#         g.apply(lambda x: np.sum(x['volume'])),
#         g.apply(lambda x: np.sum(x['turnover'])),
#         g.apply(lambda x: np.sum(x['oi'])),
#     ],
#         axis=1, keys=['open', 'high', 'low', 'close', 'settlement', 'volume', 'turnover', 'oi'])
#     df_append['symbol'] = ''.join([symbol, "0000"])
#     df_append = df_append[['symbol', 'open', 'high', 'low', 'close', 'settlement', 'volume', 'turnover', 'oi']]
#     print(df_append)
#
#     return df_append


def genMonoIdx(symbol, freq="1d", f_rebuild=False, f_dry_run=False):

    if freq == "D":
        with localData(symbol, "D") as data:
            # print(data.get_idx_data())
            data.generate_idx(f_rebuild, f_dry_run)
    elif freq == "H3":
        with sina_H3(symbol, "H3") as data:
            data.generate_idx(f_rebuild, f_dry_run)
    elif freq == "H1":
        with sina_H1(symbol, "H1") as data:
            data.generate_idx(f_rebuild, f_dry_run)
    elif freq == "M15":
        with sina_M15(symbol, "M15") as data:
            data.generate_idx(f_rebuild, f_dry_run)
    elif freq == "M30":
        with sina_M30(symbol, "M30") as data:
            data.generate_idx(f_rebuild, f_dry_run)
    elif freq == "M5":
        with sina_M5_origin(symbol, "M5") as data:
            data.generate_idx(f_rebuild, f_dry_run)
    elif freq == "M4":
        with sina_M5(symbol, "M5") as data:
            data.generate_idx(f_rebuild, f_dry_run)
    elif freq == "M1":
        with sina_M5(symbol, "M5") as data:
            data.generate_idx(f_rebuild, f_dry_run)
    else:
        print("Unknow symbol {symbol} or freqency {freq}" )
        return

    print("%s%s updated.\n" % (symbol, '00'))
    return

## main
@click.command()
@click.option("--symbol", "-s",
              type=click.STRING,
              help="symbol")
@click.option("--exchange", "-e",
              type=click.STRING,
              )
@click.option("--freq", "-f",
              type=click.STRING,
              )
@click.option("--major", "-M", is_flag=True, help="generate only major contract indexes")
@click.option("--rebuild", "-R", is_flag=True, help="to rebuild 00 data")
@click.option("--dry_run", "-D", is_flag=True, help="do not acctually save data if True")
def main(symbol, exchange, freq, major, rebuild, dry_run):

    # print(f"rebuild {rebuild}, dry_run {dry_run}")
    if symbol:
        symbol = symbol.strip().upper()
        if symbol == "ALL":
            for ex in all_exchanges:
                for smbl in exchange_symbols_map[ex]:
                    genMonoIdx(smbl, freq, rebuild, dry_run)

        elif symbol in all_symbols:
            # exchange = symbol_exchange_map[symbol]
            genMonoIdx(symbol, freq, rebuild, dry_run)
            return

        else:
            print("not a valid symbol")
            return

    if exchange:
        exchange = exchange.strip().upper()

        if exchange in all_exchanges:
            for symbol in exchange_symbols_map[exchange]:
                print(exchange, symbol)
                genMonoIdx(symbol, freq, rebuild, dry_run)

    if major:
        print("Generating watch list indexes...")
        for smb in watch_list:
            genMonoIdx(smb, freq, rebuild, dry_run)

        return

if __name__ == "__main__":
    main()

