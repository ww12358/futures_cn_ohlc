# -*- coding:utf-8 -*-
from cn.localData import localData
from cn.qlData import qlData
from cn.tsData import tsData
from cn.updateCN import update_cn_latest, update_cn, newContracts
from cn.include import symbol_exchange_map, ex_symList_map, all_exchanges, all_symbols, exchange_symbols_map
from cn.config import TUSHARE_TOKEN, QUANDL_TOKEN
import click
import tushare as ts
import quandl
from datetime import datetime
import re
import pandas as pd
import time

def monthlist_till_today(start, end):
#    start = datetime.strptime(start_date, "%Y-%m-%d")
#    end = datetime.now()
    total_months = lambda dt: dt.month + 12 * dt.year
    mlist = []
    for tot_m in range(total_months(start)-1, total_months(end)+12):
        y, m = divmod(tot_m, 12)
#        mlist.append(datetime(y, m+1, 1).strftime("%y%m"))
        if m in range(0, 9):
            mlist.append((str(y), '0'+str(m+1)))
        else:
            mlist.append((str(y), str(m+1)))

    #could be improved with delist_date, avoid last month try. to be continued...
    return mlist


def first_last_trd_day(symbol, df_basics):
    pat = re.escape(symbol) + r'\d{4}'
    dfa = df_basics.loc[df_basics["symbol"].str.match(pat)].copy()
    dfa.loc[:, "list_date"] = pd.to_datetime(dfa["list_date"].str.strip(), format="%Y%m%d")
    dfa.loc[:, "delist_date"] = pd.to_datetime(dfa["delist_date"].str.strip(), format="%Y%m%d")
    dfa = dfa.sort_values(by="list_date")
#    print(dfa)
    first_day = dfa["list_date"].min()
    last_day = dfa["delist_date"].max()

    return first_day, last_day

@click.command()
@click.option("--symbol", "-s",
              type=click.STRING,
              help='The symbol of a trading contract, or "all" is required for command')
@click.option("--exchange", "-e",
              type=click.STRING)
@click.option("--year", "-y", type=click.INT, help="Year")
@click.option("--month", "-m", type=click.STRING, help="month")
@click.option("--latest", "-u", is_flag=True, help="up-to-date")
@click.option("--new", "-n", is_flag=True, help="to import new contract")
@click.option("--clean", "-c", is_flag=True, help="Clean data")
@click.option("--source", "-r", type=click.STRING)
def main(symbol, exchange, year, month, latest, new, clean, source="T"):
    #configure data source
    if source in ["quandl", "Q"]:
        print("Using quandl as data source. Continue...")
        quandl.ApiConfig.api_key = QUANDL_TOKEN
        remote_data = qlData(quandl, exchange, symbol, "D")

    elif source in ["tushare", "T"]:
        print("Using tushare as data source. Continue...")
        ts.set_token(TUSHARE_TOKEN)
        pro = ts.pro_api()
    else:
        print("Not a valid data source, or data source not implemented yet. Abort...")
        return

    if exchange:
        exchange = exchange.strip().upper()
        if exchange in all_exchanges:
            basics_df = pro.fut_basic(exchange=exchange, fut_type='1', fields='ts_code,symbol,list_date,delist_date')
            if latest:
                for smbl in ex_symList_map[exchange]:
                    # print(exchange, smbl)
                    with localData(smbl, "D") as local_data:
                        print(local_data.symbol)
                        remote_data = tsData(pro, exchange, smbl, "D")
                        update_cn_latest(exchange, smbl, "D", local_data, remote_data, basics_df)
                        # time.sleep(60)
                print("Exchange {ex_name} updated. Quit with success.")
                return
        else:
            print("Incorrect exchange. Quit...")
            return

    if symbol:
        symbol = symbol.strip().upper()


    if symbol == "ALL":         #update all symbols
        if latest:
            for exchange in exchange_symbols_map.keys():
                basics_df = pro.fut_basic(exchange=exchange, fut_type='1', fields='ts_code,symbol,list_date,delist_date')
                for smbl in exchange_symbols_map[exchange]:
                    print("Exchange:{ex}, Symbol:{smbl}".format(ex=exchange, smbl=smbl))
                    with localData(smbl, "D") as local_data:
                        remote_data = tsData(pro, exchange, smbl, "D")
                        update_cn_latest(exchange, smbl, "D", local_data, remote_data, basics_df)
                    # time.sleep(1)
            return

        if clean:
            for smbl in all_symbols:
 #               cleanse_shfe_data(smbl)
                print(smbl)

            print("Data cleaned with success.")
            return

    elif symbol in all_symbols:           #update single symble
        exchange = symbol_exchange_map[symbol]
        print("Exchange:{ex}, Symbol:{smbl}".format(ex=exchange, smbl=symbol))
        basics_df = pro.fut_basic(exchange=exchange, fut_type='1', fields='ts_code,symbol,list_date,delist_date')
        remote_data = tsData(pro, exchange, symbol, "D")
        with localData(symbol, "D") as local_data:
            if latest:
                print("Updating %s to latest..." % symbol)
                update_cn_latest(exchange, symbol, "D", local_data, remote_data, basics_df)
                # del local_data
                return

            if new:
                print("Import %s as new contract" % symbol)
                newContracts(exchange, symbol, "D", local_data, remote_data, basics_df)
                return

            if clean:
                print("Cleaning %s data..." % symbol)
                #                cleanse_shfe_data(symbol)
                print("Data cleaning finished with success! Done.")
                return

            if year and 8 <= year <= (datetime.now().year - 1999):
                #        print("good year")
                #        print(year_s)
                with localData(symbol, "D") as local_data:
                    months = local_data.get_symbol_months()
                    if month in months:
                        update_cn(exchange, symbol, "D", str(year), month, local_data, remote_data)
                        return

                    elif month is None:
                        for month in months:
                            update_cn(exchange, symbol, "D", str(year), month, local_data, remote_data)
                        return

            elif year is None:
                print("all years")
                with localData(symbol, "D") as local_data:
                    months = local_data.get_symbol_months()
                    if month in months:
                        for y in range(2000 + year, datetime.now().year + 1):
                            update_cn(exchange, symbol, str(y), "D", month, local_data, remote_data)
                        return

                    elif month is None:  # update all data till today
                        while True:
                            answer = input(
                                "Are you sure you want to update ALL local data?('Y/yes' or 'N/no')   ").strip().lower()
                            #       print("answer is ", answer)
                            if answer in ("yes", "y"):
                                first_list_day, last_delist_day = first_last_trd_day(symbol, basics_df)
                                mlist = monthlist_till_today(first_list_day)
                                #                    print(mlist)
                                for ele in mlist:
                                    #                        print(ele[0], ele[1])
                                    update_cn(exchange, symbol, "D", ele[0], ele[1], local_data, remote_data)
                                return

                            else:
                                return
    else:
        print("Not a valid symbol was specified. Please check. Aborting...")

if __name__ == "__main__":
    main()