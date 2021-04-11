import pandas as pd

def get_list_delist_dates(symbol, exchange, contracts, df_basics):
#    print(df_basics)
#    print(contracts)
    contract_delist_date = {}
    contract_list_date = {}
    for year, month in contracts:
#        print(year, month)
        contract = year + month
        smbl_str = symbol + year + month + '.' + exchange
#        print(smbl_str)
        try:
            contract_delist_date[
                pd.to_datetime(df_basics.loc[df_basics["ts_code"] == smbl_str, "delist_date"].values[0])] = contract
            contract_list_date[
                pd.to_datetime(df_basics.loc[df_basics["ts_code"] == smbl_str, "list_date"].values[0])] = contract
        except IndexError:
            #            print("Contract not listed yet")
            continue

    return contract_list_date, contract_delist_date