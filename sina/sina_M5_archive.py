import pandas as pd
from cn.include import symbol_exchange_map
from sina.getContractDict import getContractDict, getAllContractDict
import redis
import pyarrow as pa
import datetime
from sina.include import trading_symbols
from sina.sina_M5 import sina_M5

DEBUG = 1

def archive_sina_M5(contract_dict):
    # delta = datetime.timedelta(minutes=1)      #delay for 1 circle
    # t = (datetime.datetime.now() - delta).time()

    t = datetime.datetime.now().time()
    t_symbols = trading_symbols(t)


    if t_symbols is None:
        return

    print("Saving below contracts: ", t_symbols)

    r = redis.Redis(host='localhost', port=6379, db=0)
    for symbol in t_symbols:
    # for symbol in contract_dict.keys():
        contract_d = contract_dict[symbol]
        contract_d.update({"00":(symbol+"00")})     # append index as updating contract
        # print(contract_d)
    # for symbol, contract_d in contract_dict.items():
        exchange = symbol_exchange_map[symbol]
        with sina_M5(exchange, symbol, "M5") as local_5m_data:
            # if local_5m_data.isempty():     #local file contains no data
            #     for month, contract in contract_d.items():
            #         try:
            #             print(contract)
            #             ser = r.get((contract))
            #             if not ser:
            #                 print(symbol, " not find in buffer. Skip...")
            #                 continue
            #             df = pa.deserialize(ser)
            #             print(df)
            #             if not df is None:
            #                 print("writing new dataframe to sian_5M_local\n", df)
            #                 local_5m_data.save_contract(df, exchange, symbol, "M5", month)
            #             else:
            #                 print(symbol + " data not updating within redis. Skip...")
            #                 continue
            #         except Exception as e:
            #             print("Error ocurred while creating local archive", str(e))
            #             pass

            # else:
            for month, contract in contract_d.items():
                try:
                    print("Saving : ", contract)
                    ser = r.get((contract))

                    if not ser:
                        print(symbol, "not find in buffer. Skip...")
                        continue

                    d = local_5m_data.get_contract_by_month(month)
                    # print(d)
                    df = pa.deserialize(ser)
                    if not df is None:      #redis buffer exists
                        # print(df)
                        if d is not None:   #sina_5m local data contains current contract
                            start_time = d.index[-1]
                            df = df.loc[df.index > start_time]
                        else:
                            if DEBUG:
                                print(df)
                            else:
                                local_5m_data.save_contract(df, exchange, symbol, "M5", month)  #current contract cannot find in local file, just save it
                            continue
                        # print(df)

                        if not df.empty:
                            if DEBUG:
                                print(df)
                            else:
                            # if month == "00":
                            #     print("here")
                                local_5m_data.append_data(df, exchange, symbol, "M5", month)
                        else:
                            continue
                        # if not df is None:
                        #     print(df)
                        #     pass
                    else:
                        print(contract + " data not updating within redis. Skip...")
                        continue

                except Exception as e:
                    print(str(e))
                    pass

        print("Archive finished at : ", datetime.datetime.now())

    return

# contract_dict = getAllContractDict()
# archive_sina_5m(contract_dict)

