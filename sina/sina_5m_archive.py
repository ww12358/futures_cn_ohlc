import pandas as pd
from sina.include import SINA_5M_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map
from sina.getContractDict import getContractDict, getAllContractDict
import redis
import pyarrow as pa
import datetime
from sina.include import trading_symbols

class sina_5m(h5_store):
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol.upper()
        self.exchange = exchange.upper()
        self.h5_path = "".join([SINA_5M_PATH, exchange, "/", symbol, ".hdf5"])

        super(sina_5m, self).__init__(exchange, symbol, freq)
        # print(self.h5_path)

def archive_sina_5m(contract_dict):
    # delta = datetime.timedelta(minutes=1)      #delay for 1 circle
    # t = (datetime.datetime.now() - delta).time()
    t = datetime.datetime.now()
    t_symbols = trading_symbols(t)
    print(t_symbols)

    if t_symbols is None:
        return

    r = redis.Redis(host='localhost', port=6379, db=0)
    for symbol in t_symbols:
        contract_d = contract_dict[symbol]
        contract_d.update({"00":(symbol+"00")})     # append index as updating contract
        # print(contract_d)
    # for symbol, contract_d in contract_dict.items():
        exchange = symbol_exchange_map[symbol]
        with sina_5m(exchange, symbol, "M5") as local_5m_data:
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
                    # print(contract)
                    ser = r.get((contract))

                    if not ser:
                        print(symbol, "not find in buffer. Skip...")
                        continue

                    d = local_5m_data.get_contract_by_month(month)
                    # print(d)

                        # start_time = datetime.datetime(1970, 1, 1)

                    # print(start_time)
                    # if not ser is None:
                    df = pa.deserialize(ser)
                    if not df is None:      #redis buffer exists
                        # print(df)
                        if d is not None:   #sina_5m local data contains current contract
                            start_time = d.index[-1]
                            df = df.loc[df.index > start_time]
                        else:
                            local_5m_data.save_contract(df, exchange, symbol, "M5", month)  #current contract cannot find in local file, just save it
                            continue
                        # print(df)

                        if not df.empty:
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

    return

# contract_dict = getAllContractDict()
# archive_sina_5m(contract_dict)

