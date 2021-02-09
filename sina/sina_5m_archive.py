import pandas as pd
from sina.include import SINA_5M_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map
from sina.getContractDict import getContractDict, getAllContractDict
import redis
import pyarrow as pa
import datetime

class sina_5m(h5_store):
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol.upper()
        self.exchange = exchange.upper()
        self.h5_path = "".join([SINA_5M_PATH, exchange, "/", symbol, ".hdf5"])

        super(sina_5m, self).__init__(exchange, symbol, freq)
        # print(self.h5_path)

def archive_sina_5m(contract_dict):

    r = redis.Redis(host='localhost', port=6379, db=0)
    for symbol, contract_d in contract_dict.items():
        exchange = symbol_exchange_map[symbol]
        with sina_5m(exchange, symbol, "M5") as local_5m_data:
            if local_5m_data.isempty():
                for month, contract in contract_d.items():
                    try:
                        print(contract)
                        ser = r.get((contract))
                        if not ser:
                            print(symbol, " not find in buffer. Skip...")
                            continue
                        df = pa.deserialize(ser)
                        if not df is None:
                            print("writing new dataframe to sian_5M_local\n", df)
                            local_5m_data.save_contract(df, exchange, symbol, "M5", month)
                        else:
                            print(symbol + " data not updating within redis. Skip...")
                            continue
                    except Exception as e:
                        print("Error ocurred while creating local archive", str(e))
                        pass

            else:
                for month, contract in contract_d.items():
                    try:
                        print(contract)
                        ser = r.get((contract))

                        if not ser:
                            print(symbol, "not find in buffer. Skip...")
                            continue

                        d = local_5m_data.get_contract_by_month(month)
                        # print(d)
                        if d is not None:
                            start_time = d.index[-1]
                        else:
                            start_time = datetime.datetime(1970, 1, 1)
                        print(start_time)
                        # if not ser is None:
                        df = pa.deserialize(ser)
                        if not df is None:
                            # print(df)
                            df = df.loc[df.index > start_time]
                            # print(df)

                            if not df.empty:
                                local_5m_data.append_data(df, exchange, symbol, "M5", month)
                            else:
                                continue
                            # if not df is None:
                            #     print(df)
                            #     pass
                        else:
                            print(symbol + " data not updating within redis. Skip...")
                            continue

                    except Exception as e:
                        print(str(e))
                        pass

    return

# contract_dict = getAllContractDict()
# archive_sina_5m(contract_dict)

