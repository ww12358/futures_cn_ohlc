import pandas as pd
from sina.include import SINA_5M_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map
from sina.getContractDict import getContractDict, getAllContractDict
import redis
import pyarrow as pa

class sina_5m(h5_store):
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol.upper()
        self.exchange = exchange.upper()
        self.h5_path = "".join([SINA_5M_PATH, exchange, "/", symbol, ".hdf5"])

        super(sina_5m, self).__init__(exchange, symbol, freq)

        print(self.h5_path)

def archive_sina_5m(contract_dict):
    r = redis.Redis(host='localhost', port=6379, db=0)
    for symbol, contract_d in contract_dict.items():
        exchange = symbol_exchange_map[symbol]
        local_5m_data = sina_5m(exchange, symbol, "5m")
        for month, contract in contract_d.items():
            try:
                ser = r.get((contract))
                if not ser is None:
                    df = pa.deserialize(ser)
                else:
                    print(symbol + " data not updating within redis. Skip...")
                    pass

                local_5m_data.save_contract(df, exchange, symbol, "M5", month)

            except Exception as e:
                print(str(e))
                pass

            if not df is None:
                print(df)
                pass
    return

contract_dict = getAllContractDict()
archive_sina_5m(contract_dict)

