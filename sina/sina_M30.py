from sina.include import SINA_M30_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map

class sina_M30(h5_store):
    def __init__(self, symbol, freq):
        self.symbol = symbol.upper()
        self.exchange = symbol_exchange_map[symbol]
        self.h5_path = "".join([SINA_M30_PATH, self.exchange, "/", symbol, ".hdf5"])

        super(sina_M30, self).__init__(symbol, freq)