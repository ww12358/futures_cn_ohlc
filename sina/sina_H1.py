from sina.include import SINA_H1_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map

class sina_H1(h5_store):
    def __init__(self, symbol, freq):
        self.symbol = symbol.upper()
        # self.exchange = exchange.upper()
        self.exchange = symbol_exchange_map[symbol]
        self.h5_path = "".join([SINA_H1_PATH, self.exchange, "/", symbol, ".hdf5"])

        super(sina_H1, self).__init__(symbol, freq)