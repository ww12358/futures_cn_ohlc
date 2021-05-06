from sina.include import SINA_M5_ORIGIN_PATH, SINA_H1_PATH, SINA_H3_PATH
from cn.h5_store import h5_store
from cn.include import symbol_exchange_map

class sina_M5_origin(h5_store):
    def __init__(self, symbol, freq):
        self.symbol = symbol.upper()
        self.exchange = symbol_exchange_map[symbol]
        # self.exchange = exchange.upper()
        self.h5_path = "".join([SINA_M5_ORIGIN_PATH, self.exchange, "/", symbol, ".hdf5"])

        super(sina_M5_origin, self).__init__(symbol, freq)

    def to_H1(self):
        pass
