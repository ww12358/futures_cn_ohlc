from sina.include import SINA_M5_ORIGIN_PATH
from cn.h5_store import h5_store

class sina_M5_origin(h5_store):
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol.upper()
        self.exchange = exchange.upper()
        self.h5_path = "".join([SINA_M5_ORIGIN_PATH, exchange, "/", symbol, ".hdf5"])

        super(sina_M5_origin, self).__init__(exchange, symbol, freq)