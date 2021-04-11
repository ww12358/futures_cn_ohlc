from sina.include import SINA_H1_PATH
from cn.h5_store import h5_store

class sina_H1(h5_store):
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol.upper()
        self.exchange = exchange.upper()
        self.h5_path = "".join([SINA_H1_PATH, exchange, "/", symbol, ".hdf5"])

        super(sina_H1, self).__init__(exchange, symbol, freq)