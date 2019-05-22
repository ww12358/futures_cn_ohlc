# -*- coding:utf-8 -*-

import quandl

class qlData:
    def __init__(self, ql, exchange, symbol, freq):
        self.name = "quandl"
        self.symbol = symbol
        self.feed = ql
        return