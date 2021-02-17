# -*- coding:utf-8 -*-

import pandas as pd
from cn.include import DATA_PATH_DICT
from cn.h5_store import h5_store
import os

class localData(h5_store):
    def __init__(self, exchange, symbol, freq):
        self.symbol = symbol
        self.h5_path = DATA_PATH_DICT[self.symbol]

        super(localData, self).__init__(exchange, symbol, freq)

        # print(self.h5_path)