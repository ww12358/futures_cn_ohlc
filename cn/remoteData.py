# -*- coding:utf-8 -*-
import tushare as ts
import quandl

class remoteData:
    def __init__(self, keyword, token):

        self.name = keyword

        if keyword is "tushare":
            ts.set_token(token)
            self.feed = ts.pro_api()

            return

        elif keyword is "qunadl":
            quandl.ApiConfig.api_key = token
            self.feed = quandl

            return

        else:
            print("Not a valid data source, or data source not implemented yet. Abort...")

            return