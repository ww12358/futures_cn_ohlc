from tqsdk import TqApi, TqSim, TqAuth
# import asyncio
# import concurrent.futures
# import nest_asyncio
# nest_asyncio.apply()
import pandas as pd

api = TqApi(auth=TqAuth("15381188725", "mancan@07"))

# api = TqApi(TqSim(), url="ws://192.168.3.95:7777")

# quote_li = ["SHFE.rb1910"]
# quote = api.get_quote("SHFE.rb1910")

k_serial = api.get_kline_serial("SHFE.cu2107", 60)
# print(k_serial)
while True:
    api.wait_update()
    print(k_serial.iloc[-1].close)