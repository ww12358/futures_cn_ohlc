import pandas as pd
import datetime
from tq.config import api
def download_tq(contract):
    data = pd.DataFrame()
    try:
        if len(data) > 1:
            data_l = [
                #                 datetime.strptime(data[17], "%Y-%m-%d"),    #date
                datetime.datetime.now().replace(second=0, microsecond=0),        #round instantaneous time to minute
                pd.to_numeric(data[2]),  # open
                pd.to_numeric(data[3]),  # high
                pd.to_numeric(data[4]),  # low
                pd.to_numeric(data[8]),  # close
                pd.to_numeric(data[13]),  # open interest
                pd.to_numeric(data[14]),  # volume
            ]
            #         data_flist = list(map(float, data_list))     # 字符串转换成浮点数据
            df = pd.DataFrame([data_l], columns=['date', 'open', 'high', 'low', 'close', 'oi', 'volume'])
            df.set_index("date", inplace=True)
            # print(df)
            return df

    except Exception as e:
        print("error", str(e))
        return None

# k_serial = api.get_kline_serial("SHFE.cu2107", 60)
# print(k_serial)
quote = api.get_quote("SHFE.cu2107")
print(quote)