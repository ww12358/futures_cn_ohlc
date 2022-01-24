import json
import requests
import pandas as pd
import datetime

def download_sina_data(contract):
    try:
        print(contract)
        # urls = ["http://stock.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract,
        #         "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract]
        url = "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract
        info = requests.get(url)
        if info.status_code != 200:
            print("OK"+contract+"\n")
        data = info.content
        data = json.loads(data)
        data = pd.DataFrame(data)
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(data)
        data = data.sort_values(0)
        data.columns = "date open high low close volume".split()
        data = data.set_index('date')
        data.index = pd.to_datetime(data.index)
        data[["open", "high", "low", "close", "volume"]] = data[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric)
        data['contract'] = contract[-4:]
        print(data)
    # except ValueError as error:
    #     print("Decoding falied")
    #     # raise error.with_traceback(sys.exc_info()[2])
    #     raise ValueError("BUSTERED")
    #     pass

    except Exception as e:
        print("error", str(e))

    return data

def download_sina_data_hq(contract):
    try:
        # print("Download : ", contract)
        # urls = ["http://stock.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract,
        #         "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract]
        url = ('http://hq.sinajs.cn/list=' + contract)
        resp = requests.get(url)  # 获取数据
        data = resp.text.split(',')  # 数据分解成list
        # if info.status_code != 200:
        #     print("OK"+contract+"\n")
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
