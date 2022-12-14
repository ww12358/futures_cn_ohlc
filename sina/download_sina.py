import json
import requests
import pandas as pd
import datetime

urls = ["http://stock.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=",    \
        "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol="]

def switch_url(s):
    if s == 0:
        return 1
    if s == 1:
        return 0
    else:
        print("Error switch url...")
        return 0

def download_sina_data(contract, url_switch):
    count = 0
    try:
        print(contract)
        while True:
            try:
                info = requests.get(urls[url_switch] + contract)
                if info.status_code != 200:
                    print(contract+" data received.\n")

                data = info.content
                data = json.loads(data)
                break

            except Exception as e:
                count += 1
                if count == 3:
                    count = 0
                    break

                url_switch = switch_url(url_switch)
                # print("switching url", e)

        data = pd.DataFrame(data)
        # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
        #     print(data)
        data = data.sort_values(0)
        data.columns = "date open high low close volume".split()
        data = data.set_index('date')
        data.index = pd.to_datetime(data.index)
        data[["open", "high", "low", "close", "volume"]] = data[["open", "high", "low", "close", "volume"]].apply(pd.to_numeric)
        data['contract'] = contract[-4:]
        # print(data)
    # except ValueError as error:
    #     print("Decoding falied")
    #     # raise error.with_traceback(sys.exc_info()[2])
    #     raise ValueError("BUSTERED")
    #     pass

    except Exception as e:
        print("error", str(e))

    return data, url_switch

def download_sina_data_hq(contract):
    try:
        # print("Download : ", contract)
        # urls = ["http://stock.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract,
        #         "http://stock2.finance.sina.com.cn/futures/api/json.php/IndexService.getInnerFuturesMiniKLine5m?symbol=" + contract]
        url = ('http://hq.sinajs.cn/list=' + contract)
        resp = requests.get(url)  # ????????????
        data = resp.text.split(',')  # ???????????????list
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
            #         data_flist = list(map(float, data_list))     # ??????????????????????????????
            df = pd.DataFrame([data_l], columns=['date', 'open', 'high', 'low', 'close', 'oi', 'volume'])
            df.set_index("date", inplace=True)
            # print(df)
            return df

    except Exception as e:
        print("error", str(e))
        return None
