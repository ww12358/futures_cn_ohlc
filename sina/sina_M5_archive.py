import redis
import pyarrow as pa
import datetime
from sina.include import trading_symbols, DEBUG, all_symbols
from sina.tq_mh import tq_mh


def archive_sina_M5(contract_dict):
    # delta = datetime.timedelta(minutes=1)      #delay for 1 circle
    # t = (datetime.datetime.now() - delta).time()

    t = datetime.datetime.now().time()
    t_symbols = trading_symbols(DEBUG, t)

    if t_symbols is None:
        return

    # print("Saving below contracts: ", t_symbols)

    r = redis.Redis(host='localhost', port=6379, db=0)
    for symbol in t_symbols:
        contract_d = contract_dict[symbol]
        contract_d.update({"00":(symbol+"00")})     # append index as updating contract
        # print(contract_d)
        with tq_mh(symbol, "5min") as local_5m_data:
            for month, contract in contract_d.items():
                try:
                    # print("Saving : ", contract)
                    ser = r.get((contract))

                    if not ser:
                        # print(symbol, "not find in buffer. Skip...")
                        continue

                    # d = local_5m_data.get_contract_by_month(month)
                    # print(d)
                    df = pa.deserialize(ser)
                    # print(df)
                    if df.empty:
                        continue
                    else:
                        # print(df)
                        # print(d)
                        local_5m_data.append_data(df, month)

                except Exception as e:
                    print(str(e))
                    pass

        # print("Archive finished at : ", datetime.datetime.now())

    return

# contract_dict = getAllContractDict()
# archive_sina_5m(contract_dict)

