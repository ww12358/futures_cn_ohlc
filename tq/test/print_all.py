from tqsdk import TqApi, TqAuth

api = TqApi(web_gui=True, auth=TqAuth("15381188725", "mancan@07"))
map = {k: v for k, v in api._data["quotes"].items() if not k.startswith("KQ") and v["expired"] == False}


def split2exprod(exchinstr: str):
    exchange, instr = exchinstr.split('.')
    product = "".join(filter(str.isalpha, instr))
    l = [exchange, product]
    return l


result = {}
for k, v in map.items():
    if v.ins_class == 'FUTURE_OPTION':
        rl = split2exprod(v.underlying_symbol)
    elif v.ins_class == 'FUTURE':
        rl = split2exprod(k)
    else:
        pass  # pass FUTURE_COMBINE
    if rl[0] not in result.keys():
        result[rl[0]] = {rl[1]: v.trading_time}
    else:
        if rl[1] not in result[rl[0]].keys():
            result[rl[0]].update({rl[1]: v.trading_time})

for exch, v in result.items():
    print("交易所:", exch)
    for p, t in v.items():
        print("品种: ", p, " 交易时间: 日盘 ", tuple(t['day']), " 夜盘 ", t['night'])

api.close()
