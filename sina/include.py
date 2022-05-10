# -*- coding:utf-8 -*-
from datetime import time, datetime
from dce.include import dce_symbols, dce_symbols_2300pm
from cze.include import cze_symbols, cze_symbols_2300pm
from shfe.include import shfe_symbols, shfe_symbols_2300pm, shfe_symbols_0100am, shfe_symbols_0230am
from ine.include import  ine_symbols, ine_symbols_2300pm, ine_symbols_0100am, ine_symbols_0230am
from cffex.include import cffex_symbols, cffex_symbols_equity, cffex_symbols_bond

DEBUG = 0
RUN_NOW = 0
SINA_M5_PATH = '/home/sean/sync/creek/sina/'
SINA_M5_ORIGIN_PATH = '/home/sean/sync/creek/sina_origin/'
SINA_M15_PATH = '/home/sean/sync/creek/M15/'
SINA_M30_PATH = '/home/sean/sync/creek/M30/'
SINA_H1_PATH = '/home/sean/sync/creek/H1/'
SINA_H3_PATH = '/home/sean/sync/creek/H3/'
TQ_PATH = '/home/sean/sync/creek/'
CONTRACT_INFO_PATH = '/home/sean/code/utils/main_contracts.json'
all_symbols = dce_symbols + cze_symbols + shfe_symbols + ine_symbols + cffex_symbols
# exclude_li = ['LR', 'PM', 'RI', 'JR', 'WH', 'RS', 'PF', 'BB', 'FB', 'RR', 'WR']
# all_symbols = [e for e in all_symbols if e not in exclude_li]
com_symbols = dce_symbols + cze_symbols + shfe_symbols + ine_symbols
watch_list = ["CU", "AL", "ZN", "NI", "RB", "HC", "RU", "SN", "PB,"
              "BU", "SP",
              "I", "A", "B", "M", "Y", "P", "V",
              "J", "JM", "L", "PP", "LH", "C", "JD",
              "TA", "EG",  "SR", "CF", "MA", "FG", "ZC", "OI", "RM", "SA", "UR",
              "AG", "AU",
              "SC", "FU"]

all_freq = ["1min", "15min", "30min", "1h", "4h", "1d"]

if DEBUG == 0 or DEBUG == 2:
    REDIS_SVR_ADDR = '192.168.3.11'
else:
    REDIS_SVR_ADDR = '127.0.0.1'
# redis_svr_addr = '127.0.0.1'
REDIS_PORT = '6379'
REDIS_DB = 1

t_range = { (time(2, 30), time(15, 30)) : com_symbols,
            (time(15, 30), time(23, 0)) : shfe_symbols_2300pm + dce_symbols_2300pm + cze_symbols_2300pm + ine_symbols_2300pm + shfe_symbols_0100am + shfe_symbols_0230am + ine_symbols_0100am + ine_symbols_0230am,
            (time(23, 0), time(1, 0)) : shfe_symbols_0100am + ine_symbols_0100am + shfe_symbols_0230am + ine_symbols_0230am,
            (time(1, 0), time(2, 30)) : shfe_symbols_0230am + ine_symbols_0230am
}

def time_in_range(start, end, x):
    """Return true if x is in the range [start, end]"""
    if start <= end:
        return start <= x < end
    else:
        return start <= x or x < end

# class trading_symbols:
#     def __init__(self, t):
#         self._t_range = None
#
#         for tm_rng in t_range:
#             if time_in_range(tm_rng[0], tm_rng[1], t):
#                 print(tm_rng[0], tm_rng[1])
#                 # return t_range[tm_rng]
#                 self._t_range = t_range[tm_rng]
#
#     def __enter__(self):
#         return self
#
#     def __exit__(self, exc_type, exc_val, exc_tb):
#         del self
#
#     def get_t_range(self):
#         return self._t_range

def trading_symbols(debug, t, t_symbols, WATCH_LIST=True):
    t_symbols.clear()
    if debug == 1:
        # return ['SC']
        t_symbols.extend(['SC', 'CU', 'P', 'SR', 'RU'])
        return
        # return ['FG', 'SR']
    elif debug == 2:
        t_symbols.extend(["MA", "ZC"])
        return
    else:
        for tm_rng in t_range:
            if time_in_range(tm_rng[0], tm_rng[1], t):
                print(tm_rng[0], tm_rng[1], t_range[tm_rng])
                if WATCH_LIST:
                    t_symbols.extend(list(set(watch_list)&set(t_range[tm_rng])))
                else:
                    t_symbols.extend(t_range[tm_rng])

                print("current running symbols: ", t_symbols)

                return
