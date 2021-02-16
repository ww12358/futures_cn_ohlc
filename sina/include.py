# -*- coding:utf-8 -*-
from datetime import time, datetime
from dce.include import dce_symbols, dce_symbols_2300pm
from cze.include import cze_symbols, cze_symbols_2300pm
from shfe.include import shfe_symbols, shfe_symbols_2300pm, shfe_symbols_0100am, shfe_symbols_0230am
from ine.include import  ine_symbols, ine_symbols_2300pm, ine_symbols_0100am, ine_symbols_0230am
from cffex.include import cffex_symbols, cffex_symbols_equity, cffex_symbols_bond

SINA_5M_PATH = '/home/sean/sync/creek/sina/'
all_symbols = dce_symbols + cze_symbols + shfe_symbols + ine_symbols + cffex_symbols
com_symbols = dce_symbols + cze_symbols + shfe_symbols + ine_symbols


t_range = { (time(9, 0), time(9, 15)) : com_symbols,
            (time(9, 15), time(9, 30)) : com_symbols + cffex_symbols_bond,
            (time(9, 30), time(10, 15)) : all_symbols,
            (time(10, 15), time(10, 35)) : cffex_symbols,
            (time(10, 35), time(11, 35)) : all_symbols,
            (time(13, 0), time(13, 30)) : cffex_symbols,
            (time(13, 30), time(15, 5)) : all_symbols,
            (time(15, 5), time(15, 30)) : cffex_symbols_bond,
            (time(21, 0), time(23, 5)) : shfe_symbols_2300pm + dce_symbols_2300pm + cze_symbols_2300pm + ine_symbols_2300pm + shfe_symbols_0100am + shfe_symbols_0230am + ine_symbols_0100am + ine_symbols_0230am,
            (time(23, 5), time(1, 0)) : shfe_symbols_0100am + ine_symbols_0100am + shfe_symbols_0230am + ine_symbols_0230am,
            (time(1, 0), time(2, 35)) : shfe_symbols_0230am + ine_symbols_0230am
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

def trading_symbols(t):
    for tm_rng in t_range:
        if time_in_range(tm_rng[0], tm_rng[1], t):
            print(tm_rng[0], tm_rng[1], t_range[tm_rng])
            return t_range[tm_rng]