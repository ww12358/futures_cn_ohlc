from cn.tsData import tsData
from cn.updateCN import update_cn_latest

import sys
import os
import pandas as pd
import tushare as ts
import json
from functools import reduce

module_path = os.path.abspath(os.path.join('/home/sean/code/play_with_data/utils'))
if module_path not in sys.path:
    sys.path.append(module_path)

CONFIG_FILE = "/home/sean/code/play_with_data/utils/rules-v0.10.json"

with open(CONFIG_FILE, "r") as f:
    rules = json.load(f)

print(rules["RB"])

def dump_json():
    return
