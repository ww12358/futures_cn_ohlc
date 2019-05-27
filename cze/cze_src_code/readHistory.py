# coding: utf-8

import pandas as pd
import os
from include import DATA_SRC_PATH, cze_headers, cze_dtypes


def readSrcData(y):

    f_name = ''.join(['zce-20', y,'.txt'])
    FULL_PATH = os.path.join(DATA_SRC_PATH, f_name)

    df = pd.read_csv(FULL_PATH, '|', header=None, skipinitialspace=True, names=cze_headers, parse_dates=['date'], dtype=cze_dtypes)
    a = len(df.columns) - 1         # the '|' at the end of line causes an empty column, delete it
    df = df.iloc[:, :a]
#    df.set_index(["date"], inplace=True, drop=True)

    return df

