# coding: utf-8
from __future__ import absolute_import
import pandas as pd
import numpy as np
from bokeh.plotting import figure
from bokeh.models import ColumnDataSource, HoverTool
from bokeh.io import show
from shfe.include import SHFE_DATA_PATH

symbol = 'CU'
month = u'12'

df = pd.read_hdf(SHFE_DATA_PATH, '/' + symbol + '/D/' + '_' + month)
df = df.reset_index(level=['symbol'])
#print df


inc = df.close > df.open
dec = df.open > df.close

inc_source = ColumnDataSource(df[inc])
dec_source = ColumnDataSource(df[dec])
source = ColumnDataSource(df)

def get_width():
    mindate = min(source.data['date'])
    maxdate = max(source.data['date'])
#    print (maxdate-mindate).item()
#    print len(source.data['date'])
    print (np.timedelta64(1, 'D'))

    return 0.62*(maxdate-mindate)/np.timedelta64(1, 'ms')/len(source.data['date'])

w = 24*60*60*1000 # half day in ms

h = HoverTool(tooltips=[                \
            ('date',    '@date{%F}'),   \
            ('Symbol',  '@symbol'),     \
            ('Open',    '@open'),       \
            ('High',    '@high'),       \
            ('Low',     '@low'),        \
            ('Close',   '@close'),      \
            ('Volume',  '@volume'),     \
            ('OI',      '@oi'),         \
            ('TO',      '@turnover'),
            ],                          \
#        mode="vline",
        formatters={
            'date':     'datetime',     \
            'Symbol':   'printf',       \
            'Open':     'printf',       \
            'High':     'printf',       \
            'Low':      'printf',        \
            'Close':    'printf',      \
            'Volume':   'printf',     \
            'OI':       'printf',         \
            'TO':       'printf',
            }
         )

TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=8000, plot_height=800, title = "SHFE Candlestick")
p.add_tools(h)
seg_call = p.segment(x0="date", y0="high", x1="date", y1="low", line_color="#00E22D", source=inc_source)
seg_put = p.segment(x0="date", y0="high", x1="date", y1="low", line_color="#FF3030", source=dec_source)
vbar_call = p.vbar(x="date", width=get_width(), top="open", bottom="close", fill_color="#00E22D", line_color="#00E22D", source=inc_source)
vbar_put = p.vbar(x="date", width=get_width(), top="open", bottom="close", fill_color="#FF3030", line_color="#FF3030", source=dec_source)
#output_file("candlestick.html", title="candlestick.py example")


show(p)

