# coding: utf-8
from math import pi
import pandas as pd
from bokeh.plotting import figure, show, output_file
from shfe.shfe_src_code.include import TEST_DATA_PATH

symbol = 'CU'
month = '09'

df = pd.read_hdf(TEST_DATA_PATH, '/'+symbol+'/D/'+'_'+month)
df = df.reset_index(level=['symbol'])
#print df


inc = df.close >= df.open
dec = df.open > df.close
#eq = df.close == df.open
w = 24*60*60*1000 # half day in ms

TOOLS = "pan,wheel_zoom,box_zoom,reset,save"


p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=8000, title = "SHFE Candlestick")

p.xaxis.major_label_orientation = pi/4
p.grid.grid_line_alpha=0.3
#print df.index

p.segment(df.index[inc], df.high[inc], df.index[inc], df.low[inc], color="#00E22D")
p.segment(df.index[dec], df.high[dec], df.index[dec], df.low[dec], color="#FF3030")
p.vbar(df.index[inc], w, df.open[inc], df.close[inc], fill_color="#00E22D", line_color="#00E22D")
p.vbar(df.index[dec], w, df.open[dec], df.close[dec], fill_color="#FF3030", line_color="#FF3030")
#p.segment(df.index[eq], df.open[eq], df.index[eq].shift(1,freq='D'), df.open[eq], line_color='navy')
#p.segment(df.index[eq], df.high[eq], df.index[eq].shift(1,freq='D'), df.low[eq], color='navy')

output_file("candlestick.html", title="candlestick.py example")

show(p)

