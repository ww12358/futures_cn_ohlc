# coding: utf-8
from math import pi
import pandas as pd
from bokeh.plotting import figure, show, output_file
from cze.include import TEST_DATA_PATH

symbol = 'SR'
month = '09'
#year = '17'

df = pd.read_hdf(TEST_DATA_PATH, '/'+symbol+'/D/'+'_'+month)

inc = df.close >= df.open
dec = df.open > df.close
w = 24*60*60*1000 # half day in ms

TOOLS = "pan,wheel_zoom,box_zoom,reset,save"

p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=2000, title = "CZE Candlestick")
p.xaxis.major_label_orientation = pi/4
p.grid.grid_line_alpha=0.3

p.segment(df.index[inc], df.high[inc], df.index[inc], df.low[inc], color="#00E22D")
p.segment(df.index[dec], df.high[dec], df.index[dec], df.low[dec], color="#FF3030")
p.vbar(df.index[inc], w, df.open[inc], df.close[inc], fill_color="#00E22D", line_color="#00E22D")
p.vbar(df.index[dec], w, df.open[dec], df.close[dec], fill_color="#FF3030", line_color="#FF3030")

output_file("candlestick.html", title="candlestick.py example")

show(p) 

