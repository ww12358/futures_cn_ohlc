# coding: utf-8
from __future__ import absolute_import
from math import pi
import pandas as pd
from bokeh.models import HoverTool
#from bokeh.plotting import figure, show, output_file
from bokeh.models import ColumnDataSource\
#, Plot, DataRange1d, LinearAxis
#from bokeh.models.glyphs import VBar, Segment
#from bokeh.io import curdoc, show
from bokeh.plotting import figure, show, output_file
from shfe.shfe_src_code.include import DATA_PATH, TEST_DATA_PATH

symbol = 'CU'
month = u'07'

df = pd.read_hdf(DATA_PATH, '/'+symbol+'/D/'+'_'+month)
df = df.reset_index(level=['symbol'])
#print df


inc = df.close > df.open
dec = df.open > df.close
w = 24*60*60*1000 # half day in ms




TOOLS = "pan,wheel_zoom,box_zoom,reset,save"
#xdr = DataRange1d()
#ydr = DataRange1d()

#p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=8000, title = "SHFE Candlestick")
#p = Plot(
#    title=None, x_range=xdr, plot_width=8000, plot_height=400,
#    h_symmetry=False, v_symmetry=False, min_border=0
#)

p = figure(x_axis_type="datetime", tools=TOOLS, plot_width=6000, title = "SHFE Candlestick")
#xaxis = LinearAxis()
#p.add_layout(xaxis, 'below')
#yaxis = LinearAxis()
#p.add_layout(yaxis, 'left')
#p.xaxis.major_label_orientation = pi/4
#p.grid.grid_line_alpha=0.3
#source = ColumnDataSource(df)
#print df.index


p.segment(df.index[inc], df.high[inc], df.index[inc], df.low[inc], color="#00E22D")
p.segment(df.index[dec], df.high[dec], df.index[dec], df.low[dec], color="#FF3030")
p.vbar(df.index[inc], w, df.open[inc], df.close[inc], fill_color="#00E22D", line_color="#00E22D")
p.vbar(df.index[dec], w, df.open[dec], df.close[dec], fill_color="#FF3030", line_color="#FF3030")


#p.tools.append(h)
#seg_call = Segment(x0=df.index[inc], y0=df.high[inc], x1=df.index[inc], y1=df.low[inc], line_color="#00E22D")
#seg_put = Segment(x0=df.index[dec], y0=df.high[dec], x=df.index[dec], y1=df.low[dec], line_color="#FF3030")
#glyph_call = VBar(x=df.index[inc], top=df.open[inc], bottom=df.close[inc], fill_color="#00E22D", line_color="#00E22D")
#glyph_put = VBar(x=df.index[dec], top=df.open[dec], bottom=df.close[dec], fill_color="#FF3030", line_color="#FF3030")
#p.add_glyph(source, glyph_put)
#p.add_glyph(source, glyph_call)
#p.add_glyph(source, seg_call)
#p.add_glyph(source, seg_put)

#curdoc().add_root(p)
#output_file("candlestick.html", title="candlestick.py example")

show(p)

