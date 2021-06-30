import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import talib as ta


import datetime
import psycopg2

import wtsdblib

import plotly.graph_objects as go
from plotly.subplots import make_subplots

LIMIT = 5000

# Load CSV file for plotting.
df = pd.read_csv('Reliance_20210622.csv')
df.sort_values('Date/Time',inplace=True)
df['ma5'] = ta.SMA(df['Close'],5)
df['ma20'] = ta.SMA(df['Close'],20)
df['adx'] = ta.ADX(df['High'], df['Low'], df['Close'], timeperiod=14)

#fig = go.Figure(data=go.Candlestick(x=df['Date/Time'][:LIMIT],
#                    open=df['Open'][:LIMIT],
#                    high=df['High'][:LIMIT],
#                    low=df['Low'][:LIMIT],
#                    close=df['Close'][:LIMIT]), layout=go.Layout(height=800, width=1400, hovermode='x'))

fig = make_subplots(
    rows=2, cols=1,
    row_heights=[0.7, 0.3],
    shared_xaxes=True, vertical_spacing=0.02,
    specs=[[{'type': 'scatter'}],
           [{'type': 'scatter'}]])

fig.update_layout(height=800, width=1400, hovermode='x unified')
fig.add_trace(go.Scatter(x=df['Date/Time'][:LIMIT], y=df['Close'][:LIMIT], name='Reliance Intraday'), row=1, col=1)
fig.add_trace(go.Scatter(x=df['Date/Time'][:LIMIT], y=df['ma5'], name='MA5'), row=1, col=1)
fig.add_trace(go.Scatter(x=df['Date/Time'][:LIMIT], y=df['ma20'], name='MA20'), row=1, col=1)
fig.add_trace(go.Scatter(x=df['Date/Time'][:LIMIT], y=df['adx'], name='ADX'), row=2, col=1)

fig.add_hline(y=25, row=2, col=1)

fig.update_xaxes(showspikes=True)
fig.update_yaxes(showspikes=True)

dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
dbcursor = dbconn.cursor()
dbquery = ''' select "DATE","SYMBOL", "OPEN","HIGH","LOW","CLOSE" from wtst."NSE_EOD_DATA" WHERE "SYMBOL" = %s ORDER BY "DATE" DESC'''
dbparams = ('RELIANCE',)
dbcursor.execute(dbquery, dbparams)

ohlc_recordset = dbcursor.fetchall()
ohlc_db_df = pd.DataFrame()

for ohlc_row in ohlc_recordset:
    ohlc_db_df = ohlc_db_df.append(pd.DataFrame.from_records([{'plot_date': ohlc_row[0], 'symbol': ohlc_row[1], 'open':ohlc_row[2], 'high':ohlc_row[3], 'low':ohlc_row[4], 'close': ohlc_row[5]},]))

ohlc_db_df.sort_values('plot_date', inplace=True)
ohlc_db_df['ma5'] = ta.SMA(ohlc_db_df['close'],5)
ohlc_db_df['ma20'] = ta.SMA(ohlc_db_df['close'],20)
ohlc_db_df['adx'] = ta.ADX(ohlc_db_df['high'], ohlc_db_df['low'], ohlc_db_df['close'], timeperiod=14)

dbcursor.close()
dbconn.close()

fig1 = make_subplots(
    rows=2, cols=1,
    row_heights=[0.7, 0.3],
    shared_xaxes=True, vertical_spacing=0.02,
    specs=[[{'type': 'scatter'}],
           [{'type': 'scatter'}]])

fig1.update_layout(height=800, width=1400, hovermode='x unified')
fig1.add_trace(go.Scatter(x=ohlc_db_df['plot_date'], y=ohlc_db_df['close'],name='Reliance EOD'), row=1, col=1)
fig1.add_trace(go.Scatter(x=ohlc_db_df['plot_date'],y=ohlc_db_df['ma5'], name='MA5'), row=1, col=1)
fig1.add_trace(go.Scatter(x=ohlc_db_df['plot_date'],y=ohlc_db_df['ma20'], name='MA20'), row=1, col=1)
fig1.add_trace(go.Scatter(x=ohlc_db_df['plot_date'], y=ohlc_db_df['adx'], name='ADX'), row=2, col=1)
fig1.add_hline(y=25, row=2, col=1)

fig1.update_xaxes(showspikes=True)
fig1.update_yaxes(showspikes=True)

app = dash.Dash()
server = app.server
app.title = 'My first web page'
colors = {
    'background': '#AAAAAA',
    'text': '#0FDBFF'
}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Hello Dash',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),
    html.P('This is a paragraph text used to explain things'),

    html.Div(children='Dash: A web application framework for Python.', style={
        'textAlign': 'center',
        'color': colors['text']
    }),
    dcc.Graph(figure=fig),
    dcc.Graph(figure=fig1)
])
if __name__ == '__main__':
    app.run_server(debug=True)

