import dash
import dash_core_components as dcc
import dash_html_components as html
# import dash_bootstrap
from dash.dependencies import Input, Output

import pandas as pd
import talib as ta

import datetime
import psycopg2

import wtsdblib

import plotly.graph_objects as go
from plotly.subplots import make_subplots

dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
dbcursor = dbconn.cursor()

# Get NSE EOD data
dbquery = ''' select count(distinct symbol) as symbol_count , count(*) as total_records, min(date) as earliest_date, max(date) as updated_till  
from wtst.nse_eod_data   '''
dbcursor.execute(dbquery)
dbrecordset = dbcursor.fetchall()
nse_summary_df = pd.DataFrame()
for dbrow in dbrecordset:
    nse_summary_df = pd.DataFrame({"Symbol Count": dbrow[0], "Total Records": dbrow[1], "Earliest date": dbrow[2], "Last updated": dbrow[3]}, index = [dbrow[0]])

# Get IBKR EOD data
dbquery = ''' select count(distinct ibkr_symbol) as symbol_count , count(*) as total_records, min(date) as earliest_date, max(date) as updated_till  
from wtst.ibkr_eod_data   '''
dbcursor.execute(dbquery)
dbrecordset = dbcursor.fetchall()
ibkr_eod_summary_df = pd.DataFrame()
for dbrow in dbrecordset:
    ibkr_eod_summary_df = pd.DataFrame({"Symbol Count": dbrow[0], "Total Records": dbrow[1], "Earliest date": dbrow[2].date(), "Last updated": dbrow[3].date()}, index = [dbrow[0]])



# Get IBKR EOD data
dbquery = ''' select timeframe, count(distinct(ibkr_symbol)) as symbol_count, count(*) as total_records,
min(date) as earliest_date, max(date) as last_updated from wtst.ibkr_intraday_data
group by timeframe '''
dbcursor.execute(dbquery)
dbrecordset = dbcursor.fetchall()
ibkr_intraday_summary_df = pd.DataFrame()
for dbrow in dbrecordset:
    ibkr_intraday_summary_df = ibkr_intraday_summary_df.append(pd.DataFrame({"Timeframe": dbrow[0],"Symbol Count": dbrow[1], "Total Records": dbrow[2], "Earliest date": dbrow[3].date(), "Last updated": dbrow[4].date()}, index = [dbrow[0]]))


def generate_table(dataframe, max_rows=10):
    return html.Table([
        html.Thead(
            html.Tr([html.Th(col) for col in dataframe.columns])
        ),
        html.Tbody([
            html.Tr([
                html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
            ]) for i in range(min(len(dataframe), max_rows))
        ])
    ])

app = dash.Dash()
server = app.server
app.title = 'Historic data monitor'

app.layout = html.Div(style={'backgroundColor': '#CCCCCC', 'color': '#444444'}, children=[
    html.H1(style={'textAlign': 'center', 'backgroundColor': '#444444', 'color': '#FFAAFF'}, children='History DATA'),
    html.H2(style={'textAlign': 'center'}, children='NSE EOD DATA'),
    html.Div(style={'textAlign': 'center'}, children=generate_table(nse_summary_df, 1)),
    html.Hr(),

    html.H2(style = {'textAlign': 'center'}, children='IBKR EOD DATA'),
    html.Div(style={'textAlign': 'center'}, children=generate_table(ibkr_eod_summary_df, 1)),
    html.Hr(),

    html.H2(style={'textAlign': 'center'}, children='IBKR Intraday DATA'),
    html.Div(style={'textAlign': 'center'}, children=generate_table(ibkr_intraday_summary_df, 25)),
    html.Hr()
                                ])

if __name__ == '__main__':
    app.run_server(debug=True)