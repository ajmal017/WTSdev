import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import datetime, time
import os
import psycopg2

import wtsdblib
import plotly.graph_objects as go
from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

# File level configutation/parameters
TEMP_FOLDER = '/home/wts/dev/temp/'
BACKUP_FOLDER = '/home/wts/dev/backup/'
TEMP_FILE = '/home/wts/dev/temp/IBKRLiveFiveData.csv'
DATABASE = 'WTSDEV'
LIMIT_WATCHLIST = 32
LIMIT_CLIENT_COUNT = 32

# Load CSV file for plotting.

app = dash.Dash()
server = app.server
app.title = 'Reliance Live Charting'

ohlc_live_df = pd.DataFrame([{"plot_date":[], "open":[],"high":[],"low":[],"close":[]}])
fig1 = go.Figure(data=go.Candlestick(x=ohlc_live_df['plot_date'], open=ohlc_live_df['open'], high=ohlc_live_df['high'], low=ohlc_live_df['low'],
                                     close=ohlc_live_df['close']), layout=go.Layout(height=800, width=1400))

fig1.update_layout()

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
    dcc.Graph(figure=fig1)
])

def clean_csv_value(value) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

class MarketReader(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    # Class variables - Start
    dff = pd.DataFrame()

    # Class variables - End
    def iswrapper(fn):
        return fn

    def __init__(self, addr, port, client_id):
        EClient. __init__(self, self)

        # Connect to TWS, Database and Open temp file.
        self.processing_flag = 1
        self.connect(addr, port, client_id)
        self.dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
        self.fileptr = open(TEMP_FILE, "w")
        self.client_index = client_id
        self.ibkr_current_symbol = ''
        try:
            # Launch the client thread
            thread = Thread(target=self.run)
            thread.start()
            print("\nClient: {} - Thread successfully started for Process".format(self.client_index))
        except:
            print("\nClient: {} - Error in starting the thread for Process".format(self.client_index))

    @iswrapper
    def nextValidID(self, orderId):
        print(f"Client: {self.client_index} - TWS Connection established ")
        self.processing_flag = 0

    @iswrapper
    def realtimeBar(self, reqId, time: int, open_: float, high: float, low: float, close: float,
                    volume: int, wap: float, count: int):
        # print("RealTimeBar. TickerId:", reqId, RealTimeBar(time, -1, open_, high, low, close, volume, wap, count))
        global ohlc_live_df
        ohlc_live_df = ohlc_live_df.append(pd.DataFrame.from_records([{'plot_date': datetime.strftime(datetime.fromtimestamp(time), "%Y%m%d  %H:%M:%S"),'symbol': 'RELIANCE','open': open_, 'high': high,'low': low, 'close': close}]))

    @iswrapper
    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''
        print('Client: {} Error {}: {} : {}'.format(self.client_index, code, self.ibkr_current_symbol, msg))
        self.processing_flag = 0

def main():
    # Create the client and connect to TWS & Database
    client = MarketReader('127.0.0.1', 7497, 0)

    # Wait till the API receives connection status with all the data forms.
    #while client.processing_flag is None or client.processing_flag == 1:
        #print(f"Client Connecting...")
        #time.sleep(0.2)

    print(f"Client Connected...")
    time.sleep(0.25)

    # Closing the dummy file opened as part of object initiation. Actual file will be opened by the process.
    if client.fileptr is not None:
        client.fileptr.close()
    outfile_name = os.path.join(TEMP_FOLDER, 'Dash-LiveStream.csv')
    client.fileptr = open(outfile_name, "w")

    # Set the IBKR contract details
    con = Contract()
    con.secType = 'STK'
    con.exchange = 'NSE'
    con.currency = 'INR'
    req_num = 0


    client.ibkr_current_symbol = 'RELIANCE'
    con.symbol = client.ibkr_current_symbol

    client.processing_flag = 1
    client.reqRealTimeBars(req_num, con, 5, "TRADES", True, [])

    #time.sleep(120)

    app.run_server(debug=True)
    client.cancelRealTimeBars(req_num)

    print("Client: Processing is completed")
    client.fileptr.close()
    client.dbconn.close()
    client.disconnect()
    if client.dbconn is not None:
        client.dbconn.close()


if __name__ == '__main__':
    main()


