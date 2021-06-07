''' Demonstrates different ways to request financial data '''
from datetime import datetime
from threading import Thread
import time

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mplf


class MarketReader(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    dff = pd.DataFrame()
    def iswrapper(fn):
        return fn

    def __init__(self, addr, port, client_id):
        EClient. __init__(self, self)

        # Connect to TWS
        self.connect(addr, port, client_id)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()

    @iswrapper
    def fundamentalData(self, reqId, data):
        ''' Called in response to reqFundamentalData '''

        print('Fundamental data: ' + data)
    @iswrapper
    def historicalData(self, reqId, bar):
        ''' Called in response to reqHistoricalData '''
        #print('historicalData - Close price: {}'.format(bar.close))
        # df3 = pd.DataFrame()

        df1 = pd.DataFrame({"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close, "Volume": bar.volume, "Average": bar.average, "BarCount": bar.barCount}, index=[bar.date])

        self.dff = pd.concat([self.dff, df1])
        # print(df1)



    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''

        print('Error {}: {}'.format(code, msg))

def main():

    # Create the client and connect to TWS
    client = MarketReader('127.0.0.1', 7497, 0)

    # Request the current time
    con = Contract()
    con.symbol = 'RELIANCE'
    con.secType = 'STK'
    con.exchange = 'NSE'
    con.currency = 'INR'

    # Request ten ticks containing midpoint data
    #client.reqTickByTickData(0, con, 'MidPoint', 10, True)

    # Request market data
    #client.reqMktData(1, con, '', False, False, [])

    # Request current bars
    #client.reqRealTimeBars(2, con, 5, 'TRADES', True, [])

    # Request historical bars
    now = datetime.now().strftime("%Y%m%d, %H:%M:%S")
    #client.reqHistoricalData(3, con, '20210525 00:00:00', '3d d', '1 min', 'TRADES', False, 1, False, [])

    # Request fundamental data
    client.reqFundamentalData(4, con, 'ReportsFinStatements', [])

    # Sleep while the requests are processed
    time.sleep(5)

    # Disconnect from TWS
    client.disconnect()


    #client.dff['Close'].plot()
    #plt.show()

    #mplf.plot(client.dff)

    #print(client.dff)

if __name__ == '__main__':
    main()