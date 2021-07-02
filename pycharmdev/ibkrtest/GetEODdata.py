''' Demonstrates different ways to request financial data '''
from datetime import datetime
from threading import Thread
import time

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import pandas as pd
import wtsdblib


class MarketReader(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    # Class variables - Start
    dff = pd.DataFrame()
    # Dummy entry to declare the variable
    dbconn = wtsdblib.wtsdbconn.newconnection('')
    ibkr_current_symbol = ''

    # Class variables - End
    def iswrapper(fn):
        return fn


    def __init__(self, addr, port, client_id, dbsystem):
        EClient. __init__(self, self)

        # Connect to TWS
        self.connect(addr, port, client_id)
        self.dbconn = wtsdblib.wtsdbconn.newconnection(dbsystem)

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()


    @iswrapper
    def historicalData(self, reqId, bar):
        ''' Called in response to reqHistoricalData '''

        #df1 = pd.DataFrame({"Date": bar.date, "Open": bar.open, "High": bar.high, "Low": bar.low, "Close": bar.close, "Volume": bar.volume, "Average": bar.average, "BarCount": bar.barCount}, index=[bar.date])
        #self.dff = pd.concat([self.dff, df1])
        try:
            dbcursor = self.dbconn.cursor()
            dbquery = '''INSERT INTO wtst."ibkr_eod_data"("ibkr_symbol","date", "open", "high", "low", "close", "volume", "average", "trade_count") VALUES (%s, date(%s), %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING '''
            dbparams = (self.ibkr_current_symbol, bar.date, bar.open, bar.high, bar.low, bar.close, bar.volume, bar.average, bar.barCount)
            #dbcursor.execute(dbquery, dbparams)
            print(bar)
            self.dbconn.commit()
        except (Exception, psycopg2.Error) as err:
            print("Failed to insert record into table", err)
        finally:
            dbcursor.close()

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        # super().historicalDataEnd(reqId, start, end)
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end)

    @iswrapper
    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''
        print('Error {}: {} : {}'.format(code, self.ibkr_current_symbol, msg))

def main():
    # Create the client and connect to TWS & Database
    client = MarketReader('127.0.0.1', 7497, 0, dbsystem='WTSDEV')

    until_datetime = datetime.now().strftime("%Y%m%d, %H:%M:%S")

    # Set the IBKR contract details
    con = Contract()
    con.secType = 'STK'
    con.exchange = 'NSE'
    con.currency = 'INR'

    # For the symbol, loop the values in the table.

    dbcursor = client.dbconn.cursor()
    dbquery = ''' SELECT ISE."ibkr_symbol" FROM wtst.ibkr_symbols ISE '''
    dbquery = ''' SELECT ISE."ibkr_symbol" FROM wtst."ibkr_symbols" ISE WHERE ISE."ibkr_symbol" = 'RELIANCE' '''

    dbcursor.execute(dbquery)
    dbrecordset = dbcursor.fetchall()
    reqnum = 0
    for dbrow in dbrecordset:
        client.ibkr_current_symbol = dbrow[0]
        con.symbol = dbrow[0]
        reqnum += 1
        until_datetime = '20210629, 15:30:00'
        client.reqHistoricalData(reqnum, con, until_datetime, '300 S', '5 secs', 'TRADES', False, 1, False, [])
        # Sleep while the requests are processed
        time.sleep(5)

    # Finally, give additional 5 seconds for the code to run.
    time.sleep(5)

    # Disconnect from TWS & DB
    dbcursor.close()
    client.disconnect()
    if client.dbconn is not None:
        client.dbconn.close()

    #print(client.dff)

if __name__ == '__main__':
    main()