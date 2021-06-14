''' Demonstrates different ways to request financial data '''
from datetime import datetime
from threading import Thread
import time

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import pandas as pd
import wtsdblib
# Imported the below for fast db upload using IO operators.
from typing import Iterator, Optional, Dict, Any
import io

# File level configutation/parameters
TEMP_FILE = '/home/wts/wtsdevgit/pycharmdev/ibkrtest/temp/IBKREODData.csv'
DATABASE = 'WTSDEV'

#   clean_csv_value:
#       Transforms a single value
#       Escape new lines: some of the text fields include newlines, so we escape \n -> \\n.
#       Empty values are transformed to \N: The string "\N" is the default string used by PostgreSQL to indicate NULL in COPY (this can be changed using the NULL option).
def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

class MarketReader(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''

    # Class variables - Start
    dff = pd.DataFrame()
    ibkr_current_symbol = ''

    data_count = 0
    processing_flag = 0

    # Class variables - End
    def iswrapper(fn):
        return fn

    def __init__(self, addr, port, client_id):
        EClient. __init__(self, self)

        # Connect to TWS, Database and Open temp file.
        self.connect(addr, port, client_id)
        self.dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
        self.fileptr = open(TEMP_FILE, "w")

        # Launch the client thread
        thread = Thread(target=self.run)
        thread.start()


    @iswrapper
    def historicalData(self, reqId, bar):
        ''' Called in response to reqHistoricalData '''

        self.fileptr.write(','.join(map(clean_csv_value, (\
                self.ibkr_current_symbol,\
                bar.date,\
                bar.open,\
                bar.high,\
                bar.low,\
                bar.close,\
                bar.volume,\
                bar.average,\
                bar.barCount\
                    ))) + '\n'\
                           )
        self.data_count += 1

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print("HistoricalDataEnd. ReqId:", reqId, "from", start, "to", end, "for", self.ibkr_current_symbol)
        self.processing_flag = 0

    @iswrapper
    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''
        print('Error {}: {} : {}'.format(code, self.ibkr_current_symbol, msg))
        self.processing_flag = 0

def main():
    # Create the client and connect to TWS & Database
    client = MarketReader('127.0.0.1', 7497, 0)

    def sleep_till_processing():
        while client.processing_flag == 1:
            time.sleep(0.1)

    # Request historical bars for each of the stock in the table IBKR_SYMBOLS_EQUITY


    now = datetime.now().strftime("%Y%m%d, %H:%M:%S")

    # Set the IBKR contract details
    con = Contract()
    con.secType = 'STK'
    con.exchange = 'NSE'
    con.currency = 'INR'

    # For the symbol, loop the values in the table.

    dbcursor = client.dbconn.cursor()
    dbquery = ''' SELECT ISE."IBKR_SYMBOL" FROM wtst."IBKR_SYMBOLS_EQUITY" ISE WHERE ISE."IBKR_SYMBOL" LIKE 'AP%' LIMIT 100'''
    #dbquery = ''' SELECT ISE."IBKR_SYMBOL" FROM wtst."IBKR_SYMBOLS_EQUITY" ISE WHERE ISE."IBKR_SYMBOL" = 'RELIANCE' '''

    dbcursor.execute(dbquery)
    dbrecordset = dbcursor.fetchall()
    reqnum = 0
    time.sleep(1)
    for dbrow in dbrecordset:
        client.ibkr_current_symbol = dbrow[0]
        con.symbol = dbrow[0]
        reqnum += 1
        print(con.symbol)
        print(datetime.now())
        client.processing_flag = 1
        client.reqHistoricalData(reqnum, con, now, '1 y', '1 day', 'TRADES', False, 1, False, [])
        # Sleep while the requests are processed
        sleep_till_processing()
        print("After sleep: {}".format(datetime.now()))
        print(client.data_count)
    # Finally, give additional time for the code to run.
    sleep_till_processing()
    print("Final data count: {}".format(client.data_count))

    # Disconnect from TWS & DB
    dbcursor.close()
    client.fileptr.close()
    client.disconnect()
    if client.dbconn is not None:
        client.dbconn.close()

    #print(client.dff)

if __name__ == '__main__':
    main()