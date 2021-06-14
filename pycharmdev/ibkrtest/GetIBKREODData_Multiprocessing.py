''' Demonstrates different ways to request financial data '''
import queue
from datetime import datetime
from threading import Thread
import time
import multiprocessing
import os
import random
from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import pandas as pd
import wtsdblib
# Imported the below for fast db upload using IO operators.
from typing import Iterator, Optional, Dict, Any
import io

# File level configutation/parameters
TEMP_FOLDER = '/home/wts/wtsdevgit/pycharmdev/ibkrtest/temp/'
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
        self.process_index = client_id
        try:
            # Launch the client thread
            thread = Thread(target=self.run)
            thread.start()
            print("\nProcess: {} - Thread successfully started for Process".format(self.process_index))
        except:
            print("\nProcess: {} - Error in starting the thread for Process".format(self.process_index))


    @iswrapper
    def historicalData(self, reqId, bar):
        ''' Called in response to reqHistoricalData '''
        # print(self.fileptr)
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
        print('HistoricalDataEnd. ProcessID: {} ReqID: {}, from: {}, to: {}, for: {}'\
              .format(self.process_index, reqId, start,end, self.ibkr_current_symbol))
        self.processing_flag = 0

    @iswrapper
    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''
        print('Process: {} Error {}: {} : {}'.format(self.process_index, code, self.ibkr_current_symbol, msg))
        self.processing_flag = 0

# This function is called from all the processes.
def gethistoricaldata(process_index, symbol_queue):
    # Create the client and connect to TWS & Database
    client = MarketReader('127.0.0.1', 7497, process_index)
    # Closing the dummy file opened as part of object initiation. Actual file will be opened by the process.
    if client.fileptr is not None:
        client.fileptr.close()
    outfile_name = os.path.join(TEMP_FOLDER, 'IBKREODData_Process{}.csv'.format(process_index))
    client.fileptr = open(outfile_name, "w")
    # Set the IBKR contract details
    con = Contract()
    con.secType = 'STK'
    con.exchange = 'NSE'
    con.currency = 'INR'
    req_num = 0
    while not symbol_queue.empty():
        try:
            client.ibkr_current_symbol = symbol_queue.get()
            con.symbol = client.ibkr_current_symbol
            req_num += 1
            client.processing_flag = 1
            now = datetime.now().strftime("%Y%m%d, %H:%M:%S")
            client.reqHistoricalData(req_num, con, now, '1 m', '1 day', 'TRADES', False, 1, False, [])
            # Sleep while the requests are processed
            while client.processing_flag == 1:
                time.sleep(0.2)
        except queue.Empty:
            print("Process: {} - Exception - Queue is empty".format(process_index))
            break
        # todo: If any other genuine error, need to put back the symbol back in queue.
    print("Process: {} - Queue processing is completed".format(process_index))
    client.fileptr.close()
    client.disconnect()
    if client.dbconn is not None:
        client.dbconn.close()

def main():

    symbol_queue = multiprocessing.Queue()

    # Request historical bars for each of the stock in the table IBKR_SYMBOLS_EQUITY
    # For the symbol, loop the values in the table.
    dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
    dbcursor = dbconn.cursor()
    dbquery = ''' SELECT ISE."IBKR_SYMBOL" FROM wtst."IBKR_SYMBOLS_EQUITY" ISE WHERE ISE."IBKR_SYMBOL" > 'M' LIMIT 100'''
    #dbquery = ''' SELECT ISE."IBKR_SYMBOL" FROM wtst."IBKR_SYMBOLS_EQUITY" ISE WHERE ISE."IBKR_SYMBOL" = 'RELIANCE' '''

    dbcursor.execute(dbquery)
    dbrecordset = dbcursor.fetchall()
    for dbrow in dbrecordset:
        symbol_queue.put(dbrow[0])

    dbcursor.close()
    starttime = datetime.now()
    proc1 = multiprocessing.Process(target=gethistoricaldata, args=(0,symbol_queue))
    proc2 = multiprocessing.Process(target=gethistoricaldata, args=(1,symbol_queue))
    proc3 = multiprocessing.Process(target=gethistoricaldata, args=(2,symbol_queue))
    proc4 = multiprocessing.Process(target=gethistoricaldata, args=(3,symbol_queue))
    print(datetime.now())
    proc1.start()
    time.sleep(0.25)

    #proc1.join()

    proc2.start()
    time.sleep(0.25)

    proc3.start()
    time.sleep(0.25)

    proc4.start()
    time.sleep(0.25)

    proc1.join()
    proc2.join()
    proc3.join()
    proc4.join()
    endtime = datetime.now()
    print(endtime - starttime)
    print('Processes completed')
    dbconn.close()

if __name__ == '__main__':
    main()