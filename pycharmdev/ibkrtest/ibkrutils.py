import pandas as pd
from datetime import datetime
from threading import Thread
import time
import logging
import multiprocessing

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper
from typing import Iterator, Optional, Dict, Any

TEMP_FILE = '/home/wts/dev/temp/IBKREODData.csv'
DATABASE = 'WTSDEV'
TIME_FRAME = '1 day'

def clean_csv_value(value: Optional[Any]) -> str:
    if value is None:
        return r'\N'
    return str(value).replace('\n', '\\n')

class ibkr_reader(EWrapper, EClient):
    ''' Serves as the client and the wrapper '''
    def iswrapper(fn):
        return fn

    def __init__(self, addr, port, client_id):
        EClient. __init__(self, self)
        # Connect to TWS, Database and Open temp file.
        self.processing_flag = 1
        self.connect(addr, port, client_id)
        #self.dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
        self.fileptr = open(TEMP_FILE, "w")
        self.process_index = client_id
        self.ibkr_current_symbol = ''
        self.ibkr_current_symbol_start_date = ''
        self.request_log = pd.DataFrame()

        try:
            # Launch the client thread
            self.thread = Thread(target=self.run)
            self.thread.start()
            print("\nProcess: {} - Thread successfully started for Process".format(self.process_index))
        except:
            print("\nProcess: {} - Error in starting the thread for Process".format(self.process_index))

    @iswrapper
    def nextValidId(self, orderId):
        print(f"Process: {self.process_index} - TWS Connection established ")
        self.processing_flag = 0

    @iswrapper
    def historicalData(self, reqId, bar):
        ''' Called in response to reqHistoricalData '''
        # print(self.fileptr)\

        if TIME_FRAME == '1 day':
            if datetime.strptime(bar.date, "%Y%m%d").date() < self.ibkr_current_symbol_start_date:
                return
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
            print(','.join(map(clean_csv_value, (\
                    self.ibkr_current_symbol,\
                    bar.date,\
                    bar.open,\
                    bar.high,\
                    bar.low,\
                    bar.close,\
                    bar.volume,\
                    bar.average,\
                    bar.barCount\
                        ))) + '\n')
        else:
            self.fileptr.write(','.join(map(clean_csv_value, ( \
                TIME_FRAME,\
                self.ibkr_current_symbol, \
                bar.date, \
                bar.open, \
                bar.high, \
                bar.low, \
                bar.close, \
                bar.volume, \
                bar.average, \
                bar.barCount \
                ))) + '\n' \
                               )
            print(','.join(map(clean_csv_value, ( \
                TIME_FRAME,\
                self.ibkr_current_symbol, \
                bar.date, \
                bar.open, \
                bar.high, \
                bar.low, \
                bar.close, \
                bar.volume, \
                bar.average, \
                bar.barCount \
                ))) + '\n' )


    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print('HistoricalDataEnd. ProcessID: {} ReqID: {}, from: {}, to: {}, for: {}'\
              .format(self.process_index, reqId, start, end, self.ibkr_current_symbol))
        self.processing_flag = 0

    @iswrapper
    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''
        if code in [2103,2104, 2105, 2106, 2108, 2158]:
            print('Process: {} Warning {}: {} : {}'.format(self.process_index, code, self.ibkr_current_symbol, msg))
        else:
            print('Process: {} Error {}: {} : {}'.format(self.process_index, code, self.ibkr_current_symbol, msg))
            self.processing_flag = 0

    @iswrapper
    def sleep_till_processing(self):
        while self.processing_flag == 1:
            time.sleep(0.2)

def test_ibkr_reader():
    logging.basicConfig(level=logging.INFO)
    process_index = 0
    client = ibkr_reader('127.0.0.1', 7497, process_index)
    # After instantiating the reader, wait for 250ms for initialising the IBKR data channels
    time.sleep(0.25)


    start_time = datetime.now().strftime("%Y%m%d, %H:%M:%S")
    curr_time = start_time
    print(start_time)
    # Set the IBKR contract details
    reqnum = 0
    con = Contract()
    con.secType = 'STK'
    con.exchange = 'NSE'
    con.currency = 'INR'
    con.symbol = 'TCS'
    client.ibkr_current_symbol = con.symbol
    client.processing_flag = 1
    client.ibkr_current_symbol_start_date = datetime(year=2021,month=7,day=12).date()
    client.reqHistoricalData(reqnum, con, curr_time, '3 W', '1 day', 'TRADES', False, 1, False, [])
    # Sleep while the requests are processed
    client.sleep_till_processing()
    print(f'After sleep: {datetime.now()}')
    client.fileptr.close()
    client.disconnect()

if __name__ == '__main__':
    test_ibkr_reader()