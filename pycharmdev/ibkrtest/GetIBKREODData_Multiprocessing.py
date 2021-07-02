''' Demonstrates different ways to request financial data '''
import queue
from datetime import datetime
from threading import Thread
import time
import multiprocessing
import os, shutil
import logging

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import pandas as pd
import wtsdblib


# File level configutation/parameters
TEMP_FOLDER = '/home/wts/dev/temp/'
BACKUP_FOLDER = '/home/wts/dev/backup/'
TEMP_FILE = '/home/wts/dev/temp/IBKREODData.csv'
DATABASE = 'WTSDEV'
#SCHEMA = '' (Not yet implemented)
TIME_FRAME = '1 day'
TIME_PERIOD = '2 Y'

#   clean_csv_value:
#       Transforms a single value
#       Escape new lines: some of the text fields include newlines, so we escape \n -> \\n.
#       Empty values are transformed to \N: The string "\N" is the default string used by PostgreSQL to indicate NULL in COPY (this can be changed using the NULL option).

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
        self.process_index = client_id
        self.ibkr_current_symbol = ''
        try:
            # Launch the client thread
            thread = Thread(target=self.run)
            thread.start()
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
        # print(self.fileptr)
        self.fileptr.write(','.join(map(clean_csv_value, (\
                TIME_FRAME,
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

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print('HistoricalDataEnd. ProcessID: {} ReqID: {}, from: {}, to: {}, for: {}'\
              .format(self.process_index, reqId, start,end, self.ibkr_current_symbol))
        self.processing_flag = 0

    @iswrapper
    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''
        print('Process: {} Error {}: {} : {}'.format(self.process_index, code, self.ibkr_current_symbol, msg))
        if code not in [2104, 2106, 2108, 2158]:
            self.processing_flag = 0

# This function is called from all the processes.
def gethistoricaldata(process_index, symbol_queue):
    # Create the client and connect to TWS & Database
    client = MarketReader('127.0.0.1', 7497, process_index)

    # Wait till the connection retrieves connection with all the data forms.
    while client.processing_flag is None or client.processing_flag == 1:
        print(f"Process {process_index} Connecting...")
        time.sleep(0.2)
    print(f"Process {process_index} Connected...")

    time.sleep(0.25)
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
            client.reqHistoricalData(req_num, con, now, TIME_PERIOD, TIME_FRAME, 'TRADES', False, 1, False, [])
            # Sleep while the requests are processed
            while client.processing_flag == 1:
                time.sleep(0.2)
        except queue.Empty:
            print("Process: {} - Exception - Queue is empty".format(process_index))
            break

        # todo: If any other genuine error, need to put back the symbol back in queue. do it with caution !!!
    print("Process: {} - Queue processing is completed".format(process_index))
    client.fileptr.close()
    client.disconnect()
    if client.dbconn is not None:
        client.dbconn.close()

def main():

    for default_handler in logging.root.handlers:
        logging.root.removeHandler(default_handler)
    logging.basicConfig(level=logging.INFO)

    mainlogger = logging.Logger("IBKREOD_MAIN")
    mainlogger.setLevel(logging.WARNING)
    symbol_queue = multiprocessing.Queue()
    mainlogger.critical("Main process started")

    # Request historical bars for each of the stock in the table IBKR_SYMBOLS_EQUITY
    # For the symbol, loop the values in the table.
    dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
    dbcursor = dbconn.cursor()
    #dbquery = ''' SELECT ISE."IBKR_SYMBOL" FROM wtst."IBKR_SYMBOLS_EQUITY" ISE WHERE ISE."SERIES" = 'EQ' AND ISE."IBKR_SYMBOL" > 'S' LIMIT 25'''
    # Focus stocks
    dbquery = ''' SELECT fs.ibkr_symbol FROM wtst.focus_stocks fs ORDER BY fs.averagetradevalue DESC'''
    #Reliance
    #dbquery = ''' SELECT ISE."IBKR_SYMBOL" FROM wtst."IBKR_SYMBOLS_EQUITY" ISE WHERE ISE."IBKR_SYMBOL" = 'RELIANCE' '''

    dbcursor.execute(dbquery)
    dbrecordset = dbcursor.fetchall()
    for dbrow in dbrecordset:
        symbol_queue.put(dbrow[0])

    dbcursor.close()
    start_time = datetime.now()
    proc1 = multiprocessing.Process(target=gethistoricaldata, args=(0,symbol_queue))
    proc2 = multiprocessing.Process(target=gethistoricaldata, args=(1,symbol_queue))
    proc3 = multiprocessing.Process(target=gethistoricaldata, args=(2,symbol_queue))
    proc4 = multiprocessing.Process(target=gethistoricaldata, args=(3,symbol_queue))
    proc5 = multiprocessing.Process(target=gethistoricaldata, args=(4, symbol_queue))
    proc6 = multiprocessing.Process(target=gethistoricaldata, args=(5, symbol_queue))

    print(datetime.now())
    proc1.start()
    time.sleep(0.25)

    proc2.start()
    time.sleep(0.25)

    proc3.start()
    time.sleep(0.25)

    proc4.start()
    time.sleep(0.25)

    proc5.start()
    time.sleep(0.25)

    proc6.start()
    time.sleep(0.25)

    proc1.join()
    proc2.join()
    proc3.join()
    proc4.join()
    proc5.join()
    proc6.join()

    ibkr_download_endtime = datetime.now()

    for csv_file_name in os.listdir(TEMP_FOLDER):
        try:
            dbcursor = dbconn.cursor()
            csv_file_ptr = open(os.path.join(TEMP_FOLDER, csv_file_name), 'r')
            print(f"Uploading file: {csv_file_name}")
            dbcursor.copy_from(csv_file_ptr,'wtst.ibkr_hist_data', sep=',', null='\\N', size=8192, columns=None)
            csv_file_ptr.flush()
            csv_file_ptr.close()
            dbconn.commit()
            print(f"Successfully uploaded data from {csv_file_name}")
            #todo: Handle the issue of DB Error while inserting same data again.
            shutil.move(os.path.join(TEMP_FOLDER, csv_file_name),
                        os.path.join(BACKUP_FOLDER, csv_file_name+"_" + datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")))
            dbcursor.close()
            dbconn.commit()
        except psycopg2.Error as err:
            print(f"Db error: {csv_file_name} : {err}")
            # Continue execution with new DB connection.
            dbconn.close()
            dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
            continue
        except Exception as err:
            print(f"Non-database error - {csv_file_name}: {err}")
            dbcursor.close()

    dbconn.close()
    db_upload_endtime = datetime.now()
    print(f"IBKR download time: {ibkr_download_endtime-start_time}")
    print(f"DB upload time: {db_upload_endtime - ibkr_download_endtime}")
    print(f"Total time: {db_upload_endtime - start_time}")
    print('Processes completed')
if __name__ == '__main__':
    main()