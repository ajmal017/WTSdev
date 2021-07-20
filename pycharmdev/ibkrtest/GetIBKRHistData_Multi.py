''' Demonstrates different ways to request financial data '''
import queue
from datetime import datetime, timedelta
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
SCHEMA = '' # (Not yet implemented)
TIME_FRAME = '1 day'
STR_START_DATE = '20210737'
STR_END_DATE = 'NOW'
LIMIT_CLIENT_COUNT = 32

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
        self.ibkr_current_symbol_start_date = ''
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

    @iswrapper
    def historicalDataEnd(self, reqId: int, start: str, end: str):
        print('HistoricalDataEnd. ProcessID: {} ReqID: {}, from: {}, to: {}, for: {}'\
              .format(self.process_index, reqId, start, end, self.ibkr_current_symbol))
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
            symbol_data = symbol_queue.get()
            client.ibkr_current_symbol = symbol_data['symbol']
            con.symbol = client.ibkr_current_symbol
            req_num += 1
            client.processing_flag = 1
            if TIME_FRAME == '1 day':
                delta = datetime.now().date() - symbol_data['max_date'].date()
                TIME_PERIOD = f'{delta.days} d'
                now = datetime.now().strftime("%Y%m%d, %H:%M:%S")
                client.ibkr_current_symbol_start_date = symbol_data['max_date'].date() + timedelta(days=1)
            else:
                end_date = datetime.strptime(STR_END_DATE, "YYYYMMDD")
                TIME_PERIOD = '1 d'

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

def get_ibkr_hist_data():
    # Set File level configutation variables based on parameters (if passed)
    global DATABASE, SCHEMA, TIME_FRAME, STR_START_DATE, STR_END_DATE, LIMIT_CLIENT_COUNT

    #DATABASE = 'WTSDEV'
    # SCHEMA = '' (Not yet implemented)
    #TIME_FRAME = '1 day'
    #STR_START_DATE = '20210730'
    #STR_END_DATE = 'NOW'
    #get_ibkr_hist_data_main()


    for default_handler in logging.root.handlers:
        logging.root.removeHandler(default_handler)
    logging.basicConfig(level=logging.INFO)

    mainlogger = logging.Logger("IBKREOD_MAIN")
    mainlogger.setLevel(logging.DEBUG)
    symbol_queue = multiprocessing.Queue()
    mainlogger.critical("Main process started")

    dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
    start_time = datetime.now()

    skip_download = False
    if (skip_download):
        mainlogger = logging.Logger("Download skipped")
    else:
        # Request historical bars for each of the stock in the table IBKR_SYMBOLS_EQUITY
        # For the symbol, loop the values in the table.
        dbcursor = dbconn.cursor()
        if (TIME_FRAME == '1 day'):
            dbquery = ''' SELECT ibkr_symbol, coalesce((select MAX(date) from wtst.ibkr_eod_data ied where isym.ibkr_symbol = ied.ibkr_symbol) ,DATE('TODAY')-364) AS max_date
            FROM wtst.ibkr_symbols isym  WHERE isym.series = 'EQ'  '''

            #dbquery = ''' SELECT ibkr_symbol, coalesce((select MAX(date) from wtst.ibkr_eod_data ied where isym.ibkr_symbol = ied.ibkr_symbol) ,DATE('05 -JUL-2020')) AS max_date
            #FROM wtst.ibkr_symbols isym  WHERE isym.series = 'EQ' AND isym.ibkr_symbol in ('RELIANCE','20MICRONS')  '''

        else:
            #dbquery = ''' SELECT fs.ibkr_symbol FROM wtst.focus_stocks fs ORDER BY fs.averagetradevalue DESC'''
            dbquery = ''' SELECT fs.ibkr_symbol,date('29-Jun-2021 0:0:0') FROM wtst.focus_stocks fs WHERE fs.ibkr_symbol = 'RELIANCE' '''

        dbcursor.execute(dbquery)
        dbrecordset = dbcursor.fetchall()
        dbcursor.close()

        for dbrow in dbrecordset:
            symbol_queue.put({"symbol": dbrow[0], "max_date": dbrow[1]})

        # Number of clients reduced if the number of symbols is lesser than the configured Client limit.
        if symbol_queue.qsize() < LIMIT_CLIENT_COUNT:
            LIMIT_CLIENT_COUNT = symbol_queue.qsize()


        # Create processes for each client.
        client_id = 0
        client_df = pd.DataFrame()
        while client_id < LIMIT_CLIENT_COUNT:
            proc = multiprocessing.Process(target=gethistoricaldata, args=(client_id, symbol_queue))
            client_df = client_df.append(pd.DataFrame.from_records([{'client_index': client_id, 'process': proc}], index='client_index'))
            client_id += 1

        for proc in client_df['process']:
            proc.start()
            time.sleep(0.25)

        for proc in client_df['process']:
            proc.join()


    ibkr_download_endtime = datetime.now()

    for csv_file_name in os.listdir(TEMP_FOLDER):
        try:
            dbcursor = dbconn.cursor()
            csv_file_ptr = open(os.path.join(TEMP_FOLDER, csv_file_name), 'r')
            print(f"Uploading file: {csv_file_name}")
            if TIME_FRAME == '1 day':
                dbcursor.copy_from(csv_file_ptr,'wtst.ibkr_eod_data(ibkr_symbol, date, open, high, low, close, volume, average, trade_count)', sep=',', null='\\N', size=8192, columns=None)
            else:
                dbcursor.copy_from(csv_file_ptr, 'wtst.ibkr_intraday_data', sep=',', null='\\N', size=8192, columns=None)
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
    get_ibkr_hist_data()