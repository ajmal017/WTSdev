''' Demonstrates different ways to request financial data '''
import logging
import multiprocessing
import os
import queue
import shutil
import time
from datetime import datetime, date
from threading import Thread

import pandas as pd
from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import wtsdblib

# File level configutation/parameters
TEMP_FOLDER = '/home/wts/dev/temp/'
BACKUP_FOLDER = '/home/wts/dev/backup/'
TEMP_FILE = '/home/wts/dev/temp/IBKRLiveFiveData.csv'
DATABASE = 'WTSDEV'
LIMIT_WATCHLIST = 32
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
        self.fileptr.write(','.join(map(clean_csv_value, (\
                self.ibkr_current_symbol, datetime.strftime(datetime.fromtimestamp(time),"%Y%m%d  %H:%M:%S"),
                open_, high, low, close, volume, wap, count))) + '\n'\
                           )

        self.fileptr.flush()

    @iswrapper
    def error(self, reqId, code, msg):
        ''' Called if an error occurs '''
        print('Client: {} Error {}: {} : {}'.format(self.client_index, code, self.ibkr_current_symbol, msg))
        self.processing_flag = 0

# This is the client function that gets called from all the processes.
def ibkr_livefive_client(client_index, symbol_queue):
    logger_name = "Client: {}"+
    client_logger = logging.getLogger(f"Client : {client_index}")
    client_logger.setLevel(logging.DEBUG)
    # Create the client and connect to TWS & Database
    client = MarketReader('127.0.0.1', 7497, client_index)

    # Wait till the API receives connection status with all the data forms.
    while client.processing_flag is None or client.processing_flag == 1:
        print(f"Client {client_index} Connecting...")
        time.sleep(0.2)

    print(f"Client {client_index} Connected...")
    time.sleep(0.25)

    # Closing the dummy file opened as part of object initiation. Actual file will be opened by the process.
    if client.fileptr is not None:
        client.fileptr.close()
    outfile_name = os.path.join(TEMP_FOLDER, 'IBKRLiveFive_Client{}.csv'.format(client_index))
    client.fileptr = open(outfile_name, "w")

    # Set the IBKR contract details
    con = Contract()
    con.secType = 'STK'
    con.exchange = 'NSE'
    con.currency = 'INR'
    req_num = 0
    wts_date = date.today()

    try:
        dbcursor = client.dbconn.cursor()
        dbquery = "SELECT ibkr_symbol FROM wtst.wts_process_map WHERE wts_date = %s AND ibkr_client_index = %s"
        dbparams = (wts_date.strftime("%y%m%d"), client_index)

        dbcursor.execute(dbquery, dbparams)
        client_recordset = dbcursor.fetchall()

        for client_row in client_recordset:
            client.ibkr_current_symbol = client_row[0]
            con.symbol = client.ibkr_current_symbol
            req_num += 1
            client.processing_flag = 1
            client.reqRealTimeBars(req_num, con, 5, "TRADES", True, [])

        # Loop for 1 minute at a time till 4 PM before cancelling the IBKR request and close the process.
        while 1:
            time.sleep(60)
            hour_now = datetime.now().hour
            minute_now = datetime.now().minute
            if hour_now == 15 and minute_now > 30:
                break
        client.cancelRealTimeBars(req_num)
    except:
        print("Client: {} - Exception".format(client_index))

    print("Client: {} - Processing is completed".format(client_index))
    client.fileptr.close()
    client.dbconn.close()
    client.disconnect()
    if client.dbconn is not None:
        client.dbconn.close()

def main():
    ibkr_client_list = dict()
    watch_list = dict()

    for default_handler in logging.root.handlers:
        logging.root.removeHandler(default_handler)
    logging.basicConfig(level=logging.INFO)

    mainlogger = logging.Logger("IBKREOD_MAIN")
    mainlogger.setLevel(logging.WARNING)
    symbol_queue = multiprocessing.Queue()
    symbol_queue.put('SBIN')
    symbol_queue.put('TATAMOTOR')
    mainlogger.critical("Main process started")

    # Request historical bars for each of the stock in the table IBKR_SYMBOLS_EQUITY
    # For the symbol, loop the values in the table.
    dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
    dbcursor = dbconn.cursor()
    dbquery = ''' SELECT ibkr_symbol from wtst.focus_stocks order by averagetradevalue desc'''
    #dbquery = ''' SELECT ibkr_symbol from wtst.focus_stocks where ibkr_symbol != 'RELIANCE' order by averagetradevalue desc '''
    dbcursor.execute(dbquery)
    dbrecordset = dbcursor.fetchall()

    # Initialise a Process map table based on LIMIT_WATCHLIST & LIMIT_CLIENT_COUNT
    processor_id = 0
    client_id = 0
    wts_date = date.today()

    dbquery = 'DELETE FROM wtst.wts_process_map where wts_date = %s'
    dbparams = (wts_date.strftime("%y%m%d"),)
    dbcursor.execute(dbquery,dbparams)

    for dbrow in dbrecordset:
        dbquery = "INSERT INTO wtst.wts_process_map VALUES (%s, %s, %s, 'OFF', 'NONE', %s, NULL, 'OFF', 'NONE')"
        dbparams = (wts_date.strftime("%y%m%d"), dbrow[0], processor_id, client_id)
        dbcursor.execute(dbquery, dbparams)
        processor_id += 1
        if processor_id >= LIMIT_WATCHLIST:
            break
        client_id = (client_id + 1) % LIMIT_CLIENT_COUNT

    dbcursor.close()
    dbconn.commit()

    start_time = datetime.now()
    # Create processes for each client.
    client_id = 0
    client_df = pd.DataFrame()
    while client_id < LIMIT_CLIENT_COUNT:
        proc = multiprocessing.Process(target=ibkr_livefive_client, args=(client_id, symbol_queue))
        client_df = client_df.append(pd.DataFrame.from_records([{'client_index': client_id, 'process': proc}], index='client_index'))
        client_id += 1

    for proc in client_df['process']:
        proc.start()
        time.sleep(0.2)

    for proc in client_df['process']:
        proc.join()

    print(datetime.now())

    ibkr_download_endtime = datetime.now()


    for csv_file_name in os.listdir(TEMP_FOLDER):
        try:
            dbcursor = dbconn.cursor()
            csv_file_ptr = open(os.path.join(TEMP_FOLDER, csv_file_name), 'r')
            print(f"Uploading file: {csv_file_name}")
            dbcursor.copy_from(csv_file_ptr,'wtst.ibkr_livefive_data', sep=',', null='\\N', size=8192, columns=None)
            csv_file_ptr.close()
            dbconn.commit()
            print(f"Successfully uploaded data from {csv_file_name}")
            shutil.move(os.path.join(TEMP_FOLDER, csv_file_name),
                        os.path.join(BACKUP_FOLDER, csv_file_name+"_" + datetime.strftime(datetime.now(), "%Y%m%d_%H%M%S")))
            dbcursor.close()
            dbconn.commit()
        except psycopg2.Error as err:
            print(f"Db error: {csv_file_name} : {err}")
            # Continue execution with new DB connection.
            dbconn.close()
            dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
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