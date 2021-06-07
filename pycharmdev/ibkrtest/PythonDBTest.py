''' Demonstrates different ways to request financial data '''
from datetime import datetime
from threading import Thread
import time

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import pandas as pd
import wtsdblib



def insert():

    try:

        psymbol='RELIANCE'
        pdate = '20210605'
        popen = 2000
        phigh = 2050
        plow = 1950
        pclose = 2025
        pvolume = 10000000
        paverage = 2020
        pbarcount = 100000

        dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
        dbcursor = dbconn.cursor()
        dbquery = '''INSERT INTO wtst."IBKR_EOD_DATA"("IBKR_SYMBOL","Date", "Open", "High", "Low", "Close", "Volume", "Average", "BarCount") VALUES (%s, date(%s), %s, %s, %s, %s, %s, %s, %s)'''
        dbparams = (psymbol, pdate, popen, phigh, plow, pclose, pvolume, paverage, pbarcount)

        dbcursor.execute(dbquery, dbparams)
        dbconn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into mobile table", error)
    finally:
        dbcursor.close()

def select():

    try:

        dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
        dbcursor = dbconn.cursor()

        # dbquery = ''' SELECT MAX(IED."Date") FROM wtst."IBKR_EOD_DATA" IED WHERE IED."IBKR_SYMBOL" = 'RELIANCE' '''
        dbquery = ''' SELECT ISE."IBKR_SYMBOL" FROM wtst."IBKR_SYMBOLS_EQUITY" ISE '''

        dbcursor.execute(dbquery)
        dbrecordset = dbcursor.fetchall()
        for dbrow in dbrecordset:
            print(dbrow[0])

        dbconn.commit()
    except (Exception, psycopg2.Error) as error:
        print("Failed to insert record into mobile table", error)
    finally:
        dbcursor.close()

def main():
    select()

if __name__ == '__main__':
    main()