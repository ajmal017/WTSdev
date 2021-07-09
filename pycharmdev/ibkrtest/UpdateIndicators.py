
from datetime import datetime
from threading import Thread
import time

from ibapi.client import EClient, Contract
from ibapi.wrapper import EWrapper

import wtsdblib
import pandas as pd
import wtsdblib

DATABASE = 'WTSDEV'
SCHEMA = 'wtst'

def update_narrow_range_indicator(table_name, dbcursor):
    # Get unique stocks first
    dbquery = f'select distinct ibkr_symbol from {SCHEMA}.{table_name}'
    dbcursor.execute(dbquery)
    stock_list = dbcursor.fetchall()
    for stock_name in stock_list:
        dbquery = f'''select ibkr_symbol, date, high - low from {SCHEMA}.{table_name} 
        where ibkr_symbol = %s and narrow_range is NULL
        order by date '''
        dbparams = (stock_name[0],)
        dbcursor.execute(dbquery, dbparams)
        stock_data = dbcursor.fetchall()
        for stock_row in stock_data:
            dbquery = f''' select count(*) from 
            (select (high - low) as range from {SCHEMA}.{table_name} where ibkr_symbol = %s and date < DATE(%s) order by date DESC LIMIT 6) prev_6 
            where prev_6.range > %s '''

            # dbquery = f''' select (high - low) as range from {SCHEMA}.{table_name} where ibkr_symbol = %s and date < DATE(%s) and %s is not NULL order by date DESC LIMIT 6 '''
            dbparams = ( stock_row[0], datetime.strftime(stock_row[1],'%Y-%m-%d %H:%M:%S'),stock_row[2])

            dbcursor.execute(dbquery,dbparams)
            dbrecordset_nr7 = dbcursor.fetchall()
            for dbrow_nr7 in dbrecordset_nr7:
                dbquery = f'''update {SCHEMA}.{table_name} set narrow_range = {(dbrow_nr7[0])+1}
                where ibkr_symbol = %s and date = %s'''
                # print (dbquery)
                dbparams = (stock_row[0], stock_row[1])
                dbcursor.execute(dbquery,dbparams)
                # print(f'{stock_row[0]}  Date: {stock_row[1]} {stock_row[2]} NR7: {dbrow_nr7[0]}')
def update_indicators():
    dbconn = wtsdblib.wtsdbconn.newconnection(DATABASE)
    dbcursor = dbconn.cursor()
    update_narrow_range_indicator('ibkr_eod_data', dbcursor)
    dbcursor.close()
    dbconn.commit()
    dbconn.close()
    print ('Completed successfully')


if __name__ == '__main__':
    # todo: reset global values based on parameters later
    update_indicators()