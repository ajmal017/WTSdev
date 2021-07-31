
from datetime import datetime
from threading import Thread
import time
import pandas as pd
import wtsdblib
import matplotlib.pyplot as plt
import numpy as np

DATABASE = 'WTSDEV'
SCHEMA = 'wtst'

class update_indicators:
    def __init__(self, database, schema):
        self.database = database
        self.schema = schema
        try:
            self.dbconn = wtsdblib.wtsdbconn.newconnection(database)
        except Exception as error:
            print (f'Error: {error}')
            exit(1)
    def __del__(self):
        self.dbconn.close()
    def update_narrow_range(self, table_name):
        # Get unique stocks first
        dbcursor = self.dbconn.cursor()
        if table_name == 'ibkr_eod_data':
            dbquery = f'select distinct ibkr_symbol from {SCHEMA}.{table_name}'
            dbcursor.execute(dbquery)
            dbrecordset1 = dbcursor.fetchall()
            for stock_name in dbrecordset1:
                dbquery = f'''select ibkr_symbol, date, high - low from {SCHEMA}.{table_name} 
                where ibkr_symbol = %s and narrow_range is NULL
                order by date '''
                dbparams = (stock_name[0],)
                dbcursor.execute(dbquery, dbparams)
                dbrecordset2 = dbcursor.fetchall()
                for stock_row in dbrecordset2:
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
            self.dbconn.commit()
            dbcursor.close()
        elif table_name == 'ibkr_intraday_data': # Not implemted in table
            return
        else:
            return

    def update_pct_range (self, table_name):
        dbcursor = self.dbconn.cursor()
        dbquery = f'update {self.schema}.{table_name} set pct_range = ((high-low)*100)/close where pct_range is NULL'
        dbcursor.execute(dbquery)
        self.dbconn.commit()
        dbcursor.close()

    def update_momentum(self,table_name):
        if table_name == 'ibkr_eod_data':
            dbcursor = self.dbconn.cursor()
            dbquery = f'select max(date) from {self.schema}.{table_name}'
            dbcursor.execute(dbquery)
            # Fetch a single record from the cursor. Convert a single value tuple into a date variable.
            (max_date,) = dbcursor.fetchone()
            #dbquery = f'select ibkr_symbol from {self.schema}.{table_name} where date = %s and ibkr_symbol = %s'
            #dbparams = (max_date,'ICICIPRUL')
            #dbquery = f'select ibkr_symbol from {self.schema}.{table_name} where date = %s'
            dbquery = f'''select ibkr_symbol from {self.schema}.{table_name} where date = %s and ibkr_symbol in (select ibkr_symbol from wtst.ibkr_symbols where fo_stock = True)  order by momentum_indicator DESC '''
            dbparams = (max_date,) # todo: need to check if max_date is null
            max_date = datetime(year=2021, month=7, day=16)
            dbcursor.execute(dbquery,dbparams)
            stock_list = dbcursor.fetchall()
            for stock_data in stock_list:
                # Fetched latest 90 records but in ascending order.
                dbquery = f'''select ibkr_symbol, date, close from 
                (select ibkr_symbol, date, close from {self.schema}.{table_name} where ibkr_symbol = %s and date <= %s order by date desc limit 90) temp 
                order by date asc'''
                dbparams = (stock_data[0],max_date)
                dbcursor.execute(dbquery,dbparams)
                price_list = dbcursor.fetchall()
                df = pd.DataFrame(price_list,columns=['ibkr_symbol','date','close'])
                df = df.sort_values(by='date')
                slope, start = np.polyfit(df.index,np.log(df['close']),1)
                df['estimated_close'] = [( np.exp(slope * xval) * np.exp(start)) for xval in df.index]
                correlation = np.corrcoef( df['close'], df['estimated_close'])
                r_squared = correlation[0, 1] ** 2
                if True: # Enabled only while analyzing
                    print(f'Annualised Slope = {slope*250}, r_squared = {r_squared}, Momentum indicator = {slope*250*r_squared*r_squared}')
                    print(df)
                    plt.plot( df['date'], df['close'])
                    plt.plot( df['date'], df['estimated_close'])
                    plt.title(f'Annualised Slope = {slope*250}, r_squared = {r_squared}, Momentum indicator = {slope*250*r_squared*r_squared}')
                    plt.ylabel(stock_data[0])
                    plt.show()
                    continue
                dbquery = f''' update {self.schema}.{table_name} set exp_reg_slope_annual = %s, exp_reg_coefficient = %s, momentum_indicator = %s  
                where ibkr_symbol = %s and date = %s'''
                dbparams = (slope*250, r_squared, slope*250*r_squared*r_squared, stock_data[0], max_date)
                dbcursor.execute(dbquery,dbparams)
            self.dbconn.commit()
            dbcursor.close()
        elif table_name == 'ibkr_intraday_data':
            dbcursor = self.dbconn.cursor()
            #dbquery = f'select max(date) from {self.schema}.{table_name}'
            #dbcursor.execute(dbquery)
            # Fetch a single record from the cursor. Convert a single value tuple into a date variable.
            #(max_date,) = dbcursor.fetchone()

            dbquery = f'''select ibkr_symbol from {self.schema}.{table_name} where date = %s and ibkr_symbol in (select ibkr_symbol from wtst.ibkr_symbols where fo_stock = True)  order by momentum_indicator ASC '''





def main():
    client = update_indicators(DATABASE, SCHEMA)
    EXECUTE_STABLE_INDICATORS = False
    EXECUTE_DEV_INDICATORS = True
    if (EXECUTE_STABLE_INDICATORS):
        print(datetime.now())
        #client.update_narrow_range('ibkr_eod_data')
        client.update_pct_range('ibkr_eod_data')
        print(datetime.now())
    # Execute the current development indicator
    if (EXECUTE_DEV_INDICATORS):
        client.update_momentum('ibkr_eod_data')

    print ('Completed successfully')

if __name__ == '__main__':
    # todo: reset global values based on parameters later
    main()