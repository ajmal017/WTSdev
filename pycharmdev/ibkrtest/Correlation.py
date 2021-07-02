import pandas as pd
import matplotlib.pyplot as plt
import psycopg2

import wtsdblib as db


def compute_correlation():
    dbconn = db.wtsdbconn.newconnection('WTSDEV')
    dbcursor = dbconn.cursor()
    dbquery= '''select ned."DATE", ied."volume", ned."VOLUME" 
                from wtst."ibkr_eod_data" ied, wtst."NSE_EOD_DATA" ned 
                where ied."ibkr_symbol" = ned."SYMBOL" AND
                ied."ibkr_symbol" = 'RELIANCE'
                and ied."date" = ned."DATE"
                and ied."volume" != ned."VOLUME"'''
    dbcursor.execute(dbquery)
    dbrecordset = dbcursor.fetchall()
    df = pd.DataFrame()
    idx = 0
    for dbrow in dbrecordset:
        temp_df = pd.DataFrame({"date": dbrow[0], "ibkr_volume": dbrow[1], "nse_volume": dbrow[2]}, index=[idx])
        df = pd.concat([df, temp_df])
        idx += 1
    #print(df)
    #df['ibkr_volume'].plot()
    #plt.show()
    dbcursor.close()
    combodf = pd.DataFrame()
    combodf['ibkr_volume']=df['ibkr_volume']
    combodf['nse_volume']=df['nse_volume']
    correlation = combodf.corr()
    print(correlation)
    #plt.matshow(correlation)
    #plt.show()

if __name__ == '__main__':
    compute_correlation()