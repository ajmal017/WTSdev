''' Demonstrates different ways to request financial data '''
import queue
from datetime import datetime, timedelta
from threading import Thread
import time
import multiprocessing
import os, shutil
import logging
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import talib as ta


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import wtsdblib

TEMP_FOLDER = '/home/wts/dev/temp/'
class TrendRegression():
    def __init__(self,ibkr_symbol, period_start: datetime, period_end: datetime,min_periods_for_trend_identification, trend_minimum_gradient, trend_minimum_reg_coeff, pct_change_limit, break_even):
        self.ibkr_symbol = ibkr_symbol
        self.start_period = period_start
        self.end_period = period_end
        self.current_period = period_start
        self.min_periods_for_trend_identification = min_periods_for_trend_identification
        self.trend_minimum_gradient = trend_minimum_gradient
        self.trend_minimum_reg_coeff = trend_minimum_reg_coeff
        self.pct_change_limit = pct_change_limit
        self.break_even = break_even
        self.price_data_df = pd.DataFrame()
        self.trend_data_df = pd.DataFrame()
        self.first_data = True
        self.price_index = -1
        self.current_price = -1
        self.prev_price = -1
        self.last_exception_index = -1
        self.current_trend_index = -1
        self.trend_start = None
        self.trend_start_index = -1
        self.trend_end = None
        self.trend_end_index = -1
        self.trend_high_index = -1
        self.trend_high_price = 0
        self.trend_low_index = -1
        self.trend_low_price = 0
        self.number_of_periods_in_trend = 0
        self.trend_status = 'NONE'
        self.trend_momentum = 0
        self.trend_regression_gradient = 0
        self.trend_regression_intercept = 0
        self.trend_regression_coefficient = 0
        self.price_at_trend_start = None
        self.price_at_trend_identification = None
        self.price_at_trend_close = None
        self.trend_pct_change_total = None
        self.trend_pct_change_locked = None

    def calc_regression(self):
        pass

    def add_data(self,new_data):
        if new_data['date'] < self.start_period or new_data['date']>self.end_period:
            print(f'Data does not fall between {self.start_period} and {self.end_period}')
            return


        if self.first_data == True:
            self.current_price = new_data['close']
            self.prev_price = self.current_price
            self.first_data = False
        else:
            self.prev_price = self.current_price
            self.current_price = new_data['close']

        self.current_period = new_data['date']

        self.price_data_df = self.price_data_df.append(pd.DataFrame.from_records([new_data],index='index'))
        self.price_index += 1

        # Start a new trend if this is first data
        if self.current_trend_index == -1:
            self.add_new_trend(self.price_index,self.current_period)
        else:
            self.trend_end_index = self.price_index
            self.trend_end = self.current_period
            self.trend_data_df.loc[self.current_trend_index, 'trend_end_index'] = self.trend_end_index
            self.trend_data_df.loc[self.current_trend_index, 'trend_end'] = self.trend_end

        if self.trend_status == 'NONE':
            if (abs(self.current_price - self.prev_price)*100/self.prev_price) > self.pct_change_limit:
                print(f'Abnormal trading data encountered: {self.price_index}')
                self.price_data_df.loc[self.price_index, 'signal'] = 'ABNORMAL'
                self.price_data_df.loc[self.price_index, 'abnormal_value'] = self.current_price

                self.trend_start_index = self.price_index
                self.trend_start = self.current_period
                self.trend_data_df.loc[self.current_trend_index, 'trend_start_index'] = self.trend_start_index
                self.trend_data_df.loc[self.current_trend_index, 'trend_start'] = self.trend_start

            # Calculate regression parameters. Check for the trend - If yes- change trend status or roll over the trend start date
            #print (self.price_data_df.loc[self.trend_start_index:self.trend_end_index,['date','close']])
            if (self.trend_start_index + 1) < self.trend_end_index:
                priceval = self.price_data_df.loc[self.trend_start_index:self.trend_end_index,['close']]
                #print(priceval)
                index = [indexval for indexval in range(self.trend_end_index - self.trend_start_index + 1)]
                #print(index)
                self.gradient, self.intercept = np.polyfit(index,np.log(priceval['close']),1)
                self.trend_momentum = self.gradient * 375
                estimated_close = [( np.exp(self.gradient * xval) * np.exp(self.intercept)) for xval in index]
                correlation = np.corrcoef( priceval['close'], estimated_close)
                self.r_squared = correlation[0, 1] ** 2
                #print (f'Start = {self.intercept}, Day Slope = {self.gradient*375}, r_squared = {self.r_squared}')
                self.price_data_df.loc[self.price_index, ('trend_regression_gradient')] = self.gradient
                self.price_data_df.loc[self.price_index, ('trend_regression_intercept')] = self.intercept
                self.price_data_df.loc[self.price_index, ('trend_regression_coefficient')] = self.r_squared
                self.price_data_df.loc[self.price_index, ('trend_momentum')] = self.trend_momentum
                # Update Trend end date in both scenarios - trend confirmed as well as trend not confirmed
                self.trend_data_df.loc[self.current_trend_index, 'trend_end_index'] = self.trend_end_index
                self.trend_data_df.loc[self.current_trend_index, 'trend_end'] = self.trend_end

            if (self.trend_end_index - self.trend_start_index) < self.min_periods_for_trend_identification:
                return
            if self.r_squared > self.trend_minimum_reg_coeff and (self.trend_momentum >= self.trend_minimum_gradient or self.trend_momentum  <= (-1 * self.trend_minimum_gradient)) :
                self.trend_status = 'CONFIRMED'
                self.trend_high_index = self.price_index
                self.trend_high_price = self.current_price
                self.trend_low_index = self.price_index
                self.trend_low_price = self.current_price
                self.trend_data_df.loc[self.current_trend_index, ('trend_status')] = self.trend_status
                self.price_at_trend_start = self.price_data_df.loc[self.trend_start_index,'close']
                self.trend_data_df.loc[self.current_trend_index, 'price_at_trend_start'] = self.price_at_trend_start
                self.price_at_trend_identification = self.price_data_df.loc[self.price_index,'close']
                self.trend_data_df.loc[self.current_trend_index, 'price_at_trend_identification'] = self.price_at_trend_identification
                if self.gradient > 0:
                    self.price_data_df.loc[self.price_index,'signal'] = 'BUY'
                else:
                    self.price_data_df.loc[self.price_index, 'signal'] = 'SELL'
                self.price_data_df.loc[self.price_index, 'signal_value'] = self.current_price
            else:
                #If trend is not confirmed increment the start date and wait for next data
                self.trend_start_index += 1
                self.trend_start = self.price_data_df.loc[self.trend_start_index,'date']
                self.trend_data_df.loc[self.current_trend_index, 'trend_start_index'] =self.trend_start_index
                self.trend_data_df.loc[self.current_trend_index, 'trend_start'] = self.trend_start
        elif self.trend_status == 'CONFIRMED':
            #print (self.price_data_df.loc[self.trend_start_index:self.trend_end_index,['date','close']])
            if self.gradient > 0:
                if self.current_price > self.trend_high_price:
                    self.trend_high_index = self.price_index
                    self.trend_high_price = self.current_price
            else:
                if self.current_price < self.trend_low_price:
                    self.trend_low_index = self.price_index
                    self.trend_low_price = self.current_price
            priceval = self.price_data_df.loc[self.trend_start_index:self.trend_end_index,['close']]
            index = [indexval for indexval in range(self.trend_end_index - self.trend_start_index + 1)]
            self.gradient, self.intercept = np.polyfit(index,np.log(priceval['close']),1)
            self.trend_momentum = self.gradient * 375
            estimated_close = [( np.exp(self.gradient * xval) * np.exp(self.intercept)) for xval in index]
            correlation = np.corrcoef( priceval['close'], estimated_close)
            self.r_squared = correlation[0, 1] ** 2
            #print (f'Start = {self.intercept}, Day Slope = {self.gradient*375}, r_squared = {self.r_squared}')
            self.price_data_df.loc[self.price_index, ('trend_regression_gradient')] = self.gradient
            self.price_data_df.loc[self.price_index, ('trend_regression_intercept')] = self.intercept
            self.price_data_df.loc[self.price_index, ('trend_regression_coefficient')] = self.r_squared
            self.price_data_df.loc[self.price_index, ('trend_momentum')] = self.trend_momentum
            # Update trend related parameters
            self.price_data_df.loc[self.trend_start_index:self.trend_end_index, 'estimated_close'] = estimated_close
            self.number_of_periods_in_trend += 1
            self.trend_data_df.loc[self.current_trend_index, 'number_of_periods_in_trend'] = self.number_of_periods_in_trend
            self.price_at_trend_close = self.current_price
            self.trend_data_df.loc[self.current_trend_index, 'price_at_trend_close'] = self.price_at_trend_close
            self.trend_pct_change_total = (self.price_at_trend_close - self.price_at_trend_start) * 100 / self.price_at_trend_start
            self.trend_data_df.loc[self.current_trend_index, 'trend_pct_change_total'] = self.trend_pct_change_total
            self.trend_pct_change_locked = (self.price_at_trend_close - self.price_at_trend_identification) * 100 / self.price_at_trend_identification
            if self.gradient < 0:
                self.trend_pct_change_locked = self.trend_pct_change_locked * -1
            self.trend_pct_change_locked = self.trend_pct_change_locked - self.break_even
            self.trend_data_df.loc[self.current_trend_index, 'trend_pct_change_locked'] = self.trend_pct_change_locked

            if self.gradient < 0 and ((self.current_price - self.prev_price)*100/self.prev_price > self.pct_change_limit):
                print(f'Abnormal trading data encountered. Closing the trend: {self.price_index}')
                self.price_data_df.loc[self.price_index, 'signal'] = 'ABNORMAL'
                self.price_data_df.loc[self.price_index, 'abnormal_value'] = self.current_price
                self.close_trend()
            elif self.gradient > 0 and ((self.prev_price - self.current_price)*100/self.prev_price > self.pct_change_limit):
                print(f'Abnormal trading data encountered. Closing the trend: {self.price_index}')
                self.price_data_df.loc[self.price_index, 'abnormal_value'] = self.current_price
                self.price_data_df.loc[self.price_index, 'signal'] = 'ABNORMAL'
                self.close_trend()
            elif self.trend_momentum  >= (self.trend_minimum_gradient-0.005) or self.trend_momentum  <= (-1 * (self.trend_minimum_gradient-0.005)):
                pass # Don't close the trend
            else:
                #If trend is not confirmed, close the trend and start a new trend.
                self.close_trend()

    def close_trend(self):
        if self.gradient > 0:
            self.price_data_df.loc[self.price_index, 'signal'] = self.price_data_df.loc[self.price_index, 'signal'] + '-SELL'
        else:
            self.price_data_df.loc[self.price_index, 'signal'] = self.price_data_df.loc[self.price_index, 'signal'] + '-BUY'
        self.price_data_df.loc[self.price_index, 'signal_value'] = self.current_price
        self.trend_status = 'COMPLETED'
        self.trend_data_df.loc[self.current_trend_index, ('trend_status')] = self.trend_status
        self.trend_data_df.loc[self.current_trend_index, 'trend_regression_gradient'] = self.gradient
        self.trend_data_df.loc[self.current_trend_index, 'trend_regression_intercept'] = self.intercept
        self.trend_data_df.loc[self.current_trend_index, 'trend_regression_coefficient'] = self.r_squared
        self.trend_data_df.loc[self.current_trend_index, 'trend_momentum'] = self.trend_momentum

        #if self.gradient > 0:
            #self.add_new_trend(self.trend_high_index, self.price_data_df.loc[self.trend_high_index,'date'])
        #else:
            #self.add_new_trend(self.trend_low_index, self.price_data_df.loc[self.trend_low_index,'date'])

        self.add_new_trend(self.price_index,self.current_period)

    # Trend can take four statuses - NONE, CONFIRMED, CLOSED
    def add_new_trend(self, start_index, start):
        # ---------- Run time values. Data not to be replicated in trend data
        self.trend_high_index = -1
        self.trend_high_price = 0
        self.trend_low_index = -1
        self.trend_low_price = 0
        # ------------------------------------
        self.current_trend_index += 1
        self.trend_start = start
        self.trend_start_index = start_index
        self.trend_end = self.price_index
        self.trend_end_index = self.price_index
        self.trend_status = 'NONE'
        self.trend_momentum = 0
        self.trend_regression_gradient = 0
        self.trend_regression_intercept = 0
        self.trend_regression_coefficient = 0
        self.number_of_periods_in_trend = 0
        self.price_at_trend_start = None
        self.price_at_trend_identification = None
        self.price_at_trend_close = None
        self.trend_pct_change_total = None
        self.trend_pct_change_locked = None
        self.trend_data_df = self.trend_data_df.append( pd.DataFrame.from_records([{
            'trend_index': self.current_trend_index,
            'trend_start': self.trend_start,
            'trend_start_index': self.trend_start_index,
            'trend_end': self.trend_end,
            'trend_end_index': self.trend_end_index,
            'number_of_periods_in_trend': self.number_of_periods_in_trend,
            'trend_momentum': self.trend_momentum,
            'trend_regression_coefficient': self.trend_regression_coefficient,
            'trend_pct_change_locked': self.trend_pct_change_locked,
            'trend_status': self.trend_status,
            'price_at_trend_start': self.price_at_trend_start,
            'price_at_trend_identification': self.price_at_trend_identification,
            'price_at_trend_close': self.price_at_trend_close,
            'trend_regression_gradient': self.trend_regression_gradient,
            'trend_regression_intercept': self.trend_regression_intercept,
            'trend_pct_change_total': self.trend_pct_change_total
        }],index = 'trend_index'))

def main():
    print(f'Start:{datetime.now()}')
    period_start = datetime(year=2021,month=7,day=9,hour=9,minute=15)
    period_end = datetime(year=2021,month=7,day=16,hour=15,minute=30)
    ibkr_symbol = 'MPHASIS'
    stock_trend = TrendRegression(
        ibkr_symbol=ibkr_symbol,
        period_start=period_start,
        period_end=period_end,
        min_periods_for_trend_identification=30,
        trend_minimum_gradient=0.07,
        trend_minimum_reg_coeff=0.5,
        pct_change_limit=0.5,
        break_even = 0.15
    )
    print(f'After class instantiation:{datetime.now()}')
    dbconn = wtsdblib.wtsdbconn.newconnection('WTSDEV')
    dbcursor = dbconn.cursor()
    dbquery = ''' select date,close from wtst.ibkr_intraday_data  where ibkr_symbol = %s and date >= %s and date <= %s order by date'''
    dbparams = (ibkr_symbol,period_start,period_end)
    dbcursor.execute(dbquery,dbparams)
    dbrecordset = dbcursor.fetchall()
    print(f'After Query execution:{datetime.now()}')
    idx = 0
    for dbrow in dbrecordset:
        stock_trend.add_data({'index': idx,
                              'date': dbrow[0],
                              'close': dbrow[1],
                              'trend_momentum': 0,
                              'trend_regression_coefficient': 0,
                              'trend_regression_gradient': 0,
                              'trend_regression_intercept': 0,
                              'number_of_periods_remaining_in_trend': 0,
                              'estimated_close': None,
                              'signal': 'NA',
                              'signal_value': None,
                              'abnormal': 'NA',
                              'abnormal_value': None
                              })
        idx += 1
    print(f'After Data looping:{datetime.now()}')
    dbcursor.close()
    dbconn.close()

    print('Total Returns = {:.2f}'.format(stock_trend.trend_data_df['trend_pct_change_locked'].sum()))

    print(stock_trend.trend_data_df.loc[:,('trend_pct_change_locked','number_of_periods_in_trend')])
    #plt.plot(stock_trend.price_data_df.index, stock_trend.price_data_df['close'])
    #plt.plot(stock_trend.price_data_df.index, stock_trend.price_data_df['estimated_close'])
    #plt.ylabel(ibkr_symbol)
    #plt.show()
    symbols = ['circle-open', 'circle', 'circle-open-dot', 'square']

    fig1 = go.Figure()
    fig1.add_trace( go.Scatter(x=stock_trend.price_data_df.index, y=stock_trend.price_data_df['close'],mode='lines',marker_color='rgba(0, 0, 255, .9)',name=ibkr_symbol))
    fig1.add_trace( go.Scatter(x=stock_trend.price_data_df.index, y=ta.SMA(stock_trend.price_data_df['close'],50),mode='lines',marker_color='rgba(255, 128, 128, .9)',name='MA50'))
    fig1.add_trace( go.Scatter(x=stock_trend.price_data_df.index, y=stock_trend.price_data_df['estimated_close'],mode='lines',marker_color='rgba(255, 0, 0, .9)',name='Trend'))
    fig1.add_trace( go.Scatter( x=stock_trend.price_data_df.index, y=stock_trend.price_data_df['signal_value'],mode='markers',
    marker = dict(
        size=8,
        color='rgba(255, 0, 255, .9)',  # set color equal to a variable
    ),
    name='Signal'))
    fig1.add_trace( go.Scatter( x=stock_trend.price_data_df.index, y=stock_trend.price_data_df['abnormal_value'],mode='markers',
    marker = dict(
        size=8,
        color='rgba(255, 0, 0, .9)',  # set color equal to a variable
    ),
    name='Abnormal'))


    fig1.update_layout(title = ibkr_symbol)
    fig1.update_xaxes(showspikes=True)
    fig1.update_yaxes(showspikes=True)
    fig1.show()
    price_data_file_name = 'price_data'+datetime.now().strftime('%Y%m%d%H%M%S')+'.csv'
    stock_trend.price_data_df.to_csv(os.path.join(TEMP_FOLDER,price_data_file_name))
    trend_data_file_name = 'trend_data'+datetime.now().strftime('%Y%m%d%H%M%S')+'.csv'
    stock_trend.trend_data_df.to_csv(os.path.join(TEMP_FOLDER,trend_data_file_name))

    #print(f'After printing:{datetime.now()}')
    #print(stock_trend.price_data_df.index)
#fig = px.scatter(df, x='Date', y='Metric1', color = 'Color', hover_data = ["Color", "Marker"],
#                 symbol = df['Marker'],
#                 symbol_sequence=symbols,
#                 color_discrete_sequence = colors
#                )

if __name__ == '__main__':
    main()

# Pending items
#1. Close the position if MA50 is breached for 3/5 consecutive points
#2. Close the positions at EOD everyday and start a new day fresh.
#3. To capture reversals, start the new trend with the peak/low of just closed trend. (will make plotting difficult)
#4. Check the possibility of loading historic values with calculations.
#5. As the trend length increases, possibility of mini (30%) opposite trend can be checked for both entry and exit.
#6. Spike up (above threshold 0.4%) in the oppostite direction should invaidate a confirmed trend.
#7. Enter with a strict momentum indicator. (0.09 or 0.1)... Relax this criteria for every n data points.
#8. Check if entry can be restricted based on the moving average of last 5 or 3 momentum indicator values.