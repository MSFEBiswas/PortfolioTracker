#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun May 10 08:34:04 2020

@author: Ankan Biswas
"""
import os
import time
import numpy as np
import pandas as pd
import yfinance as yf

from openpyxl import load_workbook
from datetime import datetime, timedelta

# Writes to a sheet that may or may not exist on an Excel file. 
def write_excel_sheet(df, sheet, excelpath):
    book = load_workbook(excelpath)
    writer = pd.ExcelWriter(excelpath, engine='openpyxl') 
    writer.book = book
    writer.sheets = dict((ws.title, ws) for ws in book.worksheets)
    df.to_excel(writer, sheet, index = False)
    writer.save()

# Iterates through a range of dates.
def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

# Input Excel file locations hard-coded.
nse_tickers = pd.read_excel(os.getcwd() + '/NSE_Tickers.xlsx')
tradebook = pd.read_excel(os.getcwd() + '/Demat Account.xlsx', sheet_name = 'Tradebook')
ledger = pd.read_excel(os.getcwd() + '/Demat Account.xlsx', sheet_name = 'Ledger').dropna()

start_dt = datetime.strptime(ledger.iloc[0]['Posting Date'], '%Y-%m-%d').date()
end_dt = datetime.strptime(ledger.iloc[len(ledger)-1]['Posting Date'], '%Y-%m-%d').date()

class tradebook_operations:
    def __init__(self, df):
        self.df = df
        self.date = df['Trade Date']
        self.ticker = df['Symbol']
        self.position = df['Trade Type']
        self.quantity = df['Qty']
        self.price = df['Price']     
    
    # Performs appropriate computation for creating portfolio up to a certain date.
    # Filters tradebook by ticker and tracks investments for each stock.
    def portfolio(self, dt):
        self.df = self.df[(self.date >= start_dt.strftime("%Y-%m-%d")) & (self.date <= dt.strftime("%Y-%m-%d"))]    
        MyPortfolio = pd.DataFrame(columns=['Ticker', 'Quantity', 'Investment Value', 'Price'])
        unique_tickers = self.ticker.unique()
        for ticker in unique_tickers:
            stock_filter = self.df.loc[self.ticker == ticker]
            stock_filter = tradebook_operations(stock_filter)
            total_holdings = 0
            investment_value = 0
            for num in range(len(stock_filter.df)):
                if stock_filter.position.iloc[num] == 'buy':
                    total_holdings += stock_filter.quantity.iloc[num]
                    if total_holdings == 0:
                        investment_value = 0
                    else:
                        investment_value += (stock_filter.quantity.iloc[num])*(stock_filter.price.iloc[num])
                else:
                    total_holdings -= stock_filter.quantity.iloc[num]
                    if total_holdings == 0:
                        investment_value = 0
                    else:
                        investment_value -= (stock_filter.quantity.iloc[num])*(stock_filter.price.iloc[num])
            if total_holdings > 0:
                avg_price = investment_value/total_holdings
                data_list = [ticker, total_holdings, investment_value, avg_price]
                MyPortfolio.loc[len(MyPortfolio)+1] = data_list
        MyPortfolio = (MyPortfolio.sort_values(by=['Ticker'])).reset_index(drop=True).reindex(columns=['Ticker', 'Quantity', 'Price', 'Investment Value'])
        
        return MyPortfolio

class portfolio_operations:
    def __init__(self, df):
        self.df = df
        self.ticker = df['Ticker']
        self.inv_value = df['Investment Value']
        self.quantity = df['Quantity']
        self.price = df['Price']
        
    # Performs appropriate computation for portfolio statistics.
    def statistics(self, dt):
        ltp_list = []
        for i in range(len(self.df)):
            count = 0
            while count == 0:
                for j in range(len(nse_tickers)):
                    if self.ticker.iloc[i] == nse_tickers['NSE Symbol'].iloc[j]:
                        flag = j
                        count +=1
            code = nse_tickers['Yahoo Code'].iloc[flag]
            ltp = yf.download(code, start=dt)['Close']
            ltp_list.append(round(ltp.iloc[0], 2))
    
        self.df['Last Traded Price'] = ltp_list
        self.df['Current Value'] = self.df['Last Traded Price']*self.quantity
        self.df['P&L'] = self.df['Current Value'] - self.df['Investment Value']
        self.df['% Change'] = (self.df['P&L']/self.df['Investment Value'])
        return self.df

class ledger_operations:
    def __init__(self, df):
        self.df = df
        self.date = df['Posting Date']
        self.type = df['Voucher Type']
        self.debit = df['Debit']
        self.credit = df['Credit']
        self.balance = df['Net Balance']
        
    # Performs appropriate computation for net return and total investment up to a certain date.
    def returns(self, dt):
        test_ledger = self.df[(self.date >= start_dt.strftime("%Y-%m-%d")) & (self.date <= dt.strftime("%Y-%m-%d"))]
        total_investment = 0
        total_value = 0
        Portfolio = portfolio_operations(tradebook_operations(tradebook).portfolio(dt)).statistics(dt)
        for i in range(len(test_ledger)):
            if test_ledger.iloc[i]['Voucher Type'] == 'Bank Receipts':
                total_investment += test_ledger.iloc[i]['Credit']
            if test_ledger.iloc[i]['Voucher Type'] == 'Bank Payments':
                total_investment -= test_ledger.iloc[i]['Debit']
        for i in range(len(Portfolio)):
            total_value += Portfolio.iloc[i]['Current Value']
        total_value += test_ledger.iloc[len(test_ledger)-1]['Net Balance']
        net_ret = total_value - total_investment
        return (round(net_ret, 2), round(total_investment, 2))
    
    # Performs appropriate computation for daily portfolio performance
    # Measured against performance of the market index.
    def performance(self):
        start_time = time.time()
        market_benchmark = yf.download('^NSEI', start=start_dt.strftime("%Y-%m-%d")).iloc[0]['Open']
        performance_dict = {}
        performance_dict['Date'] = []
        performance_dict['Capital'] = []
        performance_dict['Margin'] = []
        performance_dict['Investment Value'] = []
        performance_dict['Current Value'] = []
        performance_dict['Unrealized Return'] = []
        performance_dict['Net Return'] = []
        performance_dict['Market % Change'] = []
        
        for dt in daterange(start_dt, end_dt):
            print(dt)
            daily_updated_pf = portfolio_operations(tradebook_operations(tradebook).portfolio(dt)).statistics(dt)
            daily_inv_value = np.sum(daily_updated_pf['Investment Value'])
            daily_cur_value = np.sum(daily_updated_pf['Current Value'])
            daily_net_ret, daily_capital = ledger_operations(ledger).returns(dt)
            daily_unreal_ret = np.sum(daily_updated_pf['P&L'])
            daily_market_benchmark_close = yf.download('^NSEI', start=dt.strftime("%Y-%m-%d")).iloc[0]['Close']
            daily_market_pct = ((daily_market_benchmark_close - market_benchmark)/market_benchmark)
            if len(self.balance[self.date == dt.strftime("%Y-%m-%d")]) > 0:
                daily_margin = self.balance[self.date == dt.strftime("%Y-%m-%d")].iloc[len(self.balance[self.date == dt.strftime("%Y-%m-%d")]) - 1]
            else:
                daily_margin = performance_dict['Margin'][len(performance_dict['Margin'])-1]
            performance_dict['Date'].append(dt)
            performance_dict['Capital'].append(daily_capital)
            performance_dict['Margin'].append(daily_margin)
            performance_dict['Investment Value'].append(daily_inv_value)
            performance_dict['Current Value'].append(daily_cur_value)
            performance_dict['Unrealized Return'].append(daily_unreal_ret)
            performance_dict['Net Return'].append(daily_net_ret)
            performance_dict['Market % Change'].append(daily_market_pct)
        
        performance = pd.DataFrame(performance_dict)
        performance['Realized Return'] = performance['Net Return'] - performance['Unrealized Return']
        performance['Portfolio % Change'] = performance['Net Return']/performance['Capital']
        
        performance = performance.reindex(columns=['Date', 'Capital', 'Margin', 'Investment Value', 'Current Value', 'Realized Return', 'Unrealized Return', 'Net Return', 'Portfolio % Change', 'Market % Change'])
        end_time = time.time()
        run_time = round(((end_time - start_time)/60), 2)
        print(f'Run time = {run_time} minutes')
        return performance

portfolio = portfolio_operations(tradebook_operations(tradebook).portfolio(end_dt)).statistics(end_dt)
write_excel_sheet(portfolio, 'Portfolio', os.getcwd() + '/Demat Account.xlsx')

performance = ledger_operations(ledger).performance()
write_excel_sheet(performance, 'Performance', os.getcwd() + '/Demat Account.xlsx')

print(f'Number of stocks in portfolio = {len(portfolio)}')
print(f'Number of days since start = {len(performance)}')
