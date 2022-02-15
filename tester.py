from NSEDownload import stocks
import pandas as pd
import matplotlib.pyplot as plt


#reliance has stock split 2017-09-07

stock_sym = 'RELIANCE'
start_date = '2017-01-01'
end_date = '2017-12-31'

#get unadjusted data for each stock in stock_list
df_unadjusted = stocks.get_data(stockSymbol = stock_sym, start_date = start_date, end_date = end_date)

#get adjusted data for each stock in stock_list
df_adjusted = stocks.get_adjusted_stock(stockSymbol = stock_sym, start_date = start_date, end_date = end_date)

plt.plot(df_unadjusted['Close Price'])
plt.title("unadjusted closing price for "+stock_sym)
plt.show()

plt.plot(df_adjusted['Close Price'])
plt.title("adjusted closing price for "+stock_sym)
plt.show()


