from NSEDownload import stocks
import pandas as pd
from configparser import ConfigParser


#read config file to get stock symbol and start and end date
configur = ConfigParser()
print(configur.read('config.ini'))

num_symbol = configur.getint('stock_symbol','num_symbol')
stock_list = []
for i in range(1,num_symbol+1):
    stock_list.append(configur.get('stock_symbol', 'stock'+str(i)))

start_date = configur.get('parameters','start_date')
end_date = configur.get('parameters','end_date')

#folder to save the data of each stock
fileName = r'./data/'
fileName_temp = r'./data/ignore/'

#get adjusted data for each stock in stock_list
for stock_sym in stock_list[20:30]:
    df = stocks.get_adjusted_stock(stockSymbol = stock_sym, start_date = start_date, end_date = end_date)
    print(len(df))
    print(stock_sym)
    df.to_csv(fileName_temp+stock_sym+'.csv')
    df_ = pd.read_csv(fileName_temp+stock_sym+'.csv')
    first_date = str(df_.loc[0,'Date'])
    last_date = str(df_.loc[len(df_)-1,'Date'])
    df.to_csv(fileName+first_date+'_'+last_date+'_'+stock_sym+'.csv')