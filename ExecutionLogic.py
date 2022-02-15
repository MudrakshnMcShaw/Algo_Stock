from pymongo import MongoClient
import logging
import pandas as pd
# import backTestTools #used to fetch historic data and historic option expiries
# import rsi
# import get_sym
import datetime
import calendar
# from dataLogger import logData # Used for logging strategy outputs
import ssl
from configparser import ConfigParser
 

class algoLogic:
    
    '''Global variable declaration'''

    conn = None
    timeData = None
    
    writeFileLocation = r'./BackTestResults/' #Save the results of the backtest in this folder
    
    '''Save the info of the open position in the below df'''

    openPnl = pd.DataFrame(
            columns=['Key','Symbol', 'EntryPrice', 'current_l', 'current_c', 'current_h','Quantity',
                    'PositionStatus', 'Pnl'])

    '''Save the info of close position in the below df''' 

    closedPnl = pd.DataFrame(
                columns=['Key','ExitTime','Symbol', 'EntryPrice', 'ExitPrice',
                        'Quantity','Pnl','ExitType'])
    
    def connectToMongo(self, userName, password):
        
        '''Used for connecting to the mongoDb instance'''

        try:
            dbName = 'OHLC_MINUTE_5_N'
            self.conn = MongoClient(f"mongodb://ramakrishnan_7637:dbfb45b6baf496e24bb4@122.160.0.210:1659/?authSource=admin&readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false", ssl_cert_reqs=ssl.CERT_NONE)
    
            print("Connected successfully to MongoDB!!!")
        
        except Exception as e:
            raise Exception(e)


    def entryOrder(self, data,entry_price,symbol,quantity, entrySide, index=-1):
        
        '''Used to update the new position in the openPnl Df'''

        entryPrice = entry_price
        entryPrice_c = data['Close Price']
        entryPrice_h = data['High Price']
        entryPrice_l = data['Low Price']

        self.currentKey = self.timeData

        positionSide = 1 if entrySide=='BUY' else -1
        
        '''Adding position to the openPnl df using levelAdder method'''

        self.levelAdder(entryPrice,entryPrice_l,entryPrice_c,entryPrice_h, symbol,quantity,positionSide, index)
        
        return entryPrice
    
    def pnlCalculator(self):
        
        '''Calculting the pnl for open and closed positions and saving it to the relevant .csv files'''

        if not self.openPnl.empty:
            self.openPnl['Pnl']=(self.openPnl['current_c']-self.openPnl['EntryPrice'])*self.openPnl['Quantity']*self.openPnl['PositionStatus']
            try:self.openPnl.to_csv(self.writeFileLocation+'openPosition.csv'%())
            except:pass
        
        if not self.closedPnl.empty:
            self.closedPnl['Pnl']=(self.closedPnl['ExitPrice']-self.closedPnl['EntryPrice'])*self.closedPnl['Quantity']
            try:self.closedPnl.to_csv(self.writeFileLocation+'closePosition.csv'%())
            except:pass
    
    def levelAdder(self, entryPrice,entryPrice_l,entryPrice_c,entryPrice_h,symbol,quantity, positionSide, index=-1):
        
        '''Appending the new positon to the openPnl df and saving it to the relevant file'''
        if(index==-1):
            self.openPnl.loc[len(self.openPnl)] = [datetime.datetime.fromtimestamp(self.timeData)]+[symbol] + [entryPrice]+\
                                            [entryPrice_l]+ [entryPrice_c] + [entryPrice_h]+[quantity]+[positionSide] + [0]
        elif(index!=-1):
            self.openPnl.loc[index] = [datetime.datetime.fromtimestamp(self.timeData)]+[symbol] + [entryPrice]+[entryPrice_l]+ [entryPrice_c]+[entryPrice_h]+[quantity]+[positionSide] + [0]
        try:self.openPnl.to_csv(self.writeFileLocation + 'openPosition.csv')
        except:pass

            
    def exit_all_positions(self,exitReason):

        for index , row in self.openPnl.iterrows():

            self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(self.timeData)] +[row['Symbol']]+\
                                                [row['EntryPrice']]+\
                                                [row['current_c']]+\
                                                [row['PositionStatus']*row['Quantity']]+\
                                                [0]+[exitReason]
        
            self.openPnl = self.openPnl.drop(index)

        self.stop_loss_put = 0
        self.stop_loss_call = 0
        
        self.openPnl.to_csv(self.writeFileLocation + 'openPosition.csv')
        self.closedPnl.to_csv(self.writeFileLocation + 'closePosition.csv')

    def exit_byindex(self,exitReason,index):

        row = self.openPnl.loc[index]
        self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(self.timeData)] +[row['Symbol']]+\
                                            [row['EntryPrice']]+\
                                            [row['current_c']]+\
                                            [row['PositionStatus']*row['Quantity']]+\
                                            [0]+[exitReason]
            
        # self.openPnl = self.openPnl.drop(index)
        self.drop_index.append(index)
            
        self.openPnl.to_csv(self.writeFileLocation + 'openPosition.csv')
        self.closedPnl.to_csv(self.writeFileLocation + 'closePosition.csv')



    def run_overdays(self,startDate,endDate,indexName,lossValuePercent,
                        quantity,userName, password):

        # self.connectToMongo(userName,password)

        logFileName = f'./StrategyLog/ExecutionLog_generic_logfile.log'
        logFileName = logFileName.replace(':', '')

        #Delete any old log files of the same name
        f= open(logFileName, 'w')
        f.close()


        logging.basicConfig(level=logging.DEBUG, filename=logFileName,
                            format="%(message)s")

        logging.info('\n-----------------------------New start-----------------------\n')

        self.max_unrealized = -1
        self.max_realized = -1
        self.max_net = -1
        self.min_unrealized = 1e+9
        self.min_realized = 1e+9
        self.min_net = 1e+9
        self.unrealizedPnl = 0
        self.realizedPnl = 0
        self.netPnl = 0

        self.missing_index_data = []

        configur = ConfigParser()
        print(configur.read('config.ini'))

        num_symbol = configur.getint('stock_symbol','num_symbol')
        stock_list = []
        for i in range(1,num_symbol+1):
            stock_list.append(configur.get('stock_symbol', 'stock'+str(i)))


        while(endDate>=startDate):

            # print(startDate)
            if startDate.weekday()==5 or startDate.weekday()==6:
                startDate+= datetime.timedelta(days=1)
                continue

            day = startDate.day
            dayName = startDate.strftime("%A")
            month = startDate.month
            year = startDate.year
            
            startDateTime = datetime.datetime(year,month,day,9,20,0)

            self.timeData = startDateTime.timestamp()

            logging.info('\n\t'+ f'-------------date : {startDate}-------------\n')

            
            fileName = r'./data/ignore/'
            for stock_sym in stock_list[:2]:

                df = pd.read_csv(fileName+stock_sym+'.csv')
                index = df.index[df['Date']==startDate.strftime("%Y-%m-%d")].item()
                SMA_30 = df.loc[index-30:index-1,'Close Price'].mean()

                # print(stock_sym)
                # print(df.loc[index-1,'Close Price'])
                # print(df.loc[index-2,'Close Price'])
                # print(SMA_30)
                # print("*"*10)

                logging.info('\n\t'+ f'stock_sym : {stock_sym}')
                logging.info('\n\t'+ f'yesterday Close Price : {df.loc[index-1,"Close Price"]}')
                logging.info('\n\t'+ f'TDB yesterday Close Price : {df.loc[index-2,"Close Price"]}')
                logging.info('\n\t'+ f'SMA_30 : {SMA_30}')

                if(df.loc[index-1,'Close Price']>SMA_30 and df.loc[index-2,'Close Price']<SMA_30):
                    data = df.loc[index,:]
                    entry_price = data['High Price']
                    logging.info('\n\t'+ f'action : enter buy order : {entry_price}')
                    stockprice = self.entryOrder(data = data, entry_price=entry_price, symbol=stock_sym,
                                                    quantity=1,entrySide='BUY',index=-1)
                
                if(df.loc[index-1,'Close Price']<SMA_30 and df.loc[index-2,'Close Price']>SMA_30):
                    data = df.loc[index,:]
                    entry_price = data['Low Price']
                    logging.info('\n\t'+ f'action : enter sell order : {entry_price}')
                    stockprice = self.entryOrder(data = data, entry_price=entry_price, symbol=stock_sym,
                                                    quantity=1,entrySide='SELL',index=-1)
                
                logging.info('\n\t'+ f'-------------next stock_sym-------------\n')
            logging.info('\n\t'+ f'-------------all stock_sym parsed-------------\n')
            #udpating openposition
            if not self.openPnl.empty:
                
                for index0 , row0 in self.openPnl.iterrows():

                    try:
                        
                        stock_sym = row0['Symbol']
                        df = pd.read_csv(fileName+stock_sym+'.csv')
                        index = df.index[df['Date']==startDate.strftime("%Y-%m-%d")]
                        
                        data = df.loc[index,:]
                        
                        self.openPnl.at[index0, 'current_c'] = data['Close Price']
                        self.openPnl.at[index0, 'current_l'] = data['Low Price']
                        self.openPnl.at[index0, 'current_h'] = data['High Price']
                        # self.openPnl.at[index0, 'prev_c'] = data2['c']
                        # self.openPnl.at[index0, 'prev_h'] = data2['h']

                        
                    except Exception as e:
                        logData(e)
                        logData(f"no data for updating---{timeDataEpoch}---{timeData}---{row0['Symbol']}")
                  
            #check if target or stop_loss is met
            if not self.openPnl.empty:
                drop_index = []
                for index0,row0 in self.openPnl.iterrows():
                    
                    # print(self.openPnl.loc[index0])
                    stock_sym = row0['Symbol']
                    df = pd.read_csv(fileName+stock_sym+'.csv')
                    index = df.index[df['Date']==startDate.strftime("%Y-%m-%d")].item()
                    SMA_30 = df.loc[index-30:index-1,'Close Price'].mean()
                    # print(SMA_30)
                    
                    if(row0['PositionStatus']==-1 and row0['current_h'] > SMA_30):
                        
                        logging.info('\t'+ f'action : exiting sell order {row0["Symbol"]}')
                        exitReason = "exiting sell order"
                        self.closedPnl.loc[len(self.closedPnl)] = [row0['Key']] + [datetime.datetime.fromtimestamp(self.timeData)] +[row0['Symbol']]+\
                                                [row0['EntryPrice']]+\
                                                [row0['current_c']]+\
                                                [row0['PositionStatus']*row0['Quantity']]+\
                                                [0]+[exitReason]

                        drop_index.append(index0)
                    
                    elif(row0['PositionStatus']==1 and row0['current_l'] < SMA_30):
                        
                        logging.info('\t'+ f'action : exiting buy order {row0["Symbol"]}')
                        exitReason = "exiting buy order"
                        self.closedPnl.loc[len(self.closedPnl)] = [row0['Key']] + [datetime.datetime.fromtimestamp(self.timeData)] +[row0['Symbol']]+\
                                                [row0['EntryPrice']]+\
                                                [row0['current_c']]+\
                                                [row0['PositionStatus']*row0['Quantity']]+\
                                                [0]+[exitReason]

                        drop_index.append(index0)
                
                self.openPnl.drop(drop_index,inplace=True)
            
            if not self.openPnl.empty:
                logging.info('\t'+ f'EOD openPnl : ')
                logging.info('\t'+ self.openPnl.to_string().replace('\n', '\n\t'))
            #pnl calculation
            self.pnlCalculator()
            self.unrealizedPnl = self.openPnl['Pnl'].sum()

            if not self.closedPnl.empty:
                self.realizedPnl = self.closedPnl['Pnl'].sum()
                self.netPnl = self.unrealizedPnl+self.realizedPnl

            startDate+= datetime.timedelta(days=1)


        '''-----------Post algo end----------------'''

        #Save the final result in the closed position df and update the final pnl in the log file
        endDatetime = datetime.datetime(endDate.year,endDate.month,endDate.day,15,30,0) 
        endDateTimeEpoch = int(datetime.datetime(endDatetime.year, endDatetime.month, endDatetime.day, endDatetime.hour,endDatetime.minute,0).timestamp())


        # logData(f'------------End of day {endDatetime}---------------')
        
        if not self.openPnl.empty:

            for index , row in self.openPnl.iterrows():

                try:
                    
                    stock_sym = row0['Symbol']
                    df = pd.read_csv(fileName+stock_sym+'.csv')
                    index = df.index[df['Date']==startDate.strftime("%Y-%m-%d")]
                    
                    data = df.loc[index,:]
                    
                    self.openPnl.at[index0, 'current_c'] = data['Close']
                    self.openPnl.at[index0, 'current_l'] = data['Low']
                    self.openPnl.at[index0, 'current_h'] = data['High']
                
                except Exception as e:
                    logData(e)

            for index, row in self.openPnl.iterrows():
                self.closedPnl.loc[len(self.closedPnl)] = [row['Key']] + [datetime.datetime.fromtimestamp(endDateTimeEpoch)] +[row['Symbol']]+\
                                                                [row['EntryPrice']]+\
                                                                [row['current_c']]+\
                                                                [row['PositionStatus']*row['Quantity']]+\
                                                                [0]+['Time up']
                # logData(f'Action : Liquidating all open position')

            self.pnlCalculator()
            self.closedPnl.to_csv(self.writeFileLocation + 'stock_closePosition.csv')
        
        else:
            # logData(f'Action : no open positions')
            self.closedPnl.to_csv(self.writeFileLocation + 'stock_closePosition.csv')
        
        # logData(f'Net pnl overall is {self.closedPnl["Pnl"].sum()}')
        # logData(f'max_unrealized : {self.max_unrealized}')
        # logData(f'max_realized : {self.max_realized}')
        # logData(f'max_net : {self.max_net}')
        # logData(f'min_unrealized : {self.min_unrealized}')
        # logData(f'min_realized : {self.min_realized}')
        # logData(f'min_net : {self.min_net}')
        # logData(f'missing index data : {self.missing_index_data}')
        # logData(f'missing option data : {self.missing_option_data}')
        # logData(f'missing index_data : {self.missing_index_data}')
        # logData(f'missing call_options_data : {self.missing_option_data}')


    
if __name__ == "__main__":
    
    import multiprocessing as mp
    from time import sleep

    #Object creation
    obj = algoLogic()
    
    '''
        Algo to be run between the below days, as this is an intraday algo,
        each day is run as a seperate process using the multiprocessing 
        module
    '''

    #Defining all the input parameters
    
    indexName = 'NIFTY 50'
    lossValuePercent = 0.90
    rewardMultiple =2
    quantity = 50
    userName = 'test'#Enter provided username
    password = 'dGSolvjE8BL6kEqY'#Enter provided password
    
    startDate = datetime.date(2021,12,1)
    endDate = datetime.date(2021,12,31)

    day = startDate.day
    dayName = startDate.strftime("%A")
    month = startDate.month
    year = startDate.year
    
    obj.run_overdays(startDate,endDate,indexName,lossValuePercent,
                quantity,userName, password)

