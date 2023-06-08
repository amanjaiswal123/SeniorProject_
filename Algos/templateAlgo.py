from source.Backtest import Backtest
from source.Commons import TradingDays

#There are other indicators you can import aswell or you can make your own!
from source.Indicators import _RSI


#This is a template for any algorithims you want to create. Do not change the parameters of the function.
def UpTrend(Fields, Portfolio, aval_cash, net_worth, datadate, first_calc, YSignals=None):
    #Fields will represent the data available to you. You can check the user manual for the columns.
    #Portfolio will represent your portfolio. You can check the user manual for the columns.
    #aval_cash is the amount of cash you have available to trade.
    #net_worth is the total value of your portfolio including your open positions
    #datadate is the date of the data you are currently looking at
    #These two arguements are useful for building indicators whose calculation is diffrent the first time or that require data from previous calcualtions.
        #first_calc is a boolean that will be true if this is the first time the function is called for this backtest.
        #YSignals is the signals from the previous day.



    #***Indactors Calculations***

    #Here we are slicing the data to only include data from the last trading day.
    todayData = Fields['adj_close'].index.get_level_values('dat') == datadate
    #Here we are getting the adjusted close for the last trading day
    TodaysAdjustedClose = Fields['adj_close'][todayData]
    #Here we are modifying the dataframe to only include the adjusted close column and drop the date column.
    TodaysAdjustedClose = TodaysAdjustedClose.reset_index('dat')['adj_close']


    #Here we are getting the volume for the last 90 days
    #Get the last 90 days worth of data
    ninetyDays = (Fields['volume'].index.get_level_values('dat') >= TradingDays[TradingDays.index(datadate)-90])
    #Filter the dataframe with the last 90 days and the volume column. Resulting in the volume for the last 90 days
    Volume90 = Fields['volume'][ninetyDays]
    #Get the mean of the volume for each security by grouping by the ticker and exchange and then taking the mean
    Volume90 = Volume90.groupby(['ticker','exchange']).mean()
    #***Filter the volume to only include the securities that are in the adjusted close.***
        #This is a important step, because due to some dataset errors the volume is not always available for every security on every day.
        #For all indacators you create you must make sure that the dataframes all have the same index. As comparasions can only be done with idential indexes.
    Volume90 = Volume90[Volume90.index.isin(TodaysAdjustedClose.index)]
    #Do a the same process for 30 Day Volume
    Volume30 = Fields['volume'][(Fields['volume'].index.get_level_values('dat') >= TradingDays[TradingDays.index(datadate) - 30])].groupby(['ticker', 'exchange']).mean()
    Volume30 = Volume30[Volume30.index.isin(TodaysAdjustedClose.index)]

    # ***Creation of Signal Dataframe***


    #We are now creating the Dataframe that will be returned to the backtesting function.
        #This Dataframe must have the index of ticker, exchange.
        #This Dataframe must have 2 columns.
            #Action: This column will tell the backtesting function what to do with the security.
                #Buy will buy the security.
                #Sell will sell the security.
                #Close will close the position of the security. If it is not in your portfolio, it will be ignored.
                #Nothing will do nothing with the security.
            #Quantity or Allocation
                #You can define either a quantity or an allocation. Where Quantity is a integer, and allocation should be a decimal between 0 and 1.
        #You can add more columns to the dataframe. It is reccomended that you add columns with all the indicators you used to make your decision. As they will be shown in the backtest results.
    # Create a dataframe based on the adjusted close dataframe.
    Pipe = TodaysAdjustedClose.to_frame() #Note that we are converting it to a frame, as by defualt a series will not have a column name.
    # Add the volume columns to the dataframe
    Pipe['Volume 90'] = Volume90
    Pipe['Volume 30'] = Volume30
    #Create the action column and default it to do nothing
    Pipe['Action'] = 'nothing'
    #Assign a default allocation to 5%
    Pipe['Allocation'] = 0.05

    # ***Filtering Buy/Sell Signals Calculations***

    #Set the action based on a filter to buy or close the position
    Pipe['Action'][Pipe['Volume 90'] >= Pipe['Volume 30']] = 'buy'
    Pipe['Action'][Pipe['Volume 90'] <= Pipe['Volume 30']] = 'close'



    #Return the dataframe with the necessary columns.
    # There is no limit to how and what you can calculate you are given all the data needed for calculations.
    # You may do any calculations and filtering as you want, but you must return a dataframe with the proper index and the columns Action and Allocation.
    return Pipe


#Calling the Backtest Function. You pass the function you want to backtest and the parameters for the backtest. You can check the user manual for further details on the parameters.
Backtest(UpTrend,End_Date='2017-04-16',Days=365,StartingOffset=365,SC=100000,BacktestTitle='templateAlgo')