import pandas
from data_scripts.download_data import get_data_rds
from source.Commons import _datadate
from datetime import datetime,timedelta
from source.Commons import TradingDays
from source.Commons import GetTradingDays
import numpy
from source.Commons import NearestTradingDay, notify
pandas.set_option('mode.chained_assignment', None)
pandas.set_option('display.width', 320)
numpy.set_printoptions(linewidth=320)
from warnings import warn


def Backtest(func,WData=['all'],End_Date=_datadate(), Start_Date=None, Days=None,StartingOffset=0,EndOffset=0,SC = 100000,IntialMarginRequirment=1.5,MaintenceMarginRequirment=1.3,marg_call_protect=True,BacktestTitle='Backtest'):
    try: #Wraping everything in try statement so we may catch it later and output backtest files before crashing


        #Intialization is before the loop.

        start = datetime.now() #Getting the start time for the backtest. We will use this to calculate the total amount of time it took to complete the backtest


        #Checking if you provided the necessary data to proceed
        if Days is None and Start_Date is None:
            raise Exception('You must define either the Start Date or an amount of Days to run the backtest for')
        if Days is not None and Start_Date is not None:
            raise Exception('You can only specify an amount of days or a set a start date not both')
        if type(WData) != list:
            raise Exception('WData must be a list')



        #If you specified Days and not start day the program will calculate the start date based on the amount of days you specified
        End_Date = NearestTradingDay(End_Date)
        if Start_Date == None:
            Start_Date = NearestTradingDay(str(datetime.strptime(End_Date,'%Y-%m-%d') - timedelta(days=Days+StartingOffset))[0:10])
        else:
            Start_Date = NearestTradingDay(str(datetime.strptime(Start_Date,'%Y-%m-%d') - timedelta(days=StartingOffset))[0:10])


        #notify('The ' + BacktestTitle + '_' + Start_Date + '-' + End_Date + ' backtest has started') #Will notify start of backtest and also end later on


        Data = get_data_rds(WData, Start_Day=Start_Date, End_Day=End_Date) #Getting Data for the backtest


        #Checking if we have data for the dates you specified. If we do not the backtest will not run
        DatesinData = set(list(Data.index.get_level_values('dat')))
        if End_Date not in DatesinData:
            raise Exception('We do not have marketdata after',End_Date, 'we only have data before',DatesinData[len(DatesinData)-1],'please adjust your End_Date argument accordingly')
        if Start_Date not in DatesinData:
            raise Exception('We do not have marketdata before',Start_Date, 'we only have data after',DatesinData[0],'please adjust your Start_Date argument accordingly. Your starting offset will also effect your start date as the actual start date will be the startdate-starting offset')
        #Getting all the dates that should be in the Data by filtering trading days between the start and end dates
        Dates = GetTradingDays(Start_Date,End_Date)
        Start_Date = str(datetime.strptime(Start_Date, '%Y-%m-%d') + timedelta(days=StartingOffset))[0:10]
        Start_Date = sorted(numpy.array(Dates)[Start_Date <= numpy.array(Dates)])[0]
        if Start_Date not in DatesinData:
            raise Exception('We do not have marketdata before',Start_Date, 'we only have data after',DatesinData[0],'please adjust your Start_Date argument accordingly. Your starting offset will also effect your start date as the actual start date will be the startdate-starting offset')



        #Initialization variables
        IntegCheckEndTime = numpy.nan
        orders_len = 1
        net_worth = SC # The balanace of your portfolio. Will be equal to Starting Capital or SC
        aval_cash = SC #The cash you have available. Will be equal to Starting Capital or SC
        Daily_Balances = pandas.DataFrame(columns=['Net Worth','Exposure','Date']).set_index('Date') #The dataframe that stores the daily balance of your portfolio.
        TradingDaysCount = Dates.index(Start_Date)+1 #The total number of trading days that have passed in the backtest including today so on day 1 it will be 1 day 2 it is 2 ect...
        YearOverYearBacktestSummary = pandas.DataFrame({'Date':[Dates[TradingDaysCount+1]],'Hit Rate':[0],'Net Worth':[SC],'Max DrawDown':[net_worth],'Yearly Gains':[0]}).set_index('Date')
        Transactions = pandas.DataFrame() #Dataframe containing all the transactions of a stock
        Portfolio = pandas.DataFrame(columns=['Allocation','Stop Loss Percent','Holding Period','purchase price','Opened Position On','ticker','exchange', "Amount Holding","Stop Loss","Triggered Stop Loss","Stop Limit","max drawdown"]).set_index(['ticker','exchange']) #Your Portfolio updated Daily after the interval is passed
        TradeNumber = 0 #Every Trade will have a trade # that is assigned after it is sold
        first = True



        print('\nStarting Backtest on', Start_Date,'with','$'+str(SC))
        for Today in Dates[TradingDaysCount+1:len(Dates)-EndOffset-1]:     #Starting Iteration of Days in Back Test
            TradingDaysCount += 1 #TradingDaysCount keeps track of the amount of days your algo has been running. On day 1 it will be 1, on day 2 it will 2, and so on....
            Start = datetime.now() #Getting Start time for each day so we can print the total time it took at the end of each day
            print('\n'+Today,'\n') #Printing the current date your algo is running on
            Fields = Data.loc[Data.index.get_level_values('dat') < Today] #Fields is all the data you will have available to use for calculations. It is limited to all the data before today. It does not include the current day's data as this algorithm will be running in realtime before the market opens. Therefore you would not know the data for today, as you are placing orders before the market opens
            datadate = TradingDays[TradingDays.index(Today)-1] #The data date represent the date we have data up to. Since we place orders before the market opens, we should not access the data for the current day. This ensures there is no look forward bias. The only time we use the data from today is to get the open price so we can calculate the change in the value of the positions and in order to update our portfolio values.
            OpenPrice = Data["adj_open"][Data.index.get_level_values("dat") == Today].reset_index(level=2,drop=True) #This is the open price for today. It will be used to calculate costs and commissions, and portfolio values. It should not be used for any calculations in your algorithm. In reality you will not have this avalaible for calculations when running in realtime, because you are making calculations before the market opens.



            #Calculating the margin, equity, and using those to calculate net worth
            margin = (Portfolio["Amount Holding"][Portfolio["Amount Holding"] < 0]*OpenPrice[(OpenPrice.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index))]).sum()
            #Margin is the amount of money you have borrowed. If you short a share the amount of money you have borrowed is equal to the current price(OpenPrice) of that share times how much you own(Amount Holding Column in Portfolio). This value will be negative if you owe money, as all short orders will have a negative Amount Holding value in your Portfolio and when you multiply it by the Open Price which will always be positive, the result will always be a negative value.. If the share price goes up then the amount on margin does as well, and vice versa if it goes down. We will take the sum of (Amount of shares you own(Amount Holding Column in Portfolio) * the Open Price of the Shares) for all short orders to see how much you have on margin.
            maintenance_margin = margin * MaintenceMarginRequirment #This is our maintenance margin. It represents the total amount of you have on margin times the maintenance margin requirement. You can change the maintenance margin requirement by modifying the function parameter maintenance_margin_requirement. The worth of your long positions + your available cash should be greater than this or else you will face a margin call. Resulting in the end of your backtest
            equity = (Portfolio["Amount Holding"][Portfolio["Amount Holding"] > 0]*OpenPrice[(OpenPrice.index.isin(Portfolio[Portfolio["Amount Holding"] > 0].index))]).sum() #The equity we have is equal to the worth of your long positions. It is calculated by (Amount of Shares you Own(Amount Holding Column in Portfolio) * Open Price)
            net_worth = equity+aval_cash+margin#Finaly your calculated margin and equity with your avalaible cash(aval_cash) will be used to calculate your net worth. The sum of all of these will represent your net worth.
            ELVIM = equity+aval_cash+maintenance_margin
            # We do not subtract the margin because it will be a negative value inherently, as all short orders will have a negative Amount Holding value in your Portfolio and when you multiply it by the Open Price which will always be positive, the result will always be a negative value. Therefore adding a negative value(What we are doing) equates to subtracting a positive value.
            AllocationRemaining = aval_cash/net_worth#Should this be cash in general or available cash - margin or available cash - margin*maintenance margin requirement? In essence should this represent the amount of cash relative to our net worth or the amount of cash we can spend before facing a margin call


            #This is everything you must define within your signals dataframe to function properly - Edit this properly
                #A Data Frame with the index being ticker and exchange and the following columns:
                #Action - Must be a Buy,Sell or nothing.
                #Allocation - The amount to allocate to the signal must be a less than 1 but greater than 0. Basically a percent but in a decimal form
                #or
                #Quantity, the number of shares you want to buy.
                #Stop Loss Percent - The Percent Stop Limit for the signal in decimal form. This will be calculated using the open price - Not Yet Implemented
                #Stop Loss - The Stop Loss Price for your security
                # Stop Limit Percent - The Percent Stop Limit for the signal in decimal form. This will be calculated using the open price  - Not Yet Implemented
                # Stop Limit - The Stop Limit Price for your security
                    #If marg_call_protect is true then STop Loss and Limit must be defined
                #Holding Period - Max amount of trading to hold the stock for.(Only needed for Buy Signals) if there is no holding period set it to Infinite
                #All the indicators you used - Keeping calculations used to make buy and sell signals in the dataframe helps you analyze the post backtest results. The values for those on the indicators at the time of purchase will be recorded and the new values will be updated daily in the portfolio.
            #Calling your function it should be able to accept three arguments
                #Fields: The Data
                #Portfolio - Your current portfolio
                #aval_cash - The cash you currently have available
                #net_worth - Your cash+equity+margin
                #datadate - the date data is available(Yesterdays date.)
                #first - If this is the first calculation or not
                #Signals - Some inductors require previous values to calculate future values such as EMA's. Therefore your function should accept yesterdays calculated values as Signals
            #After the first run we pass the previous days calculations incase the user needs yesterdays data to calculate a moving average
            SignalsStartTime = datetime.now()
            if first == True: #If it is the first run we don't have yesterdays calculations so we wont pass it
                Signals = func(Fields=Fields,Portfolio=Portfolio,aval_cash=aval_cash,net_worth=net_worth,datadate=datadate,first_calc=first) #Getting calculations
            else: #After the first run we will pass yesterdays calculations
                Signals = func(Fields=Fields,Portfolio=Portfolio,aval_cash=aval_cash,net_worth=net_worth,datadate=datadate,first_calc=first,YSignals=Signals) #Getting calculations #Getting calculations and passing yesterdays Data
            SignalsTime = datetime.now()-SignalsStartTime
            #Converting Stop Loss Percents to actual Prices and Vice Versa



            # Inserting Orders to close positions who have hit a stop loss or pass their expiry. These will be placed at the top of the Signals so they will be excuted before any other orders.
            Signals["expired"] = False  # Creating the expired column in signals
            Signals["Triggered Stop Loss"] = False #Creating Column for Triggered Stop Loss
            if len(Portfolio) > 0: #Checking we have any positions. If we have none then there is no need to check for stop Loss or expiries
                if "Stop Loss" in Portfolio.columns.values:#Checking if Stop Loss is in Portfolio columns. If it is not then we cannot check for stop orders
                    Portfolio["Triggered Stop Loss"] = False #Creating the column that will tell if it is a stop order or not
                    #We need to create different filters for short and long orders because a short order stop loss is triggered when the price rises above the Stop Loss and a long order's stop loss is triggered when the price falls below the stop loss
                    stop_loss = Portfolio["Stop Loss"][Portfolio["Amount Holding"] > 0] >= Portfolio["Yesterday's Adjusted Close"][Portfolio["Amount Holding"] > 0] #Stop Loss Filter for long orders
                    stop_loss = stop_loss.append(Portfolio["Stop Loss"][Portfolio["Amount Holding"] < 0] <= Portfolio["Yesterday's Adjusted Close"][Portfolio["Amount Holding"] < 0]) #Appending the Stop Loss Filter for short orders to the Stop Loss filters for long orders. We append it because now this filter will contain all Stop Loss triggers for all values in our portfolio and we do not need to deal with long and short orders separately.
                    Portfolio["Triggered Stop Loss"][stop_loss] = True #Chaning the Stop Loss value to True for all positions
                    #positions whose stop losses have been triggered and are in our portfolio but have an order placed for it are handled differently than positions whose stop loss have been triggered but not orders for it. Portions who have orders and whose stop loss has been triggered we add the amount it would take to close the positions to the quantity of the order that needs to be placed. This is because since the Stop Loss would have been triggered in the last trading day in Real Time it would have been closed already as we are now placing the order before the open of the current trading day. Therefore we just combine the act of closing(The act of closing a position is the same as as positions whose stop loss is triggered but has no order that needs to be placed and is described further in this comment) the position and placing the new order by adding them. Adding works for long and short orders because the Amount Holding of short orders will be negative so adding is the same as subtracting. For positions that have the stop loss triggered but have no orders that need to be placed. We simply close that position by multiplying the positions Amount Holding by -1 and setting that as the Quantity that needs to be placed. This works for short orders as the Amount Holding will be negative therefore when we multiply by -1 it is the inverse of the current Amount Holding and the result will be 0, or a closed position.
                    trgstoploss = Portfolio[["Amount Holding"]][(Portfolio["Triggered Stop Loss"] == True) & (Portfolio.index.isin(Signals.index))].rename(columns={"Amount Holding":"Quantity"})*-1
                    if len(trgstoploss) > 0:
                        Signals["Quantity"][Signals.index.isin(Portfolio[Portfolio["Triggered Stop Loss"] == True].index)] += trgstoploss #Adding the amount needed to close the position for positions whom already have an order
                    stop_loss_not_in_signals = Portfolio[["Amount Holding","Triggered Stop Loss"]][(Portfolio["Triggered Stop Loss"] == True) & ~Portfolio.index.isin(Signals.index)].rename(columns={"Amount Holding":"Quantity"}).copy() #Creating orders for positions who do not have an order
                    stop_loss_not_in_signals["Quantity"] = stop_loss_not_in_signals["Quantity"]*-1 #Setting the Quantity to order to the inverse of the Amount Holding so it cancels out to 0 and the position will be closed
                    stop_loss_not_in_signals["Holding Period"] = 0 #Adding this column because if it is not there when we append it changes the dtype to float which causes problems later on when adding Holding Period to find expiry
                    Signals = Signals.append(stop_loss_not_in_signals) #Adding the new orders to Signals
                    Signals["Triggered Stop Loss"][Signals.index.isin(Portfolio[(Portfolio["Triggered Stop Loss"] == True)].index)] = True #Adding the stop loss values to signals
                if "Expiry" in Portfolio.columns.values: #Closing Positions which have passed their expiry date
                    if "Triggered Stop Loss" not in Portfolio.columns.values: #We do not do anything to orders whose stop loss has been triggered. This is because technically those would have not been our portfolio as they would have been sold yesterday. We must have the Stop Loss column because later on we filter to avoid Stop Loss and if the column is not defined an error will be raised
                        Portfolio["Triggered Stop Loss"] = False #Creating Stop Loss Column
                    else:
                        Portfolio["Triggered Stop Loss"][Portfolio["Stop Loss"].isna()] = False #If there are nan Stop Loss values then we wouldn't evaluate their expiries since they wouldn't pass a filter. Therefore we replace nan values with False so those will pass our filter and its Stop Loss will be evaluated
                    #Positions that are expired but have an order that needs to be placed will have the amount needed to close its position added to the current order(Buy Orders will be subtracting and Short Orders will be adding as the amount needed to cover it is the inverse of the Amount Holding). Positions that have expired will simply be closed by placing an Order that is the inverse of its Amount Holding.
                    Portfolio["expired"] = False #Setting the default expiry to False
                    Portfolio["expired"][pandas.to_datetime(Portfolio["Expiry"]) <= datetime.strptime(Today,'%Y-%m-%d')] = True #Setting the expired value for Expired positions to True.
                    trgstoploss = Portfolio[["Amount Holding"]][(Portfolio["Triggered Stop Loss"] == True) & (Portfolio.index.isin(Signals.index))].rename(columns={"Amount Holding": "Quantity"}) * -1
                    if len(trgstoploss) > 0:
                        Signals["Quantity"][Signals.index.isin(Portfolio[Portfolio["Triggered Stop Loss"] == True].index)] += trgstoploss  # Adding the amount needed to close the position for positions whom already have an order
                    expiry_not_in_signals = Portfolio[["Amount Holding","Triggered Stop Loss"]][(Portfolio["expired"] == True) & (Portfolio["Triggered Stop Loss"] == False) & ~Portfolio.index.isin(Signals.index)].rename(columns={"Amount Holding":"Quantity"}).copy() #Adding the amount needed to close the position to the current order for positions that have a current order
                    expiry_not_in_signals["Quantity"] = expiry_not_in_signals["Quantity"]*-1 #Creating Orders to close the position in our Portfolio for positions that do not have any current orders
                    expiry_not_in_signals["Holding Period"] = 0 #Adding this column because if it is not there when we append it changes the dtype to float which causes problems later on when adding Holding Period to find expiry
                    Signals = Signals.append(expiry_not_in_signals) #Adding the newly created orders to Signals so they are placed first
                    Signals["expired"][Signals.index.isin(Portfolio[(Portfolio["expired"] == True)].index)] = True #Setting the expired value in signals
                Signals = Signals.sort_values(["Triggered Stop Loss","expired"],ascending=False) #Sorting  Signals so that these Stop Loss and Expired values are at the top. Stop Loss orders come before Expired orders.




            #You can place buy and sell orders in two ways. Either creating negative quantities for sell orders and positive quantities for buy orders, or you can use the Action column and explicitly state if it is a buy or sell signal. The action column will over rule a negative or positive value. For example if you pass a negative quantity with a action value of buy, it will be processed as a buy order.
            if "Action" in Signals.columns: #If the Action column is defined then we will make all orders with a sell action have a negative quantity and all orders with a buy action a positive quantity, regardless of it's original sign.
                Signals["Action"] = Signals['Action'].str.lower() #taking capitalization out of the values to make it simpler to index
                Signals["Quantity"][(Signals["Action"] == "close") & Signals.index.isin(Portfolio.index)] = Portfolio["Amount Holding"][Portfolio.index.isin(Signals[(Signals["Action"] == "close")].index)] * -1             #Creating orders for orders with a close action - TBC if a close action is specified it should automatically close that position
                Signals["Action"][(Signals["Action"] == "close") & ~Signals.index.isin(Portfolio.index)] = "nothing"
                Signals["Quantity"][(Signals["Action"] == "sell") & (Signals["Quantity"] > 0)] = Signals["Quantity"][(Signals["Action"] == "sell") & (Signals["Quantity"] > 0)]*-1 #Make sure all sell orders are a negative quantity
                Signals["Quantity"][(Signals["Action"] == "buy") & (Signals["Quantity"] < 0)] = Signals["Quantity"][(Signals["Action"] == "buy") & (Signals["Quantity"] < 0)]*-1 #Make sure all buy orders are a positive quantity
            else: #If the column does not exist create an empty Action Column
                Signals["Action"] = numpy.nan #creating the empty action column
            #After we either change the Action column based on the sign of the quantity, or create a empty Action column if it was not defined. We will fill the NA action values using the sign of the quantity.
            Signals["Action"][(Signals["Action"].isna() & (Signals["Quantity"] > 0)) | ((Signals["Action"] == "close") & (Signals["Quantity"] > 0)) | ((Signals["Action"] == "nothing") & (Signals["Quantity"] < 0))] = "buy" #If the Action is na and it has a positive quantity, it is a buy order
            Signals["Action"][(Signals["Action"].isna() & (Signals["Quantity"] < 0)) | ((Signals["Action"] == "close") & (Signals["Quantity"] < 0)) | ((Signals["Action"] == "nothing") & (Signals["Quantity"] < 0))] = "sell"#If the Action is na and it has a negative quantity, it is a sell order.
            Signals["Action"][(Signals["Action"].isna() & (Signals["Quantity"] < 0)) | ((Signals["Action"] == "close") & (Signals["Quantity"] < 0)) | ((Signals["Action"] == "nothing") & (Signals["Quantity"] < 0))] = "sell"
            #By either overwriting order Quantities based on the Action column, or creating a empty action column, then filling the NA action values based on the sign of the order. We allow the user to either define the order as buy or sell by either directly defining it using the Action column, indirectly define it via the sign of the quantity, or a mixture of both and use these direct or indirect methods to fully define the action column.


            orders = Signals[(Signals["Action"] == "buy") | (Signals["Action"] == "sell") | (Signals["Action"] == "modify")].copy() #Isolating the rows with the action, buy,sell, or modify an existing position(TBC) and creating a dataframe with all the orders the user wants to place.


            if "Stop Loss" in orders.columns.values:
                orders["Stop Loss"][orders.index.isin(Portfolio["Stop Loss"].dropna().index)] = Portfolio["Stop Loss"][Portfolio.index.isin(orders.index)] #Modifying Stop Losses is not yet implemented so changing a stop loss can cause you to face a margin call. Therefore if the position is in your portfolio it's stop loss cannot be changed. This should be changed when adding the ability to modify stop losses.
            if "Holding Period" not in orders.columns.values:
                orders["Holding Period"] = "Infinite"




#Limit order functionality TBC
#                # Creating or filling the Limit Price Column to calculate commissions and costs
#                if "Limit Price" in orders.columns.values:  # Create or Fill the limit price column
#                    orders["Limit Price"][orders["Limit Price"].isna()] = orders["purchase price"][orders[
#                        "Limit Price"].isna()]  # If a column is created fill the na values with the purchase price
#                else:
#                    orders["Limit Price"] = orders[
#                        "purchase price"]  # If no column is created set the limit price to the purchase price




            if len(orders) > 0: #If there are no new orders than we can skip any order processing.
                #New Positions are handled differently, they must go through an integrity check before being transacted. The integrity check ensures that we have all the data for that position for the time we want to keep it open. The integrity check works by comparing the dates between when you open the position and when you anticipate to close it to the dates in Data for that position. If a day of data is missing that day will not be in the dates for Data. If both lists are equal the we have all the data for the position until the anticipated close date. The anticipated close date is only known when the user defines the holding period. If no holding period is defined. If no holding period is defined then we do not check the integrity .the  original date the position was first opened on will also be defined.
                #If the holding period is not defined should we check all the data until the end of the backtest?
                #How do we handle orders that are modified?
                new_position = ~orders.index.isin(Portfolio.index)#Getting new positions by filtering to only positions that are not already in our portfolio
                orders['Opened Position On'] = numpy.nan #Creatin a empty open position on column
                orders['Opened Position On'][new_position] = Today #Adding today's date to only the new positions
                IntegCheckStartTime = datetime.now()
                orders_len = len(orders[new_position & (orders["Holding Period"] != "Infinite")].dropna())
                orders["Integrity"] = True #By default the integrity of all the orders is True unless proven otherwise.
                for x in orders[new_position & (orders["Holding Period"] != "Infinite")].dropna().index: #This loop will check if the data integrity of the new positions who have a holding period defined by comparing the dates in Data with the Dates between today and the anticipated closing date of the position. The anticipated close date is equal to Today plus the Holding Period.
                    for y in Data.columns: #Iterating through the columns, then comparing the dates in each column with the dates between today and the anticipated close date.
                        try: #This is put within a try statement because we are indexing without knowing if those dates are in the Data, which may cause a index error.
                            DatesinData = list(Data[y][x][Dates[TradingDaysCount-1:TradingDaysCount + orders['Holding Period'][new_position][x]+1]].dropna().index.get_level_values('dat')) #These are the dates in data for that position and in that column
                            EndDate = TradingDays[TradingDays.index(Today) + orders['Holding Period'][x]] #This is the anticipated closing date of the position
                            if EndDate > Dates[len(Dates)-EndOffset-1]: #If the anticipated closing date is after the last day of the backtest, the then anticipated closing date is set to the last day of the backtest. This avoids the error of index Data with a date that is not as Data only contains Data until the last day of the backtest.
                                EndDate = Dates[len(Dates)-EndOffset-1] #Overwriting the anticipated closing date with the last day of the backtest
                            ActualDates = GetTradingDays(datadate,EndDate) #Getting the dates between today and the anticipated closing date
                            if DatesinData != ActualDates:  # If the lists do not equal then we do not have the data for that position until the anticipated closing date and the data integrity will be flagged as False.
                                orders['Integrity'][x] = False  #Setting the Integrity to False
                                break#Breaking the loop because we will not need to check any further columns
                        except IndexError: #If theres is an index error than the integrity will be set to False
                            orders['Integrity'][x] = False  # if it hits this except statement that means the data was not there and the stock will not be bought
                            break #Breaking becuase we do not need to check any more columns
                invalid_new_orders = (orders['Integrity'] == False) | (orders["Today's Close"] != orders["Today's Adjusted Close"]) #This will filter our orders with orders that have a data integrity, ensuring that no problems with occur in calculations while holding the position. It will also remove all securities that have had a split or merge. Splits or Merges cause problems with our calculations. I do not exactly remember why we avoid these stocks. Although we check if the stock has a split or merge as the if its Close does not equal it adjusted close then it has had a split or merge in the pass
                orders.drop(index=orders[new_position & invalid_new_orders].index,columns=['Integrity'],inplace=True) # Dropping orders with bad data integrity and that have had a split or merger
                IntegCheckEndTime = (datetime.now()-IntegCheckStartTime)



                #Getting the purchase price of the orders, how should na values be treated. Currently they automatically ignored although when we want to close a position this can cause problems.
                orders.drop(index=orders[~orders.index.isin(OpenPrice.index) & (orders["Triggered Stop Loss"] != True)].index,inplace=True) #Dropping orders whose price we do not have. Although if it is a stop loss order then we will place it and the order price will be the Stop Loss. Should this be for even when we are closing a position?
                orders["purchase price"] = numpy.nan #Creating the column
                orders['purchase price'][orders.index.isin(OpenPrice.index) & (orders["Triggered Stop Loss"] == False)] = OpenPrice[OpenPrice.index.isin(orders[orders["Triggered Stop Loss"] == False].index)] #Setting the open price for non-stop limit orders
                orders['purchase price'][(orders["Triggered Stop Loss"] == True)] = Portfolio["Stop Loss"][Portfolio.index.isin(orders[(orders["Triggered Stop Loss"] == True)].index)] #Setting the purchase price for stop limit orders
                orders = orders[~orders["purchase price"].isna()]



                if "Stop Limit" in orders.columns.values:
                    stp_limit_long =  orders[(orders["Quantity"] > 0) & (orders["purchase price"] > orders["Stop Limit"])].index#Filtering out orders whose purchase price is greater than its Stop Limit for long orders
                    stp_limit_short = orders[(orders["Quantity"] < 0) & (orders["purchase price"] < orders["Stop Limit"])].index#Filtering out orders whose purchase price is less than its Stop Limit for short orders
                    stp_limit = stp_limit_long.append(stp_limit_short)
                    orders.drop(index=stp_limit,inplace=True)



                if len(orders) > 0: #if no orders passed the Integrity Check further order processing is not required.
                    # Checking if Quantity or Allocation is defined. If one is defined we will use it to calculate the other. If neither is defined we will the value of both columns to nans
                    if 'Quantity' in orders.columns: #Checking for the quantity column
                        if 'Allocation' in orders.columns: #If the Allocation column is defined then use both columns to fill the missing values in the other column
                            QuantityNA = orders['Quantity'].isna() #Finding na values in Quantity
                            AllocNA = orders['Allocation'].isna() #Finding na values in Allocation
                            orders['Quantity'][QuantityNA] = ((net_worth * orders['Allocation']) / (orders["Today's Close"] * 1.02)-.5).round() #Calculating Quantity using Allocation. We subtract .5 because always want to round down not up
                            orders['Allocation'][AllocNA] = (orders['Quantity'][AllocNA]*orders["Today's Close"][AllocNA])/net_worth #Calculate Allocation using Quantity
                        else: #If Allocation is not defined but Quantity is then Calculate it using the values from quantity
                            orders['Allocation'] = (orders['Quantity']*orders["Today's Close"])/net_worth #Calculting Allocation using Quantity
                    else: #if Quantity is not defined
                        if 'Allocation' in orders.columns: #If Allocation is defined but Quantity is not the n fill use the Allocation Column to full the Quantity column
                            orders['Quantity'] = ((net_worth * orders['Allocation']) / (orders["Today's Close"] * 1.02)-.5).round() #Filling Quantity Column with allocation. We subtract .5 because always want to round down not up
                        else: #If both columns are not defined the set the values for both to na
                            orders['Quantity'] = numpy.nan #Setting Quantity to NA
                            orders['Allocation'] = numpy.nan #Setting Allocation to NA
                    if  len(orders[orders['Quantity'].isna() & orders['Allocation'].isna()]) > 0: #If there are rows with no Allocation or Quantity defined we drop them as thier is not enough information to place a order.
                        warn("Some orders were dropped because there was no Allocation or Quantity Specified") #Warning the user that he did not define a Quantity or Allocation for some rows
                        orders.drop(index=orders[orders['Quantity'].isna() & orders['Allocation'].isna()].index,inplace=True) #Dropping orders without a Quantity or Allocation defined.



                    #Calculating Commissions
                    long = orders["Quantity"] > 0 #Long orders
                    short = orders["Quantity"] < 0 #Short Orders
                    #The current Commissions model on IB is .005 cents per share with a $1 minimum per trade. But if it is more than 1% of the total value of the order than it will be set 1%
                    #EX: 20 shares of a $.01 stock is = .05 * 20 = $.1. But since it less than $1.00 its equal to $1. Although since the total value of the order is $.20 it is then set to $.20.
                    #A 1% of the total value of the order max, $1 min per order, .05 per share. With the precedent defined in that descending order respectively.
                    #Commisions will be a positive value, because when subtracting from our cash positive values represent adding to our costs.
                    orders["Commissions"] = numpy.nan #Creating an empty column
                    #Commissions are .005 per trade by default
                    orders['Commissions'][long] = (orders['Quantity'][long] * .005)  #Default commission for long orders
                    orders['Commissions'][short] = (orders['Quantity'][short] * .005)*-1 #Default Commissions for short order, although we multiply by -1 because for short orders the quantity will be negative and we must make them positive
                    orders['Commissions'][orders['Commissions'] < 1] = 1  # If the commissions are less than $1 than commission is $1
                    one_percent_purchase_price = (orders["Quantity"]*orders["purchase price"])/100 #Calculating 1% of the total value of the order
                    orders["Commissions"][long & (orders['Commissions']  > one_percent_purchase_price)] =  one_percent_purchase_price[long & (orders['Commissions']  > one_percent_purchase_price)]#if commissions are more than 1% of the total order value then commissions is 1% of total order cost
                    orders["Commissions"][short & (orders['Commissions']  > (one_percent_purchase_price*-1))] = (orders["Quantity"]*orders["purchase price"]/100)*-1 #if commissions are more than 1% of the order cost then commissions is 1% of total order cost.  we multiply by -1 because for short orders the quantity will be negative as the quantity will be negative and we must make it positive



                    #Calculating Costs
                    #Positive Costs will take away from our available cash and Buying Power and Negative Cash will add towards it. This is because we subtract from our cash and buying power. Although this is counter intuitive. We do this is because If negative costs took away then when we compare our Cash to our costs to see if we have enough money to place an order. The costs will always be lower as it would have been negative. We would need to make our Cash negative in the comparision to properly compare the values. So to make things simpler the costs that are positive take away and negative adds.
                    orders['Cost'] = orders["purchase price"]*orders['Quantity']#The cost of the order will be the purchase price of the shares * the quantity. Since sell orders have a negative quantity, they will inherently be negative and vice versa for buy orders.



                    if marg_call_protect is True: #TBC Yet to be tested
                        #In order for margin call protect to work a margin position must be submitted with a stop limit and a stop loss. If you do not define those columns then margin marg_call_protect will not work
                        #This is kept here and not by marg call protect because the Filters created below are used in marg call protect. Therefore if we were to filter the na stop limit and losses we would need to create the filterse again

                        if "Stop Limit" not in orders.dropna().columns.values: #Checking if a Stop Limit is defined
                            warn("No Stop Limit Present with margin call active. De-activating Marg Call Protect. To disable this please change the marg_call_protect paramater to False")
                            marg_call_protect = False
                        else:
                            if len(orders[orders["Stop Limit"].isna()]) > 0:
                                warn("Some orders did not have a Stop Limit. They were dropped. To stop this please set the Marg Call Protect function parameter to false")
                            orders = orders[~orders["Stop Limit"].isna()]
                        if "Stop Loss" not in orders.dropna().columns.values:
                            warn("No Stop Loss Present with margin call active. De-activating Marg Call Protect. To disable this please change the marg_call_protect paramater to False")
                            marg_call_protect = False
                        else:
                            if len(orders[orders["Stop Loss"].isna()]) > 0:
                                warn("Some orders did not have a Stop Loss. They were dropped. To stop this please set the Marg Call Protect function parameter to false")
                            orders = orders[~orders["Stop Limit"].isna()]



                    #Calculating the Quantity of the Position after the trade("Amount Holding" column)
                    Sell = orders['Action'] == 'sell'  # Isolating all sell signals
                    Buy = orders['Action'] == 'buy'  # Isolating all Buy
                    short = orders.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index)  # Current short position
                    long = orders.index.isin(Portfolio[Portfolio["Amount Holding"] > 0].index)  # Current long position
                    # Adding Buy orders and Subtracting Sell orders from our Portfolio to get the quantity after the order is placed
                    PortQPOrderQ = orders[~orders.index.isin(Portfolio.index) & (orders["Action"] == "buy")]["Quantity"].copy() #Orders that are not in our Portfolio have a Quantity of zero, therefore their after order Quantity will be set to the Order Quantity
                    PortQPOrderQ = PortQPOrderQ.append((Portfolio[Portfolio.index.isin(orders[Buy].index)]["Amount Holding"] + orders[orders.index.isin(Portfolio.index)][Buy]['Quantity'])) #Portfolio Quantity + Order Quantity = After Order Quantity for Buy orders
                    PortQMOrderQ = orders[~orders.index.isin(Portfolio.index) & (orders["Action"] == "sell")]["Quantity"].copy() #Orders that are not in our Portfolio have a Quantity of zero, therefore thier after order Quantity will be set to the Order Quantity
                    PortQMOrderQ = PortQMOrderQ.append((Portfolio[Portfolio.index.isin(orders[Sell].index)]["Amount Holding"] + orders[orders.index.isin(Portfolio.index)][Sell]['Quantity'])) #Portfolio Quantity + -(Order Quantity(Sell orders are Negative so this is technically subtraction)) = After Order Quantity for Buy orders
                    orders["Amount Holding After Trade"] = numpy.nan
                    orders["Amount Holding After Trade"][(orders["Action"] == "sell")] = PortQMOrderQ
                    orders["Amount Holding After Trade"][(orders["Action"] == "buy")] = PortQPOrderQ



                    #Here we are average the purchase price of a position. The we average is calculated as follows:
                        #This for orders that long(buy) to a long position(positive) or a short order(sell) to a short position.
                            #The average purchase price is equal to the (Amount Holding in portfolio times the purchase price(The total dollar value of that position at the time of purchase) plus the (Quantity in orders times the orders purchase price(Purchase Price for that order(Tomorrow's open)) (The total dollar value of that order))) / The amount we will have after the order is complete.
                                #(((Orders Amount Holding)*(Orders Purchase Price))+((Portfolio Amount Holding)*(Portfolio Purchase Price)))/(Amount Holding After Trade)
                                #This calculation gives us the average price of the order by adding the total value of the position our portfolio and the value of the order and dividing it equally among the shares we currently own. For short orders we multiply the negative values by negative 1 to create a positive sum.
                        #For orders that create New positions the purchase price is not averaged and is simply tomorrow's open as all other orders are.
                        #Selling a Long Position, or Buying a Short Position the purchase. The purchase price remains the same
                        #The reason we only average when buying more of a long position or selling more of a short position is because when we create more of a existing position is because when you are creating a new position there needs to be no average as there is no current position to average it with. Less obviously the reason we do not average decreasing a position such as a sell order on a long position or a buy order on a short position is because if we were to re-average using the calculation mentioned previously, the final sum could be negative or it very small. For example if we bought 10 share for $10 our average purchase price would be $100. If we then sold 5 shares for $5, using the calculation above our new order total would be ((-5*$5)+(10*$10))/5 = $15. Since the purpose of purchase price is a metric to gauge how a position did overtime, this new average would be confusing as we sold shares for a loss yet our average went up. Instead if we keep the purchase price the same but remove 5 shares from the Amount Holding in our portfolio it makes more sense intuitively as now our average purchase price is the same and it will be easier to gauge how that position did overtime. It is important that this is only used as metric to gauge a position overtime and not used to calculate gains when closing the position as it does not accurately represent the total value of our position as the first calculation did which is the true total value of the positions

                    #Firstly we will need to identify which orders are Buy to a long position and Sell to a Short Position.
                    PortQMOrderQN = orders.index.isin(Portfolio.index) & orders.index.isin(PortQMOrderQ[PortQMOrderQ < 0].index) #After Order Quantity that is less than zero for sell orders
                    PortQPOrderQP = orders.index.isin(Portfolio.index) & orders.index.isin(PortQPOrderQ[PortQPOrderQ > 0].index).copy() #After Order Quantity that is above zero for buy orders

                    s_ex_short = Sell & short & PortQMOrderQN & (orders.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index))  # selling more of a existing short position
                    b_ex_long = Buy & long & orders.index.isin(Portfolio.index) & PortQPOrderQP #Buying more of a existing long position

                    #Now we will calculate the average purchase price for those two case scenarios(s_ex_short and b_ex_long)
                    #The calculation is different for short orders as we multiply the order total and portfolio total by negative 1 so we can produce a positive sum
                    orders_total = (orders["purchase price"]*orders["Quantity"]) #The total cost an order cost us. Short orders are negative although we will account for this as we deal with short and long orders separately and will multiply the short orders by -1.
                    orders_total = orders_total[orders_total.index.isin(orders.index)] #Some orders that I assume are from our portfolio but not in orders gets added into the dataframe. wW must take those out as they cause an index error when indexing orders
                    portfolio_total = (Portfolio["Amount Holding"]*Portfolio["purchase price"]) #The total amount we currently own in our portfolio
                    orders["averaged purchase price"] = orders["purchase price"] #Creating the average purchase price column which will be copied into our portfolios purchase price column later on. We will change the values for (s_ex_short and b_ex_long) filters and leave the rest the same.
                    orders["averaged purchase price"][b_ex_long] = (orders_total[b_ex_long]+portfolio_total[Portfolio.index.isin(orders[b_ex_long].index)])/orders["Amount Holding After Trade"][b_ex_long] #Changing values for buying more of a long position using the calculation mentioned above.
                    orders["averaged purchase price"][s_ex_short] = ((orders_total[s_ex_short]*-1)+(portfolio_total[Portfolio.index.isin(orders[s_ex_short].index)]*-1))/(orders["Amount Holding After Trade"][s_ex_short]*-1) #Changing values for selling more of a short position using the calculation mentioned above except we multiplied the order total by negative 1 and portfolio total by negative to produce a positive sum.


                    orders["Cost"] += orders["Commissions"] #We are adding the Commissions to the costs because commissions are a positive value and positive values take away.


                    if marg_call_protect is True:


                        PortQMOrderQZ = orders.index.isin(Portfolio.index) & orders.index.isin(PortQMOrderQ[PortQMOrderQ == 0].index) #After Order Quantity that is equal to zero for sell orders
                        PortQMOrderQP = orders.index.isin(Portfolio.index) & orders.index.isin(PortQMOrderQ[PortQMOrderQ > 0].index)  #After Order Quantity that is above zero for sell orders *

                        PortQPOrderQZ = orders.index.isin(Portfolio.index) & orders.index.isin(PortQPOrderQ[PortQPOrderQ == 0].index)  #After Order Quantity that is equal to zero for buy orders
                        PortQPOrderQN = orders.index.isin(Portfolio.index) & orders.index.isin(PortQPOrderQ[PortQPOrderQ < 0].index) #After Order Quantity that is less than zero for buy orders *
                        PortQPOrderQP = orders.index.isin(Portfolio.index) & orders.index.isin(PortQPOrderQ[PortQPOrderQ > 0].index).copy() #After Order Quantity that is above zero for buy orders

                        #Various scenarios alter the effect of orders on the buying power

                        NewShort = (Sell & ~orders.index.isin(Portfolio.index)) | PortQMOrderQN & orders.index.isin(Portfolio[Portfolio["Amount Holding"] > 0].index) #new short position
                        LongShort = Sell & long & (orders.index.isin(Portfolio.index)) & PortQMOrderQN #existing long positions that is being changed to short
                        b_ex_short = Buy & short & PortQPOrderQN & (orders.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index))  # selling more of a existing short position
                        s_ex_short = Sell & short & PortQMOrderQN & (orders.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index))  # selling more of a existing short position
                        CShort = Buy & short & (orders.index.isin(Portfolio.index)) & PortQPOrderQZ #closing a existing short position



                        NewLong = Buy & ~orders.index.isin(Portfolio.index) # New Long Position
                        ShortLong = Buy & short & orders.index.isin(Portfolio.index) & PortQPOrderQP #Going from a short to a long position
                        s_ex_long = Sell & long & orders.index.isin(Portfolio.index) & PortQMOrderQP #Buying more of a existing long position
                        b_ex_long = Buy & long & orders.index.isin(Portfolio.index) & PortQPOrderQP #Buying more of a existing long position
                        CLong = Sell & long & orders.index.isin(Portfolio.index) & PortQMOrderQZ#Closing a Long Position


                        #How we are able to protect against a margin call is by calculating the value of our long positions at their stop losses. This is the lowest value the positions can be worth before they will be sold. We add that to our available cash. This is our minimum equity of our portfolio, not including margin. We then calculate the amount on margin, if all our short positions hit their stop losses (-1*(Amount Holding)*(Stop Loss)). We then take that value and multiply by our maintenance margin requirement. The product will the max_maintenance_margin or the most you will have on margin. As long as our previously calculated minimum equity remains above the max maintenance requirement. There will be no chance that we will face a margin call. This is because the value of our portfolio will never fall below minimum equity because of the stop losses on long positions and we know our max maintenance margin because of the stop losses on our short orders. Therefore as long as we ensure that new orders do not make the max maintenance margin greater than the minimum equity we will never face a margin call. min_ELVIM is equal to (minimum equity)-(max maintenance margin). This value represent the amount of money we have available to place margin orders without facing a margin call. As it is the diffrence between our min equity and max maintenance margin. We are able to keep max maintenance margin less than minimum equity by calculating how an order will effect our buying power using the orders stop loss. The max maintenance margin of a short position will be (Amount Holding)*(Stop Loss)*(Maintenance Margin Requirement)*-1. The product will be a positive value. The minimum equity a long order will add is (Amount Holding)*(Stop Loss). This will be a negative value. The reason margin positions are positive is because it is are borrowed and long positions are negative because they are not borrowed. Once we know how much our short orders will cost to maintain at their stop loss, and how much our long orders will be worth at their stop losses. We can now iterate through orders and calculate the effect on our min_ELVIM. For short orders we check if the Max Margin cost of the order is less than the min_ELVIM. If it is then that order is valid as it will not risk a margin call. For long orders we must take the diffrence between the equity we will gain from the position and its minimum equity. This diffrence represents the amount that will be taking away from our min_ELVIM. This is because when placing this order we will be taking money away from aval cash and adding to min_long_worth. Therefore the diffrence between them is what will be taken away from min_ELVIM. We then check if this diffrence is less than our min_ELVIM. If it is then we can buy the position as it will keep min_ELVIM above 0 and remove the risk a margin call. We do not need to calculate this for short positions because they do not take away cash rather add cash. At the end of each iteration we apply these changes to minimum equity, aval cash, and max_maintenance_margin and recalculate min_ELVIM so that the next order will be filtered using the new values.
                        #This works only if there is no slippage as we are guaranteeing that your order will execute at its stop limit which is unlikely in reality.
                        position_stoploss_worth = (Portfolio["Stop Loss"]*Portfolio["Amount Holding"]) #Getting the value of our positions at thier stop loss. We still need make the short orders positive.
                        max_maintenance_margin = -1*position_stoploss_worth[position_stoploss_worth < 0].sum()*MaintenceMarginRequirment #This is max maintenance margin requirement. If all our short orders hit their stop losses then our this would be the balance we need to maintain. A worst case scenario for all short orders
                        min_long_worth = position_stoploss_worth[position_stoploss_worth > 0].sum() #This is the worth of our long positions at their stop losses. A worst case scenario for our long positions
                        min_ELVIM = (min_long_worth+aval_cash-max_maintenance_margin) #This value represents the value of our portfolio if every position hit its stop loss. As long as this is above 0 we will never face a margin call.

                        #We must now calculate our Max Margin Costs for our orders. This will represent how much the maintenance margin will be for this position at its stop loss. There are 5 scenarios where a short position can fall under. Either you open a new short position, sell more of a existing short position, go from a long to a short position, buy to cover a short position, or close a short position. When opening a brand new short position and selling an existing short position the calculation is the same. Although the calculation for Long Short is different because we are getting the margin requirement for the newly shorted shares not the entire quantity of the order. Lastly when buying to cover a short position or closing a short position we do not add to our max maintenance requirement rather decreasing it as our short position is decreasing in size. Therefore it's value will be negative so when it is added to our max maintenance margin it will decrease it.

                        orders['Max Margin Cost'] = 0 #Setting the initial value to 0
                        orders['Max Margin Cost'][NewShort | s_ex_short] = -1*orders["Stop Loss"][NewShort | s_ex_short]*orders['Quantity'][NewShort | s_ex_short]*MaintenceMarginRequirment #The cost of a new short position or cost of selling more of a existing short position(Maintenance Margin Requirement)*(Current Price)*(Newly Shorted Stocks)).
                        orders['Max Margin Cost'][LongShort] = (orders['Amount Holding After Trade'][LongShort]*orders['Stop Loss'][LongShort]*MaintenceMarginRequirment*-1)#The max maintenance margin of a long position that is going to be shorted is the value of the shorted shares multiplied the maintenance requirement and multiplied by negative one to make the value positive as Amount Holding After Trade will be negative. (Amount Holding After Trade)*(Stop Loss)*(Maintenance Requirement)*-1.
                        orders['Max Margin Cost'][CShort | b_ex_short] = -1*orders['Quantity'][CShort | b_ex_short]*orders['Stop Loss'][CShort | b_ex_short]*MaintenceMarginRequirment #This is not a maintenance margin rather it is the amount our maintenance margin requirement would decrease by. Since buying to cover or closing a short position we are decreasing our short position and therefore our maintenance margin requirement will also decrease. Therefore this value will be negative so when we add it to our maintenance margin requirement our maintenance margin requirement will decrease.
                        orders['Max Margin Cost'][ShortLong] = -1*orders["Stop Loss"][ShortLong]*(orders["Quantity"][ShortLong]-orders["Amount Holding After Trade"][ShortLong])*MaintenceMarginRequirment

                        #For Long Positions we do not need to calculate the Max Margin Cost as there is no margin requirement for long positions bought in full. We do need the minimum equity as we will add it to our min_long_worth. Similarly to short orders. Long orders have 5 scenarios. You can open a new long position, buy more of a existing long position, go from a short to a long position, sell to cover a long position or close a long position. The minimum worth from a new long position or going from short to long will be the (Amount Holding After Trade)*(Stop Loss) or the value of all the shares at it's stop loss. The minimum worth of a buying more a of a long position is value of the newly purchased shares times the stop loss or (Stop Loss)*(Quantity). Lastly closing a long position or selling a existing long position will not add any equity rather decrease it since we are decreasing the position. Therefore its value will be negative. Closing or selling a long position will be equal to the Amount you are selling multiplied by it's stop loss and then multiplied by negative 1 to make the value negative.
                        orders["Min Equity"] = 0 #Default Min Equity
                        orders["Min Equity"][ShortLong | NewLong] = orders["Stop Loss"]*orders["Amount Holding After Trade"]  #The minimum equity added from a short long and new long is equal to the (Stop Loss)*(Amount Holding After Trade)
                        orders["Min Equity"][b_ex_long] = orders["Stop Loss"]*orders["Quantity"] #The minimum equity added from buying a more of a long position is equal to the (Stop Loss)*(Quantity)
                        orders['Min Equity'][CLong | s_ex_long] = orders['Quantity'][CLong | s_ex_long]*orders['Stop Loss'][CLong | s_ex_long] #The equity subtracted from selling or closing a long position is equal to the stop loss multiplied by the quantity then multiplied by negative one to make the value negative. (Stop Loss)*(Quantity)*-1
                        orders['Min Equity'][LongShort] = -1*orders["Stop Loss"][LongShort]*(orders["Amount Holding After Trade"][LongShort]-orders["Quantity"][LongShort])


                        #Now that we have the max margin and min equity calculated. We can filter our orders so we will never face a margin call. We will treat long and short orders separately as they effect our min_ELVIM differently. Short Orders will simply add to our max_maintence_margin. Although Long Orders will add to our costs in way that could negatively effect our buying power. Long orders effect our cost and our equity. The higher the cost the more it will take away from our aval_cash which will lower our min_ELVIM. The more equity it adds the more it will increase our min_ELVIM. The diffrence between the Cost and our Min Equity will be the amount it will effect our min_ELVIM. Therefore the as long as that diffrence is less than or equal to the min_ELVIM there will never be a margin call. Short Orders do effect our available cash like long orders except only in a positive way. When we sell the proceeds will be credited to the account. Therefore when selling a short you can only gain money increasing your min_ELVIM. Although Short orders will increase our maintenance margin which will negatively effect the min_ELVIM. Therefore we must make sure our max maintenance margin for that order is less than our min_ELVIM. These precautions prevent ourour min_ELVIM from going negative which would cause a margin call.
                        temp_aval_cash = aval_cash #We are creating a temporary variable to as we do not want to change aval_cash.
                        orders["Drop"] = True
                        for x in orders.index:
                            if (orders["Amount Holding After Trade"][x] < 0) or ((orders["Amount Holding After Trade"][x] == 0) & (orders["Action"][x] == "buy")): #This handles short orders. Short orders will have a Amount Holding After Trade as negative, or they could also be equal to 0 because the order could be covering a short. Therefore if a order is 0 and its a buy then it must be a short position because you can only get to zero by adding from a negative number, given you dont start a 0.
                                if (((orders["Max Margin Cost"][x]) <= min_ELVIM) & (orders["Action"][x] == "sell")) or ((orders["Cost"][x] < temp_aval_cash) & (orders["Action"][x] == "buy")): #If it is a sell order then we must check that the added maintence margin does not make our min_ELVIM negative. If the margin requirement incurred is less than our min_ELVIM then it will not be negative. For short positions that are being covered with orders we must simply have enough cash for the order to go through. This is because a covering a short order can never decrease your min_ELVIM. Although your available cash goes down your maintenance margin will go down atleast 50%. This is because the cost to purchase the shares is equal to the quantity*price and the maintenance margin is the same calculation except multiplied by the margin requirement(quantity*price*margin requirement). Therefore no matter what your min_ELVIM will only increase by covering a short order and does not need to be checked
                                    orders["Drop"][x] = False #We do not want to drop this order as it we will be placing it
                            else: #If it is a long position
                                if ((orders["Cost"][x]-orders["Min Equity"][x]+orders["Max Margin Cost"][x]) <= min_ELVIM) & (orders["Cost"][x] < temp_aval_cash): #As described above the orders Cost - (Min Equity) is the change that will effect our min_ELVIM for long orders. Therefore if that diffrence is less than our min_ELVIM then we will not face a margin call. We also must check if we have enough cash to purchase the buy order as borrowing on margin for long positions is not yet allowed
                                    orders["Drop"][x] = False #We do not want to drop this order as it we will be placing it
                            if ~orders["Drop"][x]:
                                max_maintenance_margin += orders["Max Margin Cost"][x] #We add the maintenance margin for the order to our total.
                                min_long_worth += orders["Min Equity"][x] #Adding the equity from gaining that position
                                temp_aval_cash -= orders["Cost"][x] #Subtracting the costs from our available cash
                            min_ELVIM = min_long_worth + temp_aval_cash - max_maintenance_margin #Recalcualting min_ELVIM to compare to our next order
                        orders = orders[orders["Drop"] == False] #Dropping the orders that would cause a margin call or we did not have the funds to place.



                    Sell = orders['Action'] == 'sell'  # Isolating all sell signals
                    Buy = orders['Action'] == 'buy'  # Isolating all Buy
                    short = orders.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index)  # Current short position
                    long = orders.index.isin(Portfolio[Portfolio["Amount Holding"] > 0].index)  # Current long position

                    # Adding Buy orders and Subtracting Sell orders from our Portfolio to get the quantity after the order is placed
                    PortQPOrderQ = orders[~orders.index.isin(Portfolio.index) & (orders["Action"] == "buy")]["Quantity"].copy() #Orders that are not in our Portfolio have a Quantity of zero, therefore their after order Quantity will be set to the Order Quantity
                    PortQPOrderQ = PortQPOrderQ.append((Portfolio[Portfolio.index.isin(orders[Buy].index)]["Amount Holding"] + orders[orders.index.isin(Portfolio.index)][Buy]['Quantity'])) #Portfolio Quantity + Order Quantity = After Order Quantity for Buy orders
                    PortQMOrderQ = orders[~orders.index.isin(Portfolio.index) & (orders["Action"] == "sell")]["Quantity"].copy() #Orders that are not in our Portfolio have a Quantity of zero, therefore thier after order Quantity will be set to the Order Quantity
                    PortQMOrderQ = PortQMOrderQ.append((Portfolio[Portfolio.index.isin(orders[Sell].index)]["Amount Holding"] + orders[orders.index.isin(Portfolio.index)][Sell]['Quantity'])) #Portfolio Quantity + -(Order Quantity(Sell orders are Negative so this is technically subtraction)) = After Order Quantity for Buy orders

                    PortQMOrderQN = orders.index.isin(Portfolio.index) & orders.index.isin(PortQMOrderQ[PortQMOrderQ < 0].index) #After Order Quantity that is less than zero for sell orders
                    PortQMOrderQZ = orders.index.isin(Portfolio.index) & orders.index.isin(PortQMOrderQ[PortQMOrderQ == 0].index) #After Order Quantity that is equal to zero for sell orders
                    PortQMOrderQP = orders.index.isin(Portfolio.index) & orders.index.isin(PortQMOrderQ[PortQMOrderQ > 0].index)  #After Order Quantity that is above zero for sell orders *

                    PortQPOrderQN = orders.index.isin(Portfolio.index) & orders.index.isin(PortQPOrderQ[PortQPOrderQ < 0].index) #After Order Quantity that is less than zero for buy orders *
                    PortQPOrderQZ = orders.index.isin(Portfolio.index) & orders.index.isin(PortQPOrderQ[PortQPOrderQ == 0].index)  #After Order Quantity that is equal to zero for buy orders
                    PortQPOrderQP = orders.index.isin(Portfolio.index) & orders.index.isin(PortQPOrderQ[PortQPOrderQ > 0].index).copy() #After Order Quantity that is above zero for buy orders


                    NewShort = (Sell & ~orders.index.isin(Portfolio.index)) | PortQMOrderQN & orders.index.isin(Portfolio[Portfolio["Amount Holding"] > 0].index) #new short position
                    LongShort = Sell & long & (orders.index.isin(Portfolio.index)) & PortQMOrderQN #existing long positions that is being changed to short
                    b_ex_short = Buy & short & PortQPOrderQN & (orders.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index))  # selling more of a existing short position
                    s_ex_short = Sell & short & PortQMOrderQN & (orders.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index))  # selling more of a existing short position
                    CShort = Buy & short & (orders.index.isin(Portfolio.index)) & PortQPOrderQZ #closing a existing short position

                    NewLong = Buy & ~orders.index.isin(Portfolio.index) # New Long Position
                    ShortLong = Buy & short & orders.index.isin(Portfolio.index) & PortQPOrderQP #Going from a short to a long position
                    s_ex_long = Sell & long & orders.index.isin(Portfolio.index) & PortQMOrderQP #Buying more of a existing long position
                    b_ex_long = Buy & long & orders.index.isin(Portfolio.index) & PortQPOrderQP #Buying more of a existing long position
                    CLong = Sell & long & orders.index.isin(Portfolio.index) & PortQMOrderQZ#Closing a Long Position



                    #Initial Margin defined by interactive brokers - TBC Yet to be tested
                    #https://www.interactivebrokers.com/en/index.php?f=26658&hm=us&ex=us&rgt=1&rsk=0&pm=1&rst=101004100808
                    #Here we will calculate our initial cost. It represent the inital cost needed to place the order. For a long position it would be the cost of purchasing the shares. For a short position it would the Initial MarginRequirement. Plus the commissions
                    orders['Initial Cost'] = orders["Cost"] #By default initial cost will be equal to costs and then changed for short positions to the Initial Margin Requirement.

                    #For interactive brokers there are 3 different tiers of for the Initial Margin Requirement. Greater than $16, less than $16 but greater than 5, and less than 5. The exact pricing is detailed in the link above.
                    #We need to calculate the Initial Margin for orders for short positions that are either selling more of a short position, creating a new short position or going from a long to a short position. The other three scenarios involving a short position, covering a short position, closing a short position, and going from a short to a long position. Do not require a Initial Margin Requirement as we are not increasing the amount short soled shares. The calculation for New Shorts and Long Shorts Initial Cost is the same. The calculation for a long short is different as instead of (Quantity), (Amount Holding After Trade) is used because the long shares of a long position that gets sold into a short do not need a margin requirement only the short shares of the order. EX:
                        #20 Shares
                        #30 Sell Order
                        #20 get sold as normal shares(long shares)
                        #10 get a margin maintenance requirement added to it(short shares)
                        #-10 Shares

                    short_margin_r1_1 = (NewShort & (orders['purchase price'] > 16.67)) | (s_ex_short & (orders['purchase price'] > 16.67)) #if Stock Value > $16.67 30% market value
                    short_margin_r2_1 = (NewShort & (orders['purchase price'] <= 16.67) & (orders['purchase price'] > 5)) | (s_ex_short & (orders['purchase price'] <= 16.67) & (orders['purchase price'] > 5)) # if Stock Value < $16.67 and > $5.00, $5 Per Share
                    short_margin_r3_1 = (NewShort & (orders['purchase price'] <= 5) & (orders['purchase price'] > 2.5) | (s_ex_short & (orders['purchase price'] <= 5) & (orders['purchase price'] > 2.5)))
                    short_margin_r4_1 = (NewShort & (orders['purchase price'] <= 2.5)) | (s_ex_short & (orders['purchase price'] <= 2.5)) #if Stock Value < $5.00 $2.50 per share

                    #The calculation for a new short position and selling more of a short position. It is the total price of shares multiplied by the quantity plus the initial margin requirement
                    orders['Initial Cost'][short_margin_r1_1] = -1*orders["purchase price"][short_margin_r1_1]*orders['Quantity'][short_margin_r1_1]*1.3 #if Stock Value > $16.67 30% market value
                    orders['Initial Cost'][short_margin_r2_1] = -1*orders["purchase price"][short_margin_r2_1]*orders['Quantity'][short_margin_r2_1]+(-1*orders['Quantity'][short_margin_r2_1]*5) # if Stock Value < $16.67 and > $5.00, $5 Per Share
                    orders['Initial Cost'][short_margin_r3_1] = -1*orders["purchase price"][short_margin_r3_1]*orders['Quantity'][short_margin_r3_1]*2 #if Stock Value < $5.00 100% market value
                    orders['Initial Cost'][short_margin_r4_1] = -(1*orders["purchase price"][short_margin_r4_1]*orders['Quantity'][short_margin_r4_1])+(-1*orders['Quantity'][short_margin_r4_1]*2.5)

                    short_margin_r1_2 = (LongShort & (orders['purchase price'] > 16.67))  #if Stock Value > $16.67 per share $5.00 per share
                    short_margin_r2_2 = (LongShort & (orders['purchase price'] <= 16.67) & (orders['purchase price'] > 5)) #if Stock Value <= $16.67 and > $5.00
                    short_margin_r3_2 = ((LongShort & (orders['purchase price'] <= 5)) & (orders['purchase price'] > 2.5))  #if Stock Value <= $5.00
                    short_margin_r4_2 = (LongShort & (orders['purchase price'] <= 2.5)) #if Stock Value <= $2.50, $2.5 Per Share

                    orders['Initial Cost'][short_margin_r1_2] = (-1*(orders['Amount Holding After Trade'][short_margin_r1_2]-orders['Quantity'][short_margin_r1_2])*orders['purchase price'][short_margin_r1_2])+(-1*orders['Amount Holding After Trade'][short_margin_r1_2]*orders['purchase price'][short_margin_r1_2]*1.3) #30% Market Value of Stock
                    orders['Initial Cost'][short_margin_r2_2] = (-1*(orders['Amount Holding After Trade'][short_margin_r2_2]-orders['Quantity'][short_margin_r2_2])*orders['purchase price'][short_margin_r2_2])+(-1*orders['Amount Holding After Trade'][short_margin_r2_2]*orders['purchase price'][short_margin_r2_2])+(-1*orders['Amount Holding After Trade'][short_margin_r2_2]*5) #$5.00 per share
                    orders['Initial Cost'][short_margin_r3_2] = (-1*(orders['Amount Holding After Trade'][short_margin_r3_2]-orders['Quantity'][short_margin_r3_2])*orders['purchase price'][short_margin_r3_2])+(-1*orders['Amount Holding After Trade'][short_margin_r3_2]*orders['purchase price'][short_margin_r3_2]+(-1*orders['Amount Holding After Trade'][short_margin_r3_2]*orders['purchase price'][short_margin_r3_2]*2)) #100 Market value
                    orders['Initial Cost'][short_margin_r4_2] = ((-1*(orders['Amount Holding After Trade'][short_margin_r4_2]-orders['Quantity'][short_margin_r4_2]))*orders['purchase price'][short_margin_r4_2])+(-1*orders['Amount Holding After Trade'][short_margin_r4_2]*orders['purchase price'][short_margin_r4_2])+(-1*orders['Amount Holding After Trade']*2.5) #2.5 Per Share


                    orders['Initial Cost'] += orders["Commissions"]


                    #Here we will be calculating our Maintenance Margin for each order. This will be the amount of money we will need to maintain to not face a margin call.
                    orders["Maintenance Margin"] = 0 #By default it is 0
                    orders["Maintenance Margin"][LongShort | NewShort] = orders["purchase price"][LongShort | NewShort]*orders["Amount Holding After Trade"][LongShort | NewShort]*MaintenceMarginRequirment #The calculation for a long position that gets sold into a short position and a New Short have the same calculation. Simply the product of the Purchase Price, Amount Holding After Trade, and the Maintenance Margin Requirement (Purchase Price)*(Amount Holding After Trade)*(Maintenance Margin Requirement).
                    orders["Maintenance Margin"][s_ex_short] = orders["purchase price"][s_ex_short]*orders["Quantity"][s_ex_short]*MaintenceMarginRequirment #The maintenance margin for a selling more a short position is the product of the Purchase Price, Quantity, and Maintenance Margin Requirement
                    orders["Maintenance Margin"][CShort | b_ex_short] = orders["purchase price"][CShort | b_ex_short]*orders["Quantity"][CShort | b_ex_short]*MaintenceMarginRequirment*-1 #Closing a short or buying to cover a short will decrease your Maintenance Margin. This is because you are decreasing your short shares and therefore the value of your position and inherently your maintenance margin requirement.Since these orders will decrease the maintenance margin it will a negative value so that when added to maintenance margin it subtract from it.
                    orders["Maintenance Margin"][ShortLong] = orders["purchase price"][ShortLong]*(orders["Quantity"][ShortLong]-orders["Amount Holding After Trade"][ShortLong])*MaintenceMarginRequirment


                    #Equity is used to see how much equity an order will give or take. Only long orders can give equity and there are 5 scenarios involving long orders. A new long position, going from a short to a long position, buying more of a long position, selling some of your long position, or closing your long position.
                    orders["equity"] = 0 #By default the equity will be 0
                    orders["equity"][ShortLong | NewLong] = orders["purchase price"]*orders["Amount Holding After Trade"] #The equity for New long positions and short positions that get become long positions is the product of the Purchase Price and Amount Holding After Trade.
                    orders["equity"][b_ex_long] = orders["purchase price"][b_ex_long]*orders["Quantity"][b_ex_long] #The equity for buying more a long position is the product of the Purchase Price, and the Quantity
                    orders["equity"][CLong | s_ex_long] = orders["purchase price"][CLong | s_ex_long]*orders["Quantity"][CLong | s_ex_long] #When closing or selling a long position your equity decreases instead of increasing. This is because we are decreasing the amount shares of that position and inherently lowering the value and equity of that position. Therefore the equity will be negative so when added to equity later it will subtract.
                    orders["equity"][LongShort] = -1*orders["purchase price"][LongShort]*(orders["Amount Holding After Trade"][LongShort]-orders["Quantity"][LongShort])


                    #There are some stocks that have bad data and therefore cause problems. We filter them out here so they are not ordered
                    problematic_stocks = [('NHTC','NASDAQ'),('USAU','NASDAQ'),('MBOT','NASDAQ'),('WCN','NYSE'),('PTC','NASDAQ')] #Stocks that casuse a problem
                    orders.drop(index=pandas.MultiIndex.from_tuples(list(set(problematic_stocks).intersection(orders.index.values)),names=["ticker","exchange"]),inplace=True) #Removing them if they are in orders



                    #Now we have all the necessary values to filter our orders to those that will be placed. Short orders and long orders will be handled differently. This is because short orders can be bought using margin and long positions are not yet allowed to be traded on margin. A short order's Initial Maintenance Margin Must be less than the value of our portfolio value(ELV)-Maintenance Margin(MM)=ELVIM. ELVIM represent the buying power left for short positions. The buying power left for long positions is simply the available cash. A short position that is being bought to cover will not be compared with ELVIM rather our avalaible cash, because buying on margin is not yet completed. If the order meets this criteria then for long and short orders we will add our previously calculated equity, maintenance margin for that order to our current equity, maintenance margin respectively. Set the drop column to False so it will be kept and added/modified to portfolio later.
                    orders["Drop"] = True #Set drop column to true by default
                    for x in orders.index: #Filtering orders that cannot be afforded.
                        if (orders["Amount Holding After Trade"][x] < 0) or ((orders["Amount Holding After Trade"][x] == 0) & (orders["Action"][x] == "buy")): #If it is a order that creates, closes, or modifies a short position
                            if ((orders['Initial Cost'][x] <= ELVIM) & (orders["Action"][x] == "sell")) or ((orders["Cost"][x] <= aval_cash) & (orders["Action"][x] == "buy")): #Making sure the Initial Cost is less than our ELVIM for sell orders. For buy orders the cost must be less than our available cash. This ensures we can afford it.
                                orders["Drop"][x] = False #Set the drop to false
                        else: #If the order creates, closes, or modifies a long position
                            if orders['Initial Cost'][x] <= aval_cash: #Is the Initial Cost Less than the available cash. This ensures we can afford it.
                                orders["Drop"][x] = False #Set the Drop to False
                        if ~orders["Drop"][x]:
                            equity += orders["equity"][x]  # Add the equity from the newly purchased shares to equity.
                            maintenance_margin += orders["Maintenance Margin"][x]  # Add the maintence margin for that order to our maintenance margin
                            aval_cash -= orders["Cost"][x]
                        ELVIM = equity+aval_cash+maintenance_margin #Re-calculate our ELVIM for the next order
                    orders.drop(index=orders[orders["Drop"] == True].index,columns=["Drop"],inplace=True) #Drop all orders that we could not afford


                    Transactions = Transactions.append(orders) #Adding orders to dataframe for post-backtest analysis


                    if len(orders) > 0: #If we have any orders after filtering them then do the final order processing
                        AllocationRemaining = (aval_cash / net_worth) #Re adjusting our Allocation Remaining

                        #Calculating Expires for positions. You add the amount of days to today using the datetime library to get the expiry date
                        orders['Expiry'] = None #Creating an expiry column
                        for x in orders.index[(orders['Holding Period'] != 'Infinite') & ~orders['Holding Period'].isna()]: #Only the orders whose expiry is not infinite will be calculated as the ones that are infinite do not need to be calculated
                            try:#This handles the exception that when you have a holding period longer than the dates in the
                                #backtest. If this exception is raised the expiry will be set to the last date available
                                orders['Expiry'][x] = Dates[Dates.index(Today)+orders['Holding Period'][x]] #Trying to set expiry
                            except IndexError: #If the date is not there
                                orders['Expiry'][x] = Dates[len(Dates)-1] #Set it to the last available



                        Portfolio = Portfolio.append(orders[~orders.index.isin(Portfolio.index)].drop(columns=["Amount Holding After Trade"]).copy()) #Adding the new positions that were bought to your portfolio
                        Portfolio["Amount Holding"][Portfolio.index.isin(orders.index)] = orders["Amount Holding After Trade"][orders.index.isin(Portfolio.index)] #Setting the Amount Holding your Portfolio to the one we calculated earlier in orders
                        Portfolio["purchase price"][Portfolio.index.isin(orders.index)] = orders["averaged purchase price"][orders.index.isin(Portfolio.index)] #The Purchase price is now set using the average purchase price calculation
    #                    orders = orders[orders["Quantity"] == 0]
    #                   orders = orders[orders["Quantity"] != 0]
    #                   modifying_orders = ~orders.index.isin(Portfolio.index)
    #                   modified_positions = Portfolio.index.isin(orders.index)
    #                   Portfolio["Commissions"][modified_positions] += orders["Commissions"][modifying_orders]




                    #Getting misc calculation that are useful
                    yesterdays_adj_close = Fields['adj_close'][Fields['adj_close'].index.get_level_values('dat') == datadate].reset_index('dat')['adj_close']#Yesterday's adj_close
                    Portfolio["Yesterday's Adjusted Close"] = yesterdays_adj_close[yesterdays_adj_close.index.isin(Portfolio.index)] #Adding Yesterdays adj_close to our portfolio
                    long = Portfolio["Amount Holding"] > 0
                    short = Portfolio["Amount Holding"] < 0
                    Portfolio["Position Worth"] = Portfolio["Yesterday's Adjusted Close"] * Portfolio["Amount Holding"]  # Calculating the worth of your portfolio based on Yesterday's Adjusted close
                    Portfolio["Position Worth"][short] = Portfolio["Position Worth"][short]*-1
                    Portfolio['Total Change'] = ((Portfolio["Yesterday's Adjusted Close"]-Portfolio['purchase price'])/Portfolio['purchase price']) #The total change of a position in our portfolio from the adjusted close two days ago
                    Portfolio["Total Change"][short] = ((Portfolio['purchase price'][short]-Portfolio["Yesterday's Adjusted Close"][short])/Portfolio['purchase price'][short]) #The total change of a
                    # Figuring out the max drawdown of a position
                    Portfolio['max drawdown'][Portfolio['max drawdown'].isna()] = 0
                    Portfolio['max drawdown'][(Portfolio['Total Change'] < Portfolio['max drawdown'])] = Portfolio['Total Change'][(Portfolio['Total Change'] < Portfolio['max drawdown'])]



                    for x in Signals.columns.values: #This updates the values in your Portfolio everyday. Although some of the values are static so they should not be updated
                        if x != "Stop Loss" and x != "Stop Limit" and x != "expiry":
                            Portfolio[x] = Signals[x][Signals.index.isin(Portfolio.index)] #Updating the portfolio values


            Portfolio = Portfolio[Portfolio["Amount Holding"] != 0]


            #Calculating the margin, equity, and using those to calculate net worth
            margin = (Portfolio["Amount Holding"][Portfolio["Amount Holding"] < 0]*OpenPrice[(OpenPrice.index.isin(Portfolio[Portfolio["Amount Holding"] < 0].index))]).sum()
            #Margin is the amount of money you have borrowed. If you short a share the amount of money you have borrowed is equal to the current price(OpenPrice) of that share times how much you own(Amount Holding Column in Portfolio). This value will be negative if you owe money, as all short orders will have a negative Amount Holding value in your Portfolio and when you multiply it by the Open Price which will always be positive, the result will always be a negative value.. If the share price goes up then the amount on margin does as well, and vice versa if it goes down. We will take the sum of (Amount of shares you own(Amount Holding Column in Portfolio) * the Open Price of the Shares) for all short orders to see how much you have on margin.
            maintenance_margin = margin * MaintenceMarginRequirment #This is our maintenance margin. It represents the total amount of you have on margin times the maintenance margin requirement. You can change the maintenance margin requirement by modifying the function parameter maintenance_margin_requirement. The worth of your long positions + your available cash should be greater than this or else you will face a margin call. Resulting in the end of your backtest
            equity = (Portfolio["Amount Holding"][Portfolio["Amount Holding"] > 0]*OpenPrice[(OpenPrice.index.isin(Portfolio[Portfolio["Amount Holding"] > 0].index))]).sum() #The equity we have is equal to the worth of your long positions. It is calculated by (Amount of Shares you Own(Amount Holding Column in Portfolio) * Open Price)
            net_worth = equity+aval_cash+margin#Finaly your calculated margin and equity with your avalaible cash(aval_cash) will be used to calculate your net worth. The sum of all of these will represent your net worth.
            ELVIM = equity+aval_cash+maintenance_margin
            if ELVIM < 0: #If you do not have the funds to meet your maintenance margin you will face a margin call and the backtest will be stopped.
                print("\n\n\n Margin Call \n\n\n")
                break #Ending Backtest
            # We do not subtract the margin because it will be a negative value inherently, as all short orders will have a negative Amount Holding value in your Portfolio and when you multiply it by the Open Price which will always be positive, the result will always be a negative value. Therefore adding a negative value(What we are doing) equates to subtracting a positive value.


            TodaysBalances = pandas.DataFrame({'Net Worth':[net_worth],'Exposure':[str((1-AllocationRemaining)*100)+'%'],'Date':[Today]}).set_index('Date')
            Daily_Balances = Daily_Balances.append(TodaysBalances)


            first = False
            #print('\nBought Today:\n',orders) #Printing the stocks you bought today
            #print(TempPipe)
            #print(Transactions)
            print('\n',Portfolio,'Portfolio with',len(Portfolio.index),'positions using %'+str(round((1-AllocationRemaining)*100,0)), 'of the portfolio and a Balance of $'+str(net_worth)+':\n') #printing your portfolio
            print('Total Time',datetime.now()-Start)#Printing the time it took to start
            if orders_len == 0:
                orders_len = 1
            print('Integ Check:',str(IntegCheckEndTime)+", "+str(IntegCheckEndTime/orders_len),'Signals:',SignalsTime)
            PrevDate = YearOverYearBacktestSummary.index.get_level_values('Date')[len(YearOverYearBacktestSummary.index.get_level_values('Date')) - 1]



            if datetime.strptime(Today,'%Y-%m-%d')-datetime.strptime(PrevDate,'%Y-%m-%d') == 365:
                MaxDrawDown = Daily_Balances[(Daily_Balances.index.get_level_values('Date') >= PrevDate) & (Daily_Balances.index.get_level_values('Date') <= Today)]['Net Worth'].sort_values().values[0]
                YearlyGains = str(round(((net_worth-YearOverYearBacktestSummary['Net Worth'][PrevDate])/YearOverYearBacktestSummary['Net Worth'][PrevDate])*100,2))+'%'
                HitRate = str(round((len(Transactions[(Transactions['Date Sold'] >= PrevDate) & (Transactions['Date Sold'] <= Today) & (Transactions['Gains'] > 0)])/len(Transactions))*100,2))+'%'
                YearSummary = pandas.DataFrame({'Date':[Today],'Net Worth':[net_worth],'Max DrawDown':[MaxDrawDown],'Yearly Gains':[YearlyGains],'Hit Rate':[HitRate]}).set_index('Date')
                YearOverYearBacktestSummary = YearOverYearBacktestSummary.append(YearSummary)



    except Exception as e:
        print(e)
        #notify('The '+BacktestTitle+'_'+Start_Date+'-'+End_Date+' backtest has failed')
        #Transactions.to_csv(BacktestTitle+'_'+Start_Date+'-'+End_Date+'_Transactions.csv')
        Daily_Balances.to_csv(BacktestTitle+'_'+Start_Date+'-'+End_Date+'_DailyBalances.csv')
        #Daily_Values.to_csv(BacktestTitle+'_'+Start_Date+'-'+End_Date+'_Daily_Values.csv')
        print('\nBackTest Failed on',Dates[len(Dates)-1],'with','$'+str(net_worth))
        print('\nTotal Gain:',str(((net_worth-SC)/SC)*100)+'%')
        raise e



    Transactions.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_Transactions.csv')
    Daily_Balances.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_DailyBalances.csv')
    YearOverYearBacktestSummary.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_Summary.csv')
    #Daily_Values.to_csv(BacktestTitle + '_' + Start_Date + '-' + End_Date + '_Daily_Values.csv')
    print('\nBackTest Completed on', Dates[len(Dates) - 1], 'with', '$' + str(net_worth))
    print('\nTotal Gain:', str(((net_worth - SC) / SC) * 100) + '%')
    notify('The '+BacktestTitle+'_'+Start_Date+'-'+End_Date+' backtest has finished')
