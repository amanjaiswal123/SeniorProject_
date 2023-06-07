import psycopg2

from source.Commons import notify
from sqlalchemy import create_engine
from psycopg2 import connect
from datetime import datetime
import pandas
import numpy
import time
from urllib.error import URLError
from datetime import timedelta
from config.config import *
from source.Commons import ticker_list, NearestTradingDay


def tiingo_download(exchange, start_date=None, end_date=None,days=None):
    if start_date == None:
        start_date = str(datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days))[0:10]
    # Downloads a list of symbols for NYSE and NASDAQ you must specify which exchange you want
    Symbols  = ticker_list(exchange.lower())['ticker'].values
    #Define count so we can print status updates every 100 calls to the api
    count = 1
    #Keeps Track of total amount of errors
    Errors = 0
    #Final Dataframe in which all data will be returned
    SymDat = pandas.DataFrame()
    #Start Time
    start = datetime.now()
    #List of avg time for 100 calls that will be used to give an estimate to how long it will be until the
    #program is finished retrieving the data
    calltimes = []
    print('\nRetrieving Data for', exchange, '\n')
    #Works by iterating through the list of symbols that was retrieved above and inserting that symbol into the
    #api call
    callstart = datetime.now()
    req__per_second_limit = (3600/20000)*1.05
    for x in Symbols:
        #Try to get data but some errors are unpredictable which is why except statement is broad
        try:
            #Put into a while loop because sometimes we get an error that we are requesting to quickly and we need
            #to slow down. This is not fatal and the data for that ticker can still be retrieved we just need to
            #wait and try again
            trys = 0
            while True:
                #This is the api call and we are inserting the symbol as x into the call
                try:
                    trys += 1
                    url = "https://api.tiingo.com/tiingo/daily/"+x+"/prices?startDate="+str(start_date)+'&'+str(end_date)+"&format=csv&resampleFreq=monthly&token="+tiingo_apikey
                    if datetime.now() - callstart < timedelta(seconds=req__per_second_limit):
                        Delta = ((callstart+timedelta(seconds=req__per_second_limit))-datetime.now()).total_seconds()
                        time.sleep(Delta)
                    callstart = datetime.now()
                    Data = pandas.read_csv(url)
                    break
                except URLError:
                    #If not connectd to internet retry every 5 seconds
                    time.sleep(5)
                    if trys > 10:
                        raise URLError
            #Sometimes the ticker is not found by alpha vantage's api because it is named differently on their api
            #unfortunetly there is no way to check this so we cannot get data for that symbol. This prevalent mostly
            #in NYSE as they use ^ in the tickers and the equivelent on AlphaVantage is unknown
            if len(Data) != 0:
                #If this error and the ones above did not happen we have successful retrieved that data for that ticker
                #Creating dataframe with data
                trigcount = 0
                #Creating a column that has the exchange of the ticker. The exchange is defined by the
                #user when he callsThis function
                Data['exchange'] = exchange
                #Creating the column with the ticker this is defined x which is defined in the for loop
                Data['ticker'] = x

                #Finally once all the checks are passed and the neccasry formating of the dataframe is done we add it
                #to the final dataframe symdat that at the end of the loop will contain all the values for that ticker
                #minus the ones that were lost due to errors
                SymDat = SymDat.append(Data)
                print("Received "+x)
            else:
                #If the ticker was not found in Alpha Vantage's API add 1 to errors
                Errors += 1
                #Print that it could not find the data and the current error rate
                print('Could not find Data for',x,'Current Error Rate is',Errors/len(Symbols))
        except Exception as e:
            #In case some other error happens while requesting the data
            #Add 1 to the errors
            Errors += 1
            #Print that some error happened and then print the current error rate
            print('Error Retrieving Data for',x,'Current Error Rate is',Errors/len(Symbols))
        #Add 1 to the couunt of calls
        count += 1
        #Every 100 calls print the amount of calls and estimated time remaining
        if count%100 == 0:
            #Get the amount of calls left by subtracting the amount of calls - the total amount of calls that were
            #placed. The total amount of calls will always be the amount of tickers in the exchange.
            SymbolsRem = len(Symbols)-count
            #Get the average time per call by difference between the time right now and the time at the start of the
            #100 calls and divide that delta by 100. The Start time is defined at the begetting and is reset
            #below every 100 calls
            calltime = ((datetime.now()-start).seconds)/100
            #Add that average time to a list defined at the beginning of the program. This is done so when we give
            #the user a estimated time remaining an anomaly will not effect it that much because we are not using
            #the time for the past 100 calls rather the average time for 1 call for all the 100 calls called so far.
            calltimes.append(calltime)
            #Get a average of the time for 1 call for all the past 100 calls to Alpha Vantage's API.
            avgcalltime = numpy.mean(calltimes)
            #Print a update with the the average time for 100 calls and the amount of caalls remaining(SymbolsRem)
            #and then the average minutes remaining. The average minutes remaining is defined by the total amount of
            #calls left times the average time for call for all the past calls divided by 60 so we can get estimated
            #minutes
            print('\nAverage time per call is',avgcalltime,'seconds with',SymbolsRem,'calls left. Expect',((SymbolsRem*(avgcalltime))/60),'minutes till completion\n')
            #Reseting start every 100 calls
            start = datetime.now()
    if start_date != None and end_date != None:
        SymDat = SymDat[(SymDat['date'] >= start_date) & (SymDat['date'] <= str(end_date))]
    print('\nRetrieved Data for', exchange, '\n')
    # Renaming adjusted_close to adj_close to keep data generic with other sources
    SymDat = SymDat.rename(columns={'adjClose': 'adj_close','adjHigh': 'adj_high','adjLow': 'adj_low','adjOpen': 'adj_open','adjVolume': 'adj_volume','div_cash': 'dividend_amount','splitFactor': 'split-coefficient'})
    #Return the data that was retrieved
    return SymDat
def tiingo_nyse_nasdaq_download(start_date=None, end_date=None,days=None):
    # Function to get data from NYSE and Nasdaq
    #Get Data for AMEX by specifying the exchange when call the function
   # AMEX = GetData.symbols_alpha(self,'AMEX')
    #Get Data for NYSE by specifying the exchange when call the function
    NYSE = tiingo_download('NYSE', start_date, end_date, days)
    # Get Data for NASDAQ by specifying the exchange when call the function
    NASDAQ = tiingo_download('NASDAQ', start_date, end_date, days)
    #Create a Dataframe that will have both exchange's data by setting it equal to NYSE
    Overall = NYSE
    #Append NASDAQ Data to the dataframe created above
    Overall = Overall.append(NASDAQ)
    #Append AMEX Data to the dataframe created above
    #Overall = Overall.append(AMEX)
    #Return the Data with all the Data from NYSE and NASDAQ
    return Overall
def alphavantage(Exchange,start_date=None, end_date=None,days=None):
    if start_date == None:
        start_date = str(datetime.strptime(end_date, '%Y-%m-%d') - timedelta(days=days))[0:10]
    Symbols = ticker_list(Exchange.lower())['ticker'].values
    #Define count so we can print status updates every 100 calls to the api
    count = 1
    #Keeps Track of total amount of errors
    Errors = 0
    #Final Dataframe in which all data will be returned
    SymDat = pandas.DataFrame()
    #Start Time
    start = datetime.now()
    #List of avg time for 100 calls that will be used to give an estimate to how long it will be until the
    #program is finished retrieving the data
    calltimes = []
    print('\nRetrieving Data for', Exchange,'\n')
    #Works by iterating through the list of symbols that was retrieved above and inserting that symbol into the
    #api call
    callstart = datetime.now()
    call_limiter = (60/120)*1.05
    for x in Symbols:
        #Try to get data but some errors are unpredictable which is why except statement is broad
        try:
            #Put into a while loop because sometimes we get an error that we are requesting to quickly and we need
            #to slow down. This is not fatal and the data for that ticker can still be retrieved we just need to
            #wait and try again
            while True:
                #This is the api call and we are inserting the symbol as x into the call
                try:
                    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + x +'&apikey=' + alpha_apikey + '&datatype=csv'
                    if datetime.now() - callstart < timedelta(seconds=call_limiter):
                        Delta = ((callstart+timedelta(seconds=call_limiter))-datetime.now()).total_seconds()
                        time.sleep(Delta)
                    callstart = datetime.now()
                    Data = pandas.read_csv(url)
                except URLError:
                    #If not connectd to internet retry every 5 seconds
                    time.sleep(5)
                #Sleep 1 second to space out calls and avoid error where we are requesting to quickly                    #This is the error of calling to quickly when we get this we wait 2 seconds and call it again
                if Data.values[0][0] == '    "Information": "Please consider optimizing your API call frequency."':
                    #Waiting for 2 seconds
                    time.sleep(2)
                elif Data.values[0][0] == '    "Information": "Thank you for using Alpha Vantage! Please visit https://www.alphavantage.co/premium/ if you would like to have a higher API call volume."':
                    time.sleep(60)
                else:
                    #If the we did not get any error break and go onto the next steps
                    break
            #Sometimes the ticker is not found by alpha vantage's api because it is named differently on their api
            #unfortunetly there is no way to check this so we cannot get data for that symbol. This prevalent mostly
            #in NYSE as they use ^ in the tickers and the equivelent on AlphaVantage is unknown
            if Data.values[0][0] != '    "Error Message": "Invalid API call. Please retry or visit the documentation (https://www.alphavantage.co/documentation/) for TIME_SERIES_DAILY_ADJUSTED."':
                #If this error and the ones above did not happen we have successful retrieved that data for that ticker
                #Creating dataframe with data
                trigcount = 0
                #Creating a column that has the exchange of the ticker. The exchange is defined by the
                #user when he callsThis function
                Data['exchange'] = Exchange
                #Creating the column with the ticker this is defined x which is defined in the for loop
                Data['ticker'] = x
                #Renaming timestamp column to dat to keep the dat genric with other sources
                Data = Data.rename(columns={'timestamp':'date'})
                #Renaming adjusted_close to adj_close to keep data generic with other sources
                Data = Data.rename(columns={'adjusted_close':'adj_close'})
                #Finally once all the checks are passed and the neccasry formating of the dataframe is done we add it
                #to the final dataframe symdat that at the end of the loop will contain all the values for that ticker
                #minus the ones that were lost due to errors
                SymDat = SymDat.append(Data)
                print("Received "+x)
            else:
                #If the ticker was not found in Alpha Vantage's API add 1 to errors
                Errors += 1
                #Print that it could not find the data and the current error rate
                print('Could not find Data for',x,'Current Error Rate is',Errors/len(Symbols))
        except Exception as e:
            #In case some other error happens while requesting the data
            #Add 1 to the errors
            Errors += 1
            #Print that some error happened and then print the current error rate
            print('Error Retrieving Data for',x,'Current Error Rate is',Errors/len(Symbols))
        #Add 1 to the couunt of calls
        count += 1
        #Every 100 calls print the amount of calls and estimated time remaining
        if count%100 == 0:
            #Get the amount of calls left by subtracting the amount of calls - the total amount of calls that were
            #placed. The total amount of calls will always be the amount of tickers in the exchange.
            SymbolsRem = len(Symbols)-count
            #Get the average time per call by difference between the time right now and the time at the start of the
            #100 calls and divide that delta by 100. The Start time is defined at the begetting and is reset
            #below every 100 calls
            calltime = ((datetime.now()-start).seconds)/100
            #Add that average time to a list defined at the beginning of the program. This is done so when we give
            #the user a estimated time remaining an anomaly will not effect it that much because we are not using
            #the time for the past 100 calls rather the average time for 1 call for all the 100 calls called so far.
            calltimes.append(calltime)
            #Get a average of the time for 1 call for all the past 100 calls to Alpha Vantage's API.
            avgcalltime = numpy.mean(calltimes)
            #Print a update with the the average time for 100 calls and the amount of caalls remaining(SymbolsRem)
            #and then the average minutes remaining. The average minutes remaining is defined by the total amount of
            #calls left times the average time for call for all the past calls divided by 60 so we can get estimated
            #minutes
            print('\nAverage time per call is',avgcalltime,'seconds with',SymbolsRem,'calls left. Expect',((SymbolsRem*(avgcalltime))/60),'minutes till completion\n')
            #Reseting start every 100 calls
            start = datetime.now()
    print('\nRetrieved Data for',Exchange,'\n')
    #Return the data that was retrieved
    if start_date != None and end_date != None:
        SymDat = SymDat[(SymDat['date'] >= start_date) & (SymDat['date'] <= str(end_date))]
    return SymDat
    #Function to get data from NYSE and Nasdaq
def alphavantage_nyse_nasdaq_download(start_date=None, end_date=None,days=None):
    #Get Data for AMEX by specifying the exchange when call the function
    # AMEX = GetData.symbols_alpha(self,'AMEX')
    #Get Data for NYSE by specifying the exchange when call the function
    NYSE = alphavantage('NYSE',start_date,end_date,days)
    # Get Data for NASDAQ by specifying the exchange when call the function
    NASDAQ = alphavantage('NASDAQ',start_date,end_date,days)
    #Create a Dataframe that will have both exchange's data by setting it equal to NYSE
    Overall = NYSE
    #Append NASDAQ Data to the dataframe created above
    Overall = Overall.append(NASDAQ,start_date,end_date,days)
    #Append AMEX Data to the dataframe created above
    #Overall = Overall.append(AMEX)
    #Return the Data with all the Data from NYSE and NASDAQ
    return Overall
def data_scrub(data,index,dbname=qtheus_rds['dbname'],user=qtheus_rds['user'],host=qtheus_rds['host'],password=qtheus_rds['password'],schema='public'):
    try:
        conn = create_engine('postgresql+psycopg2://' + user + ':' + password + '@' + host + '/' +dbname)  # Connection to upload to database
    except Exception as e:  # catch exception and notify
        print("Connection Error: Could not connect sql alchemy to Database to clean data")
        notify("Connection Error: Could not connect sql alchemy to Database to clean data")
        raise e
    try:
        p_conn = connect(dbname=qtheus_rds['dbname'], user=qtheus_rds['user'], host=qtheus_rds['host'],password=qtheus_rds['password'])  # Connection to get table columns as they must match
    except Exception as e:
        print("Connection Error: Could not connect psycopg2 to Database to clean data")
        notify("Connection Error: Could not connect psycopg2 to Database to clean data")
        raise e

    # Get all tables in the ticker_list schema. It will gather all the data and scrub it into one list
    db_cursor = p_conn.cursor()

    # Creating query to get all table names in schema
    s = ""
    s += "SELECT"
    s += " table_schema"
    s += ", table_name"
    s += " FROM information_schema.tables"
    s += " WHERE"
    s += " ("
    s += " table_schema = '" + 'public' + "'"
    s += " )"
    s += " ORDER BY table_schema, table_name;"

    # get the table names
    db_cursor.execute(s)
    sources = db_cursor.fetchall()  # table names are stored in tuple with the as (schema, table)

    query = f"SELECT DATE FROM data ORDER BY date LIMIT 1"  # Construct query to get most recent date in data
    mrd = pandas.read_sql(query, conn)  # Get most recent date

    # (This might get rid of columns in subsequently added rows)

    first_data_source = True  # the first data source will create the df the subsequent ones will be added to the first one
    for x in sources:
        if x[1] != 'tickers':  # We do not want the data from the ticker table because that is the overall table and we will compare that when we are uploading to rds
            query = f"SELECT * FROM {x[1]} WHERE date > {mrd}"  # Construct query to get old data from database and clean it against the new data
            dbd = pandas.read_sql(query, conn).set_index(['ticker', 'exchange','date'])  # Get data from table
            if first_data_source:  # If it is the first data source create the overall_data dataframe
                overall_data = dbd
                first_data_source = False  # Now all subsequently downloaded data will be contacted to the first one
            else:
                overall_data = pandas.concat([overall_data, dbd])  # Concat to subsequent data to overall_data

    #What to clean
    #outliers
    for col in overall_data.columns:
        overall_data[col+" sources"] = overall_data.groupby(index).isna()
        overall_data[col+" standard deviation"] = 7
    #na
    #invalid types
    #How to clean
    #Majority - Compare
    #Outliers
        #Groupby ticker,exchange,dat then get median and find pct change of that from original and filter for x:
        #if all prices vary greatly create a na value instead of a price
    #Pre-Calculate # of days until next na value for each row

def get_data_rds(Field,Start_Day=None,End_Day=NearestTradingDay(str(datetime.now().date())),Days=None):
    try:
        conn = psycopg2.connect(dbname='postgres', user=qtheus_rds['user'], host=qtheus_rds['host'],password=qtheus_rds['password'])
    except:
        raise Exception("Unable to connect to the database")
    if Start_Day == None:
        Start_Day = str(datetime.strptime(End_Day, '%Y-%m-%d') - timedelta(days=Days))[0:10]

    Data = pandas.read_sql("""SELECT * FROM alpha_vantage WHERE date >= %s and date <= %s""", conn,params=(Start_Day, End_Day))
    print('Retrieved '+str(Field)+' Data')
    Data["date"] = Data["date"].astype(str)
    Data = Data.rename(columns={'date': 'dat'})
    Data = Data.sort_values('dat')
    Data.set_index(['ticker', 'exchange', 'dat'], inplace=True)
    #Data = clean_data(Data,Field)
    return Data
