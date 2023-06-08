from source.Commons import notify
from source.Commons import upload_to_rds_table
import time
from urllib.error import URLError
from config.config import *
from source.Commons import ticker_list
#This is used to get data
import numpy
from datetime import datetime,timedelta
import pandas

Today = str(datetime.today().date())
def symbols_alpha(Exchange):
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
    warned = False
    for x in Symbols:
        #Try to get data but some errors are unpredictable which is why except statement is broad
        try:
            #Put into a while loop because sometimes we get an error that we are requesting to quickly and we need
            #to slow down. This is not fatal and the data for that ticker can still be retrieved we just need to
            #wait and try again
            while True:
                #This is the api call and we are inserting the symbol as x into the call
                try:
                    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol=' + x +'&apikey=' + alpha_apikey + '&datatype=csv&outputsize=full'
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
                Data = Data.rename(columns={'timestamp':'dat'})
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
                if Errors/len(Symbols) > .5:
                    if not warned:
                        #notify("Data pull for "+Exchange+"errors are exceeding 50%")
                        warned = True
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
    return SymDat
    #Function to get data from NYSE and Nasdaq
def NYSEandNASDAQData():
    try:
        NASDAQ = symbols_alpha('NASDAQ')
        #Get Data for AMEX by specifying the exchange when call the function
        Overall = NASDAQ
        #AMEX = get_data_a_v.symbols_alpha(self,'AMEX')
        #Overall = Overall.appeen(AMEX)
        #Get Data for NYSE by specifying the exchange when call the function
        NYSE = symbols_alpha('NYSE')
        Overall = Overall.append(NYSE)
        # Get Data for NASDAQ by specifying the exchange when call the function
        #Create a Dataframe that will have both exchange's data by setting it equal to NYSE
        #Append AMEX Data to the dataframe created above
        #Return the Data with all the Data from NYSE and NASDAQ
        return Overall
    except Exception as e:
        return Overall



#now = datetime.now()
# Get the time we want to re-run the program at
#runAt = (datetime.now() + timedelta(days=3)).replace(hour=24, minute=00, second=0, microsecond=0)
# Get the difference between the time we want to sleep at and the time right now
#delta = (runAt - now).total_seconds()
#print('\n'+ Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n')
# Sleep for the difference between the time now and the time we want to run at again where it will resume and go to
# Start of the while loop again
#sleep(delta)

try:
    todaydata = NYSEandNASDAQData()
except Exception as e:
    print("Could not download data from alpha vantage on"+Today)
    #notify("Could not download data from alpha vantage on"+Today,'#Quantheus')
    todaydata.to_csv('todaydata.csv')
    raise e
todaydata.to_csv('todaydata.csv')
upload_to_rds_table(todaydata,'alpha_vantage',row_by_row=True,save_errors=True)