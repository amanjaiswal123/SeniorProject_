from sqlalchemy import create_engine
import psycopg2
from datetime import timedelta
from datetime import datetime
from dateutil.easter import *
from calendar import day_name
import pandas
import numpy
from slack_sdk import WebClient
from warnings import warn
from config.config import logpath
from config.config import qtheus_rds
from config.config import slack_token
import warnings
pandas.options.mode.chained_assignment = None


#This is a file which will be used to import common functions and variables that do not need thier own files
Today = str(datetime.today().date())
def GetTradingDays(StartDate=None,EndDate=None):
    if StartDate == None or type(StartDate) != str:
        if type(StartDate) != str:
            print('Start Date must be a string please re-enter a start date\n')
        while True:
            StartDate = input('Enter a start date in the following format YYYY-MM-DD: ')
            print()
            Startyear,month,day = StartDate.split('-')
            try:
                datetime(int(Startyear), int(month), int(day))
                print()
                break
            except ValueError:
                print('The start date you entered, '+StartDate+', is not a valid date please re-enter a valid start date\n')
    Startyear, month, day = StartDate.split('-')
    if EndDate == None or type(EndDate) != str:
        if type(EndDate) != str:
            print('End date must be a string please re-enter a end date\n')
        while True:
            EndDate = input('Enter a end date in the following format YYYY-MM-DD: ')
            print()
            try:
                datetime(int(Endyear), int(month), int(day))
                if datetime.strptime(EndDate,'%Y-%m-%d') >= datetime.strptime(StartDate,'%Y-%m-%d'):
                    print()
                    break
                else:
                    print('Your End Date, '+EndDate+', is before your start date, '+StartDate+', please enter a end date that is after your start date\n')
            except ValueError:
                print('The end date, '+EndDate+', you entered is not a valid date please re-enter a valid start date\n')
    Endyear, month, day = EndDate.split('-')
    CurrentDate = StartDate
    Dates = []
    Dates.append(StartDate)
    while datetime.strptime(CurrentDate,'%Y-%m-%d') < datetime.strptime(EndDate,'%Y-%m-%d'):
        CurrentDate = str(datetime.strptime(CurrentDate,'%Y-%m-%d')+timedelta(days=1))[0:10]
        Dates.append(CurrentDate)
    CurrentYear = Startyear
    while datetime.strptime(CurrentYear,'%Y') <= datetime.strptime(Endyear,'%Y'):
        NotTradingDays = ['2012-10-29','2012-10-30','2001-09-11','2001-09-12','2001-09-13','2001-09-14','2004-06-11','2007-01-02','2018-12-05']
        NewYears = CurrentYear+'-01-01'
        NotTradingDays.append(NewYears)
        Date = datetime.strptime(NewYears,'%Y-%m-%d')
        while True:
            if day_name[Date.weekday()] == 'Monday':
                MartinLuterKingsDay = str(Date+timedelta(days=14))[0:10]
                break
            Date = Date + timedelta(days=1)
        NotTradingDays.append(MartinLuterKingsDay)
        Mondays = 0
        Date = datetime.strptime(CurrentYear+'-02-01', '%Y-%m-%d')
        while True:
            if day_name[Date.weekday()] == 'Monday':
                PresidentsDay = str(Date + timedelta(days=14))[0:10]
                break
            Date = Date + timedelta(days=1)
        NotTradingDays.append(PresidentsDay)
        GoodFriday = str(easter(int(CurrentYear))-timedelta(days=2))[0:10]
        NotTradingDays.append(GoodFriday)
        MondayDates = []
        Date = datetime.strptime(CurrentYear+'-05-01', '%Y-%m-%d')
        while Date.month == 5:
            if day_name[Date.weekday()] == 'Monday':
                MondayDates.append(Date)
            Date = Date + timedelta(days=1)
        MemorialDay = str(sorted(MondayDates)[len(MondayDates)-1])[0:10]
        NotTradingDays.append(MemorialDay)
        IndependenceDay = CurrentYear+'-07-04'
        NotTradingDays.append(IndependenceDay)
        Date = datetime.strptime(CurrentYear+'-09-01', '%Y-%m-%d')
        while True:
            if day_name[Date.weekday()] == 'Monday':
                LaborDay = str(Date)[0:10]
                break
            Date = Date + timedelta(days=1)
        NotTradingDays.append(LaborDay)
        Date = datetime.strptime(CurrentYear + '-11-01', '%Y-%m-%d')
        while True:
            if day_name[Date.weekday()] == 'Thursday':
                Thanksgiving = str(Date+timedelta(days=21))[0:10]
                break
            Date = Date + timedelta(days=1)
        NotTradingDays.append(Thanksgiving)
        Christmas = CurrentYear+'-12-25'
        NotTradingDays.append(Christmas)
        temp = []
        for x in NotTradingDays:
            daycdatetime = datetime.strptime(x, '%Y-%m-%d')
            dayoftheweek = day_name[daycdatetime.weekday()]
            if dayoftheweek == 'Saturday':
                NotTradingDays.remove(x)
                daycdatetime = daycdatetime-timedelta(days=1)
                if datetime.strptime(x, '%Y-%m-%d') == datetime(year=int(CurrentYear),month=1,day=1) and daycdatetime.year != int(CurrentYear) and daycdatetime >= datetime.strptime(StartDate, '%Y-%m-%d') and daycdatetime <= datetime.strptime(EndDate, '%Y-%m-%d') is False or daycdatetime.year == int(CurrentYear):
                    daycdatetime = str(daycdatetime)[0:10]
                    temp.append(daycdatetime)
            elif dayoftheweek == 'Sunday':
                NotTradingDays.remove(x)
                daycdatetime = str(daycdatetime+timedelta(days=1))[0:10]
                temp.append(daycdatetime)
        for x in temp:
            NotTradingDays.append(x)
        Date = datetime.strptime(NewYears, '%Y-%m-%d')
        Weekends = []
        while str(Date.year) == CurrentYear:
            if day_name[Date.weekday()] == 'Sunday' or day_name[Date.weekday()] == 'Saturday':
                NotTradingDays.append(str(Date)[0:10])
                Weekends.append(str(Date)[0:10])
            Date = Date + timedelta(days=1)
        for x in NotTradingDays:
            if x in Dates:
                Dates.remove(x)
        CurrentYear = str(datetime.strptime(NewYears,'%Y-%m-%d')+timedelta(days=366))[0:4]
    return Dates
TradingDays = GetTradingDays('1998-01-01','2024-12-31')
TradingDays = list(map(str, TradingDays))
#This will return the closets previous trading day/ Mostly used to index Trading Days when not on a trading day
def NearestTradingDay(Day):
    #By defualt the closest trading day will be The day defined by the user
    ClosestTradingDay = Day
    #If the day is not Trading Day keep subtracting 1 day from the day until it is a trading day and that day is the
    #closests trading day. It knows if a day is a trading day by seeing if that date is in the list defined above
    if Day not in TradingDays:
        #Keep count of iterations so we can keep subtracting 1 more each time the day is not a trading day
        count = 0
        #This while loop will keep iterating until the day is a trading day. the program knows it is a trading day
        #by checking if the day is in the pre-defined list of trading days above
        while ClosestTradingDay not in TradingDays:
            #Add 1 more to the amount of days being subtracted from the day defined by the user
            count += 1
            #Subtract the desired amount of days
            ClosestTradingDay = str((datetime.strptime(Day,'%Y-%m-%d') - timedelta(days=count)).date())
    #Once the loop is satisfied by the day being a Trading Day it will return that day which will be the closest previous trading day to that date
    return ClosestTradingDay
def _datadate(Today=str(datetime.now().date())):
    datadate = Today
    if Today in TradingDays:
        datadate = TradingDays[TradingDays.index(Today)-1]
    else:
        datadate = NearestTradingDay(Today)
    return datadate
def datadayscheck(Fields,Trading_Days=None,Days=None,Start_Date=None,End_Date=_datadate(Today),loose=False):
    #This function will check if the data meets your requirements you defined. You must pass a
    #dataframe or series as Fields where the index is ticker,dat,exchange and you must define an amount of Trading_Days,
    #an amount of days or a Start_Date. Optionally you can define End_Date by default End_Date is set as the Closest Trading
    #Day.

    #You can check the Data in the following ways:
        #Check if the Data has an exactly an amount of Days worth of Data or at least that much days worth of Data:
            #To check if the Data has exactly the amount Days you asked for you must define Trading_Days to the amount
            #of days you want
            #To Check if your data has at least that amount of days do the same as above but set loose to True
        #Check if you have data between two dates
            #You can set two dates to check between. The Start_Date will be the start of the data and the End_Date will
            #the end of the data
            #You can check if your data has atleaest the dates in between two dates by defining loose to True
        #You can also only define an End_Date and define an amount of days you want to go back from that date
        #and a Start_Date will automatically be calculated
    #Checking whether the Data is in a Dataframe or Series
    if isinstance(Fields,pandas.DataFrame) is False and isinstance(Fields,pandas.Series) is False:
        raise Exception('Fields must be either a DataFrame or a Series')
    #Checking if you defined one atleast one of the required arguements
    if Days == None and Start_Date == None and Trading_Days == None:
        raise Exception('You must specify a Start Date, amount of Days, or amount of trading days')
    #Checking if you defined arguments that are incompatible
    if Days != None and Start_Date != None or Days != None and Trading_Days != None or Start_Date != None and Trading_Days != None:
        raise Exception('You must specify only one of the following Start_Date,Trading_Days or Days')
    # Length check is always true unless it fails
    LengthCheck = True
    # Extracting the amount of days worth of data and Dates is different for a series and a dataframe. This where that
    # Exception is handled
    # If it is a Dataframe
    if isinstance(Fields, pandas.DataFrame):
        # Getting the Dates of the Data
        Dates = list(Fields[Fields.columns.values[0]].dropna()[Fields.index.get_level_values('ticker').values[0]].index.get_level_values('dat').values)
    else:
        #Getting the Dates of the Data
        if len(Fields) > 0:
            Dates = list(Fields.dropna()[Fields.index.get_level_values('ticker').values[0]].index.get_level_values('dat').values)
        else:
            Dates = []
    # Getting total amount of days worth of data
    length = len(Dates)
    #If you did not define a start date this while automatically calculate it and also calculates a length check if
    #defined Trading Days
    if Start_Date == None:
        #If you defined Trading Days
        if TradingDays != None:
            #Getting Start Date x amount of trading days ago
            Start_Date = TradingDays[TradingDays.index(End_Date) - Trading_Days+1]
            # Checking whether to check loosely or strictly
            if length != Trading_Days and loose == False:
                # If the amount of days in dataframe does not equal the amount of days needed check will be returned as False
                LengthCheck = False
            if length < Trading_Days and loose == True:
                LengthCheck = False
        #If you defined Days
        else:
            #Calculating Start Date
            Start_Date = str(datetime.strptime(End_Date, '%Y-%m-%d') - timedelta(days=Days))[0:10]
    #Converting a list of trading days into a numpy array because lists do not support filtering by dates
    temptdays = numpy.array(TradingDays)
    #Getting all the dates that should be in the Data by filtering trading days between the start and end dates
    DatesCheckInTradingDays = list(temptdays[(temptdays >= Start_Date) & (temptdays <= End_Date)])
    #The Date check is true unless it fails
    DatesCheck = True
    #Diffrent Dates checking depending on of loose is set to True or not
    #Checking Strictly
    if DatesCheckInTradingDays != Dates and loose == False:
        # If the Dates within the Data does not equal the Dates exactly in the check it will fail the date check
        DatesCheck = False
        #Checking Loosely
    elif DatesCheckInTradingDays and loose:
        #Checking if the data you provided has atleast all the dates between two dates
        DatesCheck = all(dates in Dates for dates in DatesCheckInTradingDays)
    #Checking it passed both checks if so the overall check will be passed if not it will fail
    if DatesCheck and LengthCheck:
        check = True
    else:
        check = False
    #This will return a Series with three rows, Status which is whether or not it passed the date and length check
    #Length which is the amount of days worth of data the Data has and Dates Check which is whether if it contains
    #all the dates it should have
    Data = {'Status':check,'Length':length,'Dates Check':DatesCheck}
    Data = pandas.Series.from_array(Data)
    return Data
def datadateslice(Data,Start_Date=None,End_Date=_datadate(Today),Days=None,Trading_Days=None):
    #This function will slice a Pandas DataFrame or Series between dates in the following ways:
        #You can slice a your Data between two dates by defined a Start and End Date
        #You can slice your Data to have a certain amount of data by defining Trading Days. This will get all the data for
        #the past x Trading Days from a Date you defined date. The default date is the datadate
    #Checking whether you gave a valid Data Type(DataFrame or Series)
    if isinstance(Data,pandas.DataFrame) is False and isinstance(Data,pandas.Series) is False:
        raise Exception('Fields must be either a DataFrame or a Series')
    #Checking if you defined atleast one of the required arguments
    if Days == None and Start_Date == None and Trading_Days == None:
        raise Exception('You must specify a Start Date, amount of Days, or amount of trading days')
    #Checking whether you defined variables that are compatible
    if Days != None and Start_Date != None or Days != None and Trading_Days != None or Start_Date != None and Trading_Days != None:
        raise Exception('You must specify only one of the following Start_Date,Trading_Days or Days')
    #Calculating Start Date if you defined Days
    if Days != None:
        Start_Date = str(datetime.strptime(End_Date,'%Y-%m-%d') - timedelta(days=Days))[0:10]
        #Calculting Start Date if you defined Trading_Days
    elif Trading_Days != None:
        Start_Date = TradingDays[TradingDays.index(End_Date)-Trading_Days+1]
#    datescheck = datadayscheck(Data, Start_Date=Start_Date, End_Date=End_Date, loose=True)
#    if datescheck['Status'] == False:
#        raise Exception('The data you passed does not contain the dates you asked for')
    NData = Data[(Data.index.get_level_values('dat') >= Start_Date) & (Data.index.get_level_values('dat') <= End_Date)].copy()
    if Trading_Days != None:
        sizefilter = NData.groupby(['ticker', 'exchange']).size() == Trading_Days
        sizefilter = sizefilter[sizefilter == True]
        NData = NData[NData.reset_index('dat').index.isin(sizefilter.index)]
    return NData
def RemoveBadData(Data,Start_Date=None,End_Date=_datadate(Today),Days=None,Trading_Days=None):
    #This function will remove stocks whose data does not contain the dates you specify
    #You can specify the dates in 3 ways:
        #You can specify a start date and a end date and the dates inbetween these two will be calculated including the dates you start and end date
        #You can specify abd End_Date and a amount of Days to go back and the dates between these will be calculated
        #You can specify a End_Date and amount of Trading Days to look back
        #If you do not use one of these three methods an expection will be raised
    #Checking whether you gave a valid Data Type(DataFrame or Series)
    if isinstance(Data,pandas.DataFrame) is False and isinstance(Data,pandas.Series) is False:
        raise Exception('Fields must be either a DataFrame or a Series')
    #Checking if you defined atleast one of the required arguments
    if Days == None and Start_Date == None and Trading_Days == None:
        raise Exception('You must specify a Start Date, amount of Days, or amount of trading days')
    #Checking whether you defined variables that are compatible
    if Days != None and Start_Date != None or Days != None and Trading_Days != None or Start_Date != None and Trading_Days != None:
        raise Exception('You must specify only one of the following Start_Date,Trading_Days or Days')
    if Days != None:
        Start_Date = str(datetime.strptime(End_Date,'%Y-%m-%d') - timedelta(days=Days))[0:10]
        #Calculting Start Date if you defined Trading_Days
    elif Trading_Days != None:
        Start_Date = TradingDays[TradingDays.index(End_Date)-Trading_Days+1]
    temptdays = numpy.array(TradingDays)
    #Getting all the dates that should be in the Data by filtering trading days between the start and end dates
    DatesCheckInTradingDays = list(temptdays[(temptdays >= Start_Date) & (temptdays <= End_Date)])
    for x in Data.reset_index('dat')[~Data.reset_index('dat').index.duplicated(keep='first')].index:
        Dates = Data.reset_index('dat')['dat'][x]
        if ~all(dates in Dates for dates in DatesCheckInTradingDays):
            Data.drop(x,inplace=True)
    return Data
def notify(message, channel='quantheus'):
    token = slack_token
    sc = WebClient(token=token)
    sc.chat_postMessage(channel=channel,text=message)
def ticker_list(exchange='all'): #Get list of tickers Ticker list
    conn = create_engine('postgresql+psycopg2://' + qtheus_rds['user'] + ':' + qtheus_rds['password'] + '@' + qtheus_rds['host'] + '/' + qtheus_rds['dbname'])
    if exchange.lower() == "nyse":
        query = f"SELECT * FROM ticker_lists.tickers WHERE exchange = 'NYSE'"  # Construct query to get old data from database and clean it against the new data
        tickers = pandas.read_sql(query, conn)  # get data from table in df
    elif exchange.lower() == "nasdaq":
        query = f"SELECT * FROM ticker_lists.tickers WHERE exchange = 'NASDAQ'"  # Construct query to get old data from database and clean it against the new data
        tickers = pandas.read_sql(query, conn)  # get data from table in df
    elif exchange.lower() == 'all':
        query = f"SELECT * FROM ticker_lists.tickers"  # Construct query to get old data from database and clean it against the new data
        tickers = pandas.read_sql(query, conn)  # get data from table in df
    else:
        tickers = numpy.nan
        warn("You did not pass a exchange when calling get_tickers. No tickers were returned")

    return tickers
def upload_to_rds_table(data_,table:str,dbname=qtheus_rds['dbname'],user=qtheus_rds['user'],host=qtheus_rds['host'],password=qtheus_rds['password'],schema='public',remove_duplicate_rows=True,rm_duplicate_index=True,row_by_row=False,save_errors=True,chunks=1,index=['ticker','exchange','date']): #This function allows you to upload to a table. It also checks if the columns match and will add/delete columns as needed. THE DATAFRAME MUST BE PASSED WITHOUT AN INDEX(USE df.reset_index(inplace=True)).
    # The arguments are as follows:
    # data: a dataframe with no index.
    # table: what is the name of the table in the your df
    # dbname, user, host,password, are all the credentials and connection details. By default they are taken from the config file as qtheus_rds.
    # schema: what schema is your table in
    # remove_duplicate_rows: A bool and will remove rows with all duplicate values. It does not check the index
    # remove_duplicate_index: A bool and will remove duplicate indexes specified in the index arguement
    # chunks: A int and will break the data to be uploaded into the pre-defined amount of chunks
    # row_by_row: Upload the dataframe row by row, if you do this and save_errors you can upload a dataframe to a table and save the rows that do not get uploaded to logpath\errors+table_name.csv
    # save_errors: uploads a dataframe to a table and save the rows that do not get uploaded to logpath\errors+table_name.csv, if the error cannot be added to the df then print it
    # index: the index of the dataframe, used for removing duplicate indexes. This is good for composite primary keys(primary keys based on the column values)
    # Try conditions for connecting to db.


    #Can further optimize upload speeds by varying chunks sizes when formating and when uploading to db or chunking data after error instead of row by row
    #Chunking the data when formating helps reduce max ram utilization. Formating two large dataframes takes alot of memory. Chunking it then formating chunk by chunk reduces the max utlilization.
    #Chunking the data when uploading is beneficial as if there is an error in a chunk it will not need to iterate the entire dataset to find the error. Rather just the chunk. Although the more chunks means the more queries to the db. Less queries is ussually faster. Therefore optimizing chunk size for uploading depends on how many errors are in the dataset
    chunked_data = numpy.array_split(data_,chunks)
    chunk_total_len = len(chunked_data)
    errors_overall = numpy.array([])
    print('\n\nStarting Upload of Data\n\n')
    count = 0
    total_time_ = []
    try:
        conn = create_engine(
            'postgresql+psycopg2://' + user + ':' + password + '@' + host + '/' + dbname)  # Connection to upload to database
    except Exception as e:  # catch exception and notify
        print("Connection Error: Could not connect sql alchemy to Database " + table)
        notify("Connection Error: Could not connect sql alchemy to Database " + table)
        raise e
    try:
        p_conn = psycopg2.connect(dbname=dbname, user=user, host=host,
                                  password=password)  # Connection to get table columns as they must match
    except Exception as e:
        print("Connection Error: Could not connect psycopg2 to Database " + table)
        notify("Connection Error: Could not connect psycopg2 to Database " + table)
        raise e
    uploaded_rows = 0
    not_uploaded_rows = 0
    if rm_duplicate_index or remove_duplicate_rows:
        if "date" in index:
            min_date = str(pandas.to_datetime(data_['date']).min().date())
            max_date = str(pandas.to_datetime(data_['date']).max().date())
            query = f"SELECT * FROM {schema}.{table} where date >= TO_DATE('{min_date}', 'YYYY-MM-DD') and date <= TO_DATE('{max_date}', 'YYYY-MM-DD')"
        elif "dat" in index:
            min_date = str(pandas.to_datetime(data_['date']).min().date())
            max_date = str(pandas.to_datetime(data_['date']).max().date())
            query = f"SELECT * FROM {schema}.{table} where date >= TO_DATE('{min_date}', 'YYYY-MM-DD') and date <= TO_DATE('{max_date}', 'YYYY-MM-DD')"
        else:
            query = f"SELECT * FROM {schema}.{table}"
        del data_
        # Construct query to get old data from database and clean it against the new data
        whole_dbd = pandas.read_sql(query, conn)  # get data from table in df
        for data in chunked_data:
            if "date" in index:
                min_date = str(pandas.to_datetime(data['date']).min().date())
                max_date = str(pandas.to_datetime(data['date']).max().date())
                dbd = whole_dbd[(whole_dbd['date'] >= min_date) & (whole_dbd['date'] <= max_date)]
            elif "dat" in index:
                min_date = str(pandas.to_datetime(data['dat']).min().date())
                max_date = str(pandas.to_datetime(data['dat']).max().date())
                dbd = whole_dbd[(whole_dbd['dat'] >= min_date) & (whole_dbd['dat'] <= max_date)]
            else:
                dbd = whole_dbd
        total_rows_chunk = len(data)
        start_ = datetime.now()
        print('\n\nFormatting #'+str(count+1)+' out of '+str(chunks)+' Total Chunks')
        data.reset_index(inplace=True,drop=True)
        data.set_index(index,inplace=True)

        #Removing duplicate rows/indexes
        # Clean Old Data with New Data
        new_rows = data
        uploaded_rows_before = uploaded_rows
        del data
        if rm_duplicate_index or remove_duplicate_rows:
            if (~new_rows.empty and remove_duplicate_rows and (len(dbd) > 0)) or (~new_rows.empty and rm_duplicate_index and (len(dbd) > 0)): #If we want to remove duplicate rows or indexes we need to get the data from the table and compare it the data we currently have
                if 'date' in index:
                    dbd['date'] = dbd['date'].dt.strftime('%Y-%m-%d')
                dbd.reset_index(inplace=True, drop=True)
                dbd.set_index(index, inplace=True)
                new_rows = pandas.concat([new_rows, dbd]) #combine the data we have and the data in the table. We can now use .drop_duplicates and .duplicated on the combined df to get only the rows not in the table
                new_rows = pandas.concat([new_rows, dbd]) #We concat the database data twice because when we scan for duplicates all the rows from the db will automaticlly be dropped as they were added twice
            if remove_duplicate_rows: #if the remove_duplicate_rows argument is True then we will drop duplicate rows
                new_rows['duplicated'] = new_rows.duplicated(keep=False)
                new_rows = new_rows[new_rows['duplicated'] == False]  # Removing Duplicate Rows
            if not new_rows.empty: #After the scrubbing continue if the df is not empty
                if rm_duplicate_index: #if the rm_duplicate_index argument is True then duplicate indexes will be removed.
                    new_rows['duplicated'] = new_rows.index.duplicated(keep=False)
                    new_rows = new_rows[new_rows['duplicated'] == False]  # Removing Duplicate Indexes
                new_rows = new_rows.reset_index() #reseting index again
        if not new_rows.empty:
            # Checking table for columns, if the columns do not match exactly an error will be raised.
            curr = p_conn.cursor()
            #Get Columns in db
            curr.execute(f"Select * FROM {schema}.{table} LIMIT 0")
            db_colnames = [desc[0] for desc in curr.description]
            curr.close()
            # Get Columns in current df
            cd_colnames = new_rows.columns.values
            # Find Diffrent Columns
            diffrent_columns = numpy.setdiff1d(cd_colnames, db_colnames)
            new_rows = new_rows.drop(columns=diffrent_columns).copy() #drop the diffrent columns
            cd_colnames = new_rows.columns.values #Get column values again
            missing_columns = numpy.setdiff1d(db_colnames, cd_colnames) #Find missing columns
            for col in missing_columns: #add the missing columns
                new_rows[col] = None
            cd_colnames = new_rows.columns.values
            errors = numpy.array([])
            uploaded_rows_before = uploaded_rows
            print("Uploading Chunk #"+str(count+1)+' out of '+str(chunks)+' Total Chunks'+" to RDS")
            if not new_rows.empty: #After this scrubbing if there is still data left to be uploaded then will will upload it the specified server
                if not row_by_row: #Upload at once
                    try:
                        new_rows.to_sql(table, conn, if_exists='append',schema=schema,index=False)  # Uploading to rds instance
                        uploaded_rows += len(new_rows)
                    except Exception as e:
                        print("Connection Error: Could not upload to \n"+table+str(e))
                        notify("Connection Error: Could not upload to \n"+table+str(e))
                        print('\n\nTrying to Upload Row By Row\n\n')
                        total_time_row = []
                        new_rows_len = len(new_rows)
                        for y in new_rows.index:
                            row_start = datetime.now()
                            try:
                                row = new_rows.iloc[y:y + 1]
                                row.to_sql(table, conn, if_exists='append', schema=schema,index=False)  # Upload entire row to rds
                                uploaded_rows += 1
                            except Exception as e:
                                if not save_errors:  # if we are not saving the errors raise it
                                    print("Connection Error: Could not upload to " + table)
                                    notify("Connection Error: Could not upload to " + table)
                                    raise e
                                else:  # if we are saving errors do not raise the error rather add it to a df, if the error cannot be added to the df then print it
                                    try:
                                        print("Error with " + str(row[index].values[0]))
                                        print("Exception " + str(e))
                                        errors = numpy.append(errors, row[index].values[0])
                                    except Exception as e:
                                        print('Error adding row to errors: ' + str(row[index].values[0]))  # if we cannot add print it
                                        print('\n\n\n\nException: ' + str(e) + '\n\n\n\n')
                            row_end = datetime.now()
                            total_time = (row_end - row_start).total_seconds()
                            total_time_row.append(total_time)
                            if len(total_time_row) != 0:
                                avg_time = (sum(total_time_row) / len(total_time_row))
                                if len(total_time_row) % 100 == 0:
                                    row_num = len(total_time_row)
                                    time_left = (new_rows_len - row_num) * avg_time
                                    print('Upload took',str(total_time),'Seconds'+', '+'Average Upload Time:',str(avg_time)+' Seconds, '+str(round(time_left, 2) / 60) + 'minutes till completion for row #'+str(row_num)+' out of '+str(new_rows_len)+' rows for Chunk #'+str(count+1)+' out of '+str(chunks)+' Total Chunks, '+' Rows Uploaded: '+str(uploaded_rows)+', Rows Not Uploaded:',str(not_uploaded_rows))
                    chunk_total_len = len(new_rows)
                    row_total_time = []
                else:
                    for x in new_rows.index:
                        row_start = datetime.now()
                        try:
                            row = new_rows.iloc[x:x+1]
                            row.to_sql(table, conn, if_exists='append',schema=schema,index=False) #Upload entire row to rds
                        except Exception as e:
                            if not save_errors: #if we are not saving the errors raise it
                                print("Connection Error: Could not upload to "+table)
                                notify("Connection Error: Could not upload to "+table)
                                raise e
                            else: #if we are saving errors do not raise the error rather add it to a df, if the error cannot be added to the df then print it
                                try:
                                    print("Error with "+str(row[index].values[0]))
                                    print("Exception "+str(e))
                                    errors = numpy.append(errors,row[index].values[0])
                                except Exception as e:
                                    print('Error adding row to errors: '+str(row[index].values[0])) #if we cannot add print it
                                    print('\n\n\n\nException: '+str(e)+'\n\n\n\n')
                        row_end = datetime.now()
                        total_time = (row_end - row_start).total_seconds()
                        row_total_time.append((row_end - row_start).total_seconds())
                        uploaded_rows += 1
                        if len(row_total_time)%100 == 0:
                            if len(row_total_time) != 0:
                                avg_time = (sum(row_total_time) / len(row_total_time))
                                row_num = len(row_total_time)
                                time_left = (chunk_total_len - row_num)*avg_time
                                print('Upload took',str(total_time),'Seconds'+', '+'Average Upload Time:',str(avg_time)+' Seconds, '+str(round(time_left, 2) / 60) + 'minutes till completion for row #'+str(row_num)+' out of'+str(chunk_total_len)+'rows for Chunk #'+str(count+1)+' out of '+str(chunks)+' Total Chunks, '+' Rows Uploaded: '+str(uploaded_rows)+', Rows Not Uploaded:',str(not_uploaded_rows))
            if save_errors and len(errors) != 0:
                numpy.append(errors_overall,errors)
        not_uploaded_rows += total_rows_chunk-(uploaded_rows-uploaded_rows_before)
        print('Uploaded #'+str(count+1)+' out of '+str(chunks)+' Total Chunks, '+' Rows Uploaded: '+str(uploaded_rows),', Rows Not Uploaded:',str(not_uploaded_rows))
        end_ = datetime.now()
        total_time = (end_ - start_).total_seconds()
        total_time_.append(total_time)
        if count != 0:
            avg_time = (sum(total_time_) / (count+1))
            time_left = (chunks - count+1) * avg_time
            print('Upload took',str(total_time),'Seconds, Average Upload Time:',str(avg_time),'Seconds'+', '+str(round(time_left, 2) / 60) + ' minutes till upload of all chunks')
        count += 1
    if save_errors: #Saving errors to csv if save_errors argument True
        print('errors saved to '+logpath+'errors_'+table+'_'+str(datetime.now())+'.csv')
        try:
            pandas.DataFrame(errors_overall).to_csv(logpath + 'errors_' + str(datetime.now().date()) + '.csv')
        except Exception as e:
            print('\n\n\nEXCEPTION Could not save errors Printing:\n\n\n', errors_overall)
            print(e)
    print('\n\nUploaded All to RDS\n\n')
def clean_data(data,Field):
    data.replace('', numpy.nan, inplace=True)
    data.replace(' ', numpy.nan, inplace=True)
    # Slicing the specified data fields if the desired fields are not valid it will ask you to re-enter valid ones
    if 'all' not in Field:
        while True:
            try:
                # Trying to slice the data
                data = data[Field]
                # Break out of loop is slice was succesful
                break
                # If it does not happen
            except KeyError or IndexError:
                for x in Field:
                    if x not in data.columns.values:
                        validF = list(data.columns)
                        validF.remove('index')
                        warnings.warn(x + ' is not a valid Field.' + ' Valid Fields are ' + str(validF))
                        Field = input('\nPlease enter valid Fields with a space as the delimter: ').split()
                        break
    data = data[~data.index.duplicated(keep='first')]
    data = data.apply(pandas.to_numeric, errors='coerce')

    return data

