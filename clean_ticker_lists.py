from datetime import datetime
from datetime import timedelta
import time as Time
from source.Commons import upload_to_rds_table, notify
from config.config import qtheus_rds
import pandas
from sqlalchemy import create_engine
import psycopg2


# This is used to update the ticker list
os.system('sudo timedatectl set-timezone America/New_York')
while True:
    # Defining Today's Date
    Today = str(datetime.today().date())
    # Date that data will be available. Data is will not be available for today because all data is delayed by 1 day so
    # to reflect this discrepancy accurately we must also subtract 1 day from the current day and label all the data
    # with that date. For example if we are pulling data today it is not the data for the current date rather the data
    # for yesterday
    # Check if Today is a Trading Day by looking if it is in a list of all Trading Days until 2024 that is imported
    print('Creating/Updating a cleaned ticker list on', Today)
    try:

        conn = create_engine('postgresql+psycopg2://' + qtheus_rds['user'] + ':' + qtheus_rds['password'] + '@' + qtheus_rds['host'] + '/' + qtheus_rds['dbname'])  # Connection to upload to database
    except Exception as e:  # catch exception and notify
        print("Connection Error: Could not connect sql alchemy to Database to clean ticker list")
        #notify("Connection Error: Could not connect sql alchemy to Database ")
        raise e
    try:
        p_conn = psycopg2.connect(dbname=qtheus_rds['dbname'], user=qtheus_rds['user'], host=qtheus_rds['host'],password=qtheus_rds['password'])  # Connection to get table columns as they must match
    except Exception as e:
        print("Connection Error: Could not connect psycopg2 to Database to clean ticker list")
        #notify("Connection Error: Could not connect psycopg2 to Database to clean ticker list")
        raise e
    # Get all tables in the ticker_list schema. It will gather all the data and scrub it into one list
    db_cursor = p_conn.cursor()

    #Creating query to get all table names in schema
    s = ""
    s += "SELECT"
    s += " table_schema"
    s += ", table_name"
    s += " FROM information_schema.tables"
    s += " WHERE"
    s += " ("
    s += " table_schema = '" + 'ticker_lists' + "'"
    s += " )"
    s += " ORDER BY table_schema, table_name;"

    # get the table names
    db_cursor.execute(s)
    sources = db_cursor.fetchall() #table names are stored in tuple with the as (schema, table)


    #(This might get rid of columns in subsequently added rows)

    first_data_source = True #the first data source will create the df the subsequent ones will be added to the first one
    for x in sources:
        if x[1] != 'tickers': # We do not want the data from the ticker table because that is the overall table and we will compare that when we are uploading to rds
            query = f"SELECT * FROM ticker_lists.{x[1]}"  # Construct query to get old data from database and clean it against the new data
            dbd = pandas.read_sql(query, conn).set_index(['ticker','exchange']) #Get data from table
            if first_data_source: #If it is the first data source create the overall_data dataframe
                overall_data = dbd
                first_data_source = False #Now all subsequently downloaded data will be concated to the first one
            else:
                overall_data = pandas.concat([overall_data, dbd])  # Concat to subsequent data to overall_data
    overall_data = overall_data.loc[~overall_data.index.duplicated(keep='first')]  # Removing Duplicate Indexes
    overall_data = overall_data.reset_index() #reseting the index
    overall_data['exchange'] = overall_data['exchange'].str.upper() #making sure exchange is all uppercase so that NASDAQ and nasdaq will be counted as duplicates
    overall_data['ticker'] = overall_data['ticker'].str.upper() #making sure exchange is all uppercase so that AAPL and aapl will be counted as duplicates
    upload_to_rds_table(overall_data, 'tickers', schema='ticker_lists',index=['ticker','exchange'],row_by_row=True,save_errors=True) #Uploading to db, we do not want to remove duplicate rows becuase most rows have mostly na values, this will run the risk that those rows will be dropped when uploading.

    now = datetime.now()
    # Get the time we want to re-run the program at
    runAt = (datetime.now() + timedelta(days=1)).replace(hour=17, minute=45, second=0, microsecond=0)
    # Get the difference between the time we want to sleep at and the time right now
    delta = (runAt - now).total_seconds()
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n')
    # Sleep for the difference between the time now and the time we want to run at again where it will resume and go to
    # Start of the while loop again
    Time.sleep(delta)

