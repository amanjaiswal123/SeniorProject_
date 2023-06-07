import pandas
from source.Commons import notify
from config.config import *
from sqlalchemy import create_engine
from psycopg2 import connect


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
