from datetime import datetime
from datetime import timedelta
import time as Time
from source.Commons import upload_to_rds_table
from data_scripts.download_tickers import get_tickers_advfn

# This is used to update data everyday from Alpha Vantage
os.system('sudo timedatectl set-timezone America/New_York')
while True:
    raised = False
    # Defining Today's Date
    Today = str(datetime.today().date())
    # Date that data will be available. Data is will not be available for today because all data is delayed by 1 day so
    # to reflect this discrepancy accurately we must also subtract 1 day from the current day and label all the data
    # with that date. For example if we are pulling data today it is not the data for the current date rather the data
    # for yesterday
    # Check if Today is a Trading Day by looking if it is in a list of all Trading Days until 2024 that is imported
    print('Updating Ticker List from advfn on', Today)

    #Get Tickers
    tickers = get_tickers_advfn()

    #Upload tickers to rds
    upload_to_rds_table(tickers,'advfn',schema='ticker_lists',index=['ticker','exchange'],row_by_row=True,save_errors=True)


    now = datetime.now()
    # Get the time we want to re-run the program at
    runAt = (datetime.now() + timedelta(days=1)).replace(hour=17, minute=15, second=0, microsecond=0)
    # Get the difference between the time we want to sleep at and the time right now
    delta = (runAt - now).total_seconds()
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n')
    # Sleep for the difference between the time now and the time we want to run at again where it will resume and go to
    # Start of the while loop again
    Time.sleep(delta)

