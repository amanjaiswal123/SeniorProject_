from data_scripts.get_data import tiingo_nyse_nasdaq_download
from datetime import datetime
from datetime import timedelta
import time as Time
from source.Commons import notify
from config.config import logpath
from source.Commons import upload_to_rds_table
from source.AWS import get_max_date
from source.Commons import  NearestTradingDay
import os
# This is used to update data everyday from tiingo
os.system('sudo timedatectl set-timezone America/New_York')
while True:
    # Defining Today's Date
    Today = str(datetime.today().date())
    recent_date_rds = get_max_date('tiingo')
    before_market_close = datetime.now() < (datetime.now() + timedelta(days=0)).replace(hour=19, minute=30, second=0,microsecond=0)
    if before_market_close:
        recent_trading_day = NearestTradingDay(str((datetime.now() - timedelta(days=1)).date()))
    else:
        recent_trading_day = NearestTradingDay(Today)
    # Date that data will be available. Data is will not be available for today because all data is delayed by 1 day so
    # to reflect this discrepancy accurately we must also subtract 1 day from the current day and label all the data
    # with that date. For example if we are pulling data today it is not the data for the current date rather the data
    # for yesterday
    # Check if Today is a Trading Day by looking if it is in a list of all Trading Days until 2024 that is imported
    start_date = str((datetime.strptime(recent_date_rds, '%Y-%m-%d') + timedelta(days=1)).date())
    if True:
        print('Starting Data Pull from', start_date, 'to', recent_trading_day)
        try:
            todaydata = tiingo_nyse_nasdaq_download(end_date=recent_trading_day, start_date=start_date)
        except Exception as e:
            todaydata.to_csv(logpath+'todaydata_tiingo_'+Today+'.csv')
            print("Could not download data from tiingo on"+Today)
            #notify("Could not download data from tiingo on"+Today)
            raise e
        if len(todaydata) > 0:
            todaydata.to_csv(logpath+'todaydata_tiingo_' + Today + '.csv')
            upload_to_rds_table(todaydata,'tiingo',row_by_row=False,save_errors=True)
    now = datetime.now()
    # Get the time we want to re-run the program at
    if datetime.now() > (datetime.now() + timedelta(days=0)).replace(hour=21, minute=30, second=0, microsecond=0):
        runAt = (datetime.now() + timedelta(days=0)).replace(hour=21, minute=30, second=0, microsecond=0)
    else:
        runAt = (datetime.now() + timedelta(days=1)).replace(hour=21, minute=30, second=0, microsecond=0)
    # Get the difference between the time we want to sleep at and the time right now
    delta = (runAt - now).total_seconds()
    print('\nFinished Task for', Today + '. Going to sleep for,', str(round((int(delta) / 60) / 60, 2)) + ' hours.\n')
    # Sleep for the difference between the time now and the time we want to run at again where it will resume and go to
    # Start of the while loop again
    Time.sleep(delta)
