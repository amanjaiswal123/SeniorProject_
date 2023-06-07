#This is used to get data
import warnings
from datetime import datetime,timedelta
import os
from source.Commons import NearestTradingDay,clean_data
from config.config import backtestdatapath
import pandas
import psycopg2
from config.config import qtheus_rds
from data_scripts.download_data import tiingo_nyse_nasdaq_download, alphavantage_nyse_nasdaq_download

def get_data_tiingo(Field:[],Start_Day=None,End_Day=NearestTradingDay(str(datetime.now().date())),Days=None):
    if Start_Day == None:
        Start_Day = str(datetime.strptime(End_Day,'%Y-%m-%d') - timedelta(days=Days))[0:10]
    # Get Data directly from Alpha Vantage
    Data = tiingo_nyse_nasdaq_download(Start_Day, End_Day)
    print('Retrieved '+str(Field)+' Data')

    if 'adj_open' not in Data.columns.values:
        Data['adj_open'] = (Data['adj_close'] / Data['close']) * Data['open']
    if "date" in Data.columns.values:
        try:
            Data = Data.rename(columns={'date': 'dat'})
        except:
            pass
    Data = Data[(Data['dat'] >= Start_Day) & (Data['dat'] <= str(End_Day))]
    # Setting Index and organizing Data
    Data = Data.sort_values(['ticker', 'exchange', 'dat'])
    Data.set_index(['ticker', 'exchange', 'dat'], inplace=True)
    Data = clean_data(Data,Field)
    return Data
def get_data_alpha(Field:[],Start_Day=None,End_Day=NearestTradingDay(str(datetime.now().date())),Days=None):
    if Start_Day == None:
        Start_Day = str(datetime.strptime(End_Day, '%Y-%m-%d') - timedelta(days=Days))[0:10]
        # Get Data directly from Alpha Vantage
    Data = alphavantage_nyse_nasdaq_download()
    print('Retrieved '+str(Field)+' Data')
    Data['adj_open'] = (Data['adj_close'] / Data['close']) * Data['open']
    Data = Data[(Data['dat'] >= Start_Day) & (Data['dat'] <= str(End_Day))]
    # Setting Index and organizing Data
    Data = Data.sort_values(['ticker', 'exchange', 'dat'])
    Data.set_index(['ticker', 'exchange', 'dat'], inplace=True)
    Data = clean_data(Data,Field)
    return Data
def get_data_backtest(Field:[],Start_Day=None,End_Day=NearestTradingDay(str(datetime.now().date())),Days=None):
    # Get static EOD Data file for the last 20 years
    if Start_Day == None:
        Start_Day = str(datetime.strptime(End_Day, '%Y-%m-%d') - timedelta(days=Days))[0:10]
    if os.path.exists(backtestdatapath):
        if True in [set(list('Backtest')).issubset(list(x)) for x in os.listdir(backtestdatapath)]:
            for x in os.listdir(backtestdatapath):
                if set(list('BacktestData')).issubset(list(x)):
                    Data = pandas.read_csv(backtestdatapath + '/' + x)
                    print('Retrieved ' + str(Field) + ' Data')
                    break
        else:
            raise Exception('Backtest Market Data File not Found')
    else:
        raise Exception('Backtest Market Data File not Found')
    for column in Data.columns.values:
        if 'Unnamed' in column:
            Data.drop(columns=column, inplace=True)
    Data = Data[(Data['dat'] >= Start_Day) & (Data['dat'] <= str(End_Day))]
    # Setting Index and organizing Data
    Data = Data.sort_values(['ticker', 'exchange', 'dat'])
    Data.set_index(['ticker', 'exchange', 'dat'], inplace=True)
    Data = clean_data(Data,Field)
    return Data
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
    Data = clean_data(Data,Field)
    return Data