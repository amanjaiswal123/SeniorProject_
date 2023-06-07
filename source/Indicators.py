import pandas
from source.Commons import datadayscheck, datadateslice, TradingDays
from datetime import datetime, timedelta
import numpy
def _RSI(Data,datadate,first:bool,prevgain=None,prevloss=None,Period=14):
    if isinstance(Data,pandas.Series) is False:
        raise Exception('To calculate RSI you need to pass a series')
    #Below is checking whether the user has supplied the correct amount of Data needed to calculate RSI
    #If it is the first time RSI is being calculated the amount of data should equal the amount of periods
#    if first == True:
#        check = datadayscheck(Data,Period,datadate=datadate)
    #If it is not the first time it is being calculated you only need the previous 2 days worth of data
#    else:
#        check = datadayscheck(Data,2,datadate=datadate)
    #This is checking whether it passed the data check if it did not then it will raise the appropriate exception
#    if check['Status'] == False:
#        raise Exception('To Calculate RSI with this configuration you need 2 Days worth of data you provided',check['Length'],'days worth of data')
    #To calculate RSI after the first calculation the user must specify the previous calculations gains and losses which is returned every calcualtion
    if first == False and prevgain is None or first == False and prevloss is None:
        raise Exception('To calculate RSI after the first calculation you must pass the previous calculations gains and losses')
    #If all the checks have been passed without any exceptions then the calculation will begin
    #The first calculation of RSI will be different than the subsequent ones
    if first ==  True:
        #Daily Diffrence between Close prices for the given period
        diff = Data.groupby(['ticker','exchange']).diff()
        #Dropping first day because it will be a NaN
        diff = diff[diff.index.get_level_values('dat').isin(diff.index.get_level_values('dat').values[1:Period])]
        # The sum of the total gains for the given period divided by the given period
        diffgain = diff.copy()
        diffgain[diffgain < 0] = 0
        diffgain = diffgain.groupby(['ticker','exchange']).sum()/Period
        # The sum of the total absolute value losses for the given period divided by given period
        diffloss = diff.copy()
        diffloss[diffloss > 0] = 0
        diffloss = diffloss.abs()
        diffloss = diffloss.groupby(['ticker','exchange']).sum()/Period
        # Previous Average Gains Calculation/Previous Average Losses Calculation(Absoulute value)
        RS = diffgain / diffloss
    else:
        #Diffrence between previous close and current close
        diff = Data.groupby(['ticker','exchange']).diff()
        diff = diff[diff.index.get_level_values('dat') == diff.index.get_level_values('dat').values[1]].reset_index().drop(columns='dat').set_index(['ticker','exchange'])
        diff = diff[diff.columns.values[0]]
        #The Gain from previous close to current close if any there is any
        diffgain = diff.copy()
        diffgain[diffgain <= 0] = 0
        diffgain = ((prevgain*(Period-1))+diffgain)/Period
        #The Loss from previous close to current close if any there is any
        diffloss = diff.copy()
        diffloss[diffloss > 0] = 0
        diffloss = diffloss.abs()
        diffloss = ((prevloss*(Period-1))+diffloss)/Period
        #Using the previous gain and loss calculations to calculate the RS
        RS = diffgain/diffloss
    #RSI Calculation
    RSI = (100 - 100 / (1 + RS))
    RSIData = pandas.DataFrame(columns=['RSI','diffgain','diffloss'])
    RSIData['RSI'] = RSI
    RSIData['diffgain'] = diffgain
    RSIData['diffloss'] = diffloss
    #The returned Data will be returned in a Dataframe with three columns RSI,diffgains and diffloss
    return RSIData

def _MACD(Data, first:bool, Period1=26, Period2=12, SignalLinePeriod=9, PrevLongEMA=None, PrevShortEMA=None, PrevSignalEMA=None):
    #This function is used to get MACD.

    #To use this function you must give it Data that is a series where the index is a ticker date then exchange and the
    #values are numeric and it must contain the long period + the signal line period worth of data points. After you pass
    #Data you must define whether or not it is the first time it is running. After defining that you can define
    #Period1, Period2, and Signal Line Period or leave them at the default configuration which is
    #Period1=26 Days, Period2=12 Days,SignalLinePeriod=9 Days.
    #If it is not the first run you must pass the previous LongEMA, ShortEMA, and SignalLineEMA which will have
    #been returned in the dataframe the first time you calculated MACD.
    if isinstance(Data,pandas.Series) is False:
        raise Exception('To calculate MACD please pass a series as the Data')
    End_Date = Data[Data.index.get_level_values('ticker').values[0]].tail(1).index.get_level_values('dat').values[0]
    if Period1 > Period2:
        longerperiod = Period1
    else:
        longerperiod = Period2
    #Checking whether the data you provided contains the correct amount of data points
    if first == True:
        #The first time you calculate MACD you need longer Period+Signal Line Period worth of data points
        check = datadayscheck(Data, longerperiod + SignalLinePeriod,End_Date=End_Date)
    else:
        #After the first calculation you only need to pass 1 Day worth of data points which should be the most current
        #close or other applicable value
        check = datadayscheck(Data,1,End_Date=End_Date)
    #If you do not have the required amount of data
    if check['Status'] == False:
        if first:
            raise Exception('To Calculate This configuration of MACD we need', Period1 + SignalLinePeriod, 'Days worth of data you provided', check['Length'], 'days worth of data')
        else:
            Exception('After the first calculation of MACD you only need to provide 1 Days worth of Data. You Provided',check['Length'], 'worth of Data.')
    #Checking if you provided the previous Long EMA, Short EMA, and Signal EMA if it is past the first run
    if first == False and PrevLongEMA is None or first == False and PrevShortEMA is None or first == False and PrevSignalEMA is None:
        raise Exception('You must give the previous EMA for the Long, Short and Signal Periods after the first calculation')
    #Calculting The Multipliers
    Period1Multi = (2 / (Period1 + 1))
    Period2Multi = (2 / (Period2 + 1))
    SignalMulti = (2/(SignalLinePeriod+1))
    #To calculate the signal line for MACD you need to get the previous MACD for the amount of days you defined in
    #Signal Line Periods so here we calculate the MACD for the defined amount of Days and create update the EMA each day.

    #This variable is used to signify that it is the first time calculting the MACD when calculating the MACD
    frst = True
    #Here we are getting the previous dates required to calculate the MACD for the Signal Line
    Dates = Data.index.get_level_values('dat').values[Period1:Period1 + SignalLinePeriod]
    #The first time you calculate MACD you use a SMA and then use that SMA value to calculate a EMA for the subsquent
    #calculations
    if first == True:
        #Calculating the MACD for the Signal Line
        for x in Dates:
            if frst is True:
                #Getting the SMA for the Long Period
                EMALong = datadateslice(Data,End_Date=x,Trading_Days=26).groupby(['ticker','exchange']).sum() / Period1
                #Getting the SMA for the Short Period
                EMAShort = datadateslice(Data, End_Date=x, Trading_Days=Period2).groupby(['ticker', 'exchange']).sum() / Period2
                #Getting the MACD
                MACD = EMAShort-EMALong
                #For the first Day the Signal Line is equal to the MACD and we will use it to Calculte the EMA the next Day
                SignalLine = MACD
                frst = False
            else:
                #Now we use the previous SMA to create EMA for the Long Period, Short Period and Signal Line Period
                TodaysAdjClose = Data[Data.index.get_level_values('dat') == x].reset_index().drop(columns='dat').set_index(['ticker','exchange'])['adj_close']
                EMALong = (TodaysAdjClose-EMALong)*Period1Multi+EMALong
                EMAShort =(TodaysAdjClose-EMAShort)*Period2Multi+EMAShort
                MACD = EMAShort-EMALong
                SignalLine = (MACD-SignalLine)*SignalMulti+SignalLine
    else:
        #Used to calculate the MACD and Signal Line after the first calculation
        TodaysAdjClose = (Data.reset_index().drop(columns='dat').set_index(['ticker','exchange']))
        TodaysAdjClose = TodaysAdjClose[TodaysAdjClose.columns.values[0]]
        EMALong = (TodaysAdjClose-PrevLongEMA)*Period1Multi+PrevLongEMA
        EMAShort = (TodaysAdjClose-PrevShortEMA)*Period2Multi+PrevShortEMA
        MACD = EMAShort-EMALong
        SignalLine = (MACD-PrevSignalEMA)*SignalMulti+PrevSignalEMA
    #The Data will be returned in a Dataframe that contains the following column MACD','Signal Line','EMA Long','EMA Short
    MACDData = pandas.DataFrame(columns=['MACD','Signal Line','EMA Long','EMA Short','Histogram'])
    MACDData['MACD'] = MACD
    MACDData['Signal Line'] = SignalLine
    MACDData['EMA Long'] = EMALong
    MACDData['EMA Short'] = EMAShort
    MACDData['Histogram'] = MACDData['MACD']-MACDData['Signal Line']
    return MACDData

def _EOV(Data,first:bool,TimePeriod=14,prevEOV=None):
    #Used to calculate EOV Exponential Oscillating Volatility
    #EOV is a meant to determine recent volatility
    #It is calculated by getting the Avg Absolute Value % change over a given time performing a EMA calculation on it
    #with the percent change from yesterday to today

    #To calculate EOV the Data you pass should be a series of in which the index is ticker, dat, and exchange. The Data's
    #Data points must be equal to the Time Period
    #The time period is set to 14 for default

    #The calculation for EOV is as follows:
        #Required Data:
            #Today's Percent Change = Absolute value percent change from closest previous trading day to today
            #Average Percent Change = Absolute Value average percent change over a given period of a time
            #Previous EOV = EOV from last calculation
            #Multi = 2/(TimePeriod+1)

        #First Calculation
            #EOV = ((Today's Percent Change - Average Percent Change)*Multi + Average Percent Change)

        #Subsequent Calculations
            #EOV = ((Today's Percent Change - Previous EOV)*Multi + Previous EOV

    if isinstance(Data, pandas.Series) is False:
        raise Exception('To calculate MACD please pass a series as the Data')
    End_Date = Data[Data.index.get_level_values('ticker').values[0]].tail(1).index.get_level_values('dat').values[0]
    #Multiplier
    Multi = 2 / (TimePeriod + 1)
    #On the first run your data needs to have an equivalent amount of days worth of data as your defined Time Period but
    #subsequent calculations only require 2 days worth of data
#    if first == True:
        #Checking if you provided data with an equivalent amount of days as the Time Period you defined
#        check = datadayscheck(Data,TimePeriod,End_Date=End_Date)
        #If you did not an error is raised
#        if check['Status'] == False:
#            raise Exception('To calculate you configuration of EOV you need',TimePeriod,'days worth of data. You provided',check['Length'],'days worth of data')
#    else:
        #Checking if you provided 2 days worth of data
#        check = datadayscheck(Data,2,End_Date=End_Date)
        #Raising Exception if you did not
#        if check['Status'] == False:
#            raise Exception('After the first calculation of EOV you only need to provide 2 Days worth of Data. You Provided',check['Length'],'days worth of data')
    #After the first calculation you must provide the most recently calculated EOV this is checks if you did and will
    #raise an exception if you did not
    if first == False and prevEOV is None:
        raise Exception('To calculate EOV after the first calcultuion you need to provide the previous EOV that has been returned in the previous EOV calculation')
    Data = Data.sort_index(level=['ticker','exchange','dat'])
    #Calculation of the first day
    if first == True:
        #The percent change for most recent previous trading day to today
        TodaysPercentChange = Data.groupby(['ticker', 'exchange']).tail(2).groupby(['ticker', 'exchange']).pct_change().groupby(['ticker','exchange']).tail(1).reset_index(['dat']).drop(columns=['dat'])
        #Getting absolute value and converting to a series by index the first column
        TodaysPercentChange = TodaysPercentChange[TodaysPercentChange.columns.values[0]].abs()
        #Absolute value percent change over a given Period of time
        AveragePercentChange = Data.groupby(['ticker','exchange']).head(TimePeriod-1).groupby(['ticker','exchange']).pct_change().abs().dropna().groupby(['ticker','exchange']).mean()
        #EOV Calculation
        EOV = ((TodaysPercentChange-AveragePercentChange)*Multi+AveragePercentChange)
        #Calculation of subsequent calculations
    else:
        #Percent Change for most recent previous trading day to today
        TodaysPercentChange = Data.groupby(['ticker','exchange']).pct_change().groupby(['ticker','exchange']).tail(1).reset_index(['dat']).drop(columns=['dat'])
        #Converting to a series by indexing first column and getting absolute value
        TodaysPercentChange = TodaysPercentChange[TodaysPercentChange.columns.values[0]].abs()
        #EOV calculation
        EOV = ((TodaysPercentChange - prevEOV) * Multi + prevEOV)
    #Returning EOV
    return EOV

def _PeakTroughs(Fields,PeakGap=.2,AggregationDivsor=.03,MinHits=3,PeakProximity=.9,TroughProximity=.1):
    if isinstance(Fields, pandas.Series) is False:
        raise Exception('To calculate PeakTrough please pass a series as the Data')

    Fields = Fields.reset_index().sort_values(['ticker', 'exchange', 'adj_close', 'dat'], ascending=[True,True,True,False]).set_index(['ticker','exchange','dat'])
    Fields['Position'] = Fields.groupby(['ticker','exchange']).cumcount()+1
    Fields['Above'] = Fields['Position']-1
    Fields = Fields.reset_index('dat').drop(columns=['Position'])
    Fields['Above'] = Fields['Above']/Fields.groupby(['ticker','exchange']).size()
    Fields = Fields.reset_index().set_index(['ticker','exchange','dat'])
    Fields['PCT Change'] = Fields['adj_close'].groupby(['ticker','exchange']).pct_change().abs()
    Fields['Cumulative PCT Change'] = Fields['PCT Change'].groupby(['ticker','exchange']).cumsum()
    Fields[Fields['Cumulative PCT Change'].isna()] = 0
    Fields['test'] = Fields['Cumulative PCT Change']//AggregationDivsor

    Fields = Fields.reset_index().sort_values(['ticker', 'exchange', 'dat'], ascending=[True,True,True]).set_index(['ticker','exchange','dat'])
    Fields['test diff'] = Fields['test'].groupby(['ticker','exchange']).diff().abs()
    Fields['PCT Change'] = Fields['adj_close'].groupby(['ticker','exchange']).pct_change().abs()
    Fields['PCT Change'][Fields['PCT Change'] == numpy.inf] = 0
    Fields['Cumulative PCT Change'] = Fields['PCT Change'].groupby(['ticker','exchange']).cumsum()
    Fields = Fields.reset_index().sort_values(['ticker', 'exchange', 'test','dat'], ascending=[True,True,True,True]).set_index(['ticker','exchange','dat'])
    Fields['Rank'] = Fields[['Cumulative PCT Change','test']].groupby(['ticker','exchange','test']).cumcount()
    Fields['Cumulative PCT Change diff'] = Fields['Cumulative PCT Change'].diff().abs()
    Fields['Cumulative PCT Change diff'][Fields['Rank'] == 0] = numpy.nan
    Fields['PCT Change'][(Fields['test diff'] != 0) & (Fields['Rank'] != 0)] = Fields['Cumulative PCT Change diff'][(Fields['test diff'] != 0) & (Fields['Rank'] != 0)]
    Fields = Fields[Fields['PCT Change'] >= PeakGap]

    Fields = Fields.sort_index()
    temp = Fields[['adj_close','Above','test']].groupby(['ticker','exchange','test']).mean().round(2)
    temp['Hits'] = Fields[['test']].groupby(['ticker','exchange','test']).size()
    if 'adj_close' in temp.columns:
        PeaksTroughs = temp.groupby(['ticker', 'exchange']).tail(1)[['adj_close']].reset_index('test').drop(columns='test')
    else:
        PeaksTroughs = temp.reset_index('adj_close').groupby(['ticker', 'exchange']).tail(1)[['adj_close']]
    if 'adj_close' in temp.columns:
        PeaksTroughs['Peak'] = temp[(temp['Hits'] > MinHits) & (temp['Above'] > PeakProximity)].groupby(['ticker', 'exchange']).tail(1)[['adj_close']].reset_index('test').drop(columns='test')
    else:
        PeaksTroughs['Peak'] = temp[(temp['Hits'] > MinHits) & (temp['Above'] > PeakProximity)].groupby(['ticker','exchange']).tail(1).reset_index('adj_close')[['adj_close']]
    if 'adj_close' in temp.columns:
        PeaksTroughs['Trough'] = temp[(temp['Hits'] > MinHits) & (temp['Above'] < TroughProximity)].groupby(['ticker','exchange']).tail(1)[['adj_close']].reset_index('test').drop(columns='test')
    else:
        PeaksTroughs['Trough'] = temp[(temp['Hits'] > MinHits) & (temp['Above'] < TroughProximity)].groupby(['ticker','exchange']).tail(1).reset_index('adj_close')[['adj_close']]
    PeaksTroughs = PeaksTroughs.drop(columns='adj_close')
    return PeaksTroughs

def _TrendID(Data, datadate, OpposingTrendLimit, Trend, OposingQualifer:float):
    Trend = Trend.lower()
    if isinstance(Data, pandas.Series) is False:
        raise Exception('To identify trends please pass a series of prices')
    if Trend not in ['up','down']:
        raise Exception('The only valid inputs for Trend is up or down')
    PCTChange = Data.groupby(['ticker', 'exchange']).pct_change().dropna()
    if Trend == 'up':
        OpposingTrend = PCTChange[PCTChange < OposingQualifer]
    else:
        OpposingTrend = PCTChange[PCTChange > OposingQualifer]
    OpposingTrend = OpposingTrend.reset_index()
    OpposingTrend['temp date'] = OpposingTrend['dat']
    OpposingTrend.set_index(['ticker', 'exchange', 'dat'],inplace=True)
    OpposingTrend['Trading Days Index'] = OpposingTrend['temp date'].apply(lambda x: TradingDays.index(x))
    for x in range(1,OpposingTrendLimit):
        OpposingTrend[x] = OpposingTrend['Trading Days Index'].apply(lambda y: TradingDays[y+x])
        OpposingTrend[str(x)+'filter'] = OpposingTrend.groupby(['ticker', 'exchange']).apply(lambda y: y[x].isin(y['temp date'])).reset_index(level=[2,3])[x]
    for x in range(1,OpposingTrendLimit):
         OpposingTrend = OpposingTrend[OpposingTrend[str(x)+'filter'] == True]
    TrendOpposed = OpposingTrend.groupby(['ticker','exchange']).tail(1)[OpposingTrendLimit-1].reset_index().set_index(['ticker','exchange'])[OpposingTrendLimit-1]
    return TrendOpposed