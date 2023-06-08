from source.Backtest import Backtest
import pandas
from datetime import datetime
from source.Commons import TradingDays, datadateslice
from source.Indicators import _PeakTroughs,_RSI
import numpy

def UpTrend(Fields, Portfolio, aval_cash, net_worth, datadate, first_calc, YSignals=None):
    TodaysClose = Fields['close'][(Fields['close'].index.get_level_values('dat') == datadate)].reset_index('dat')['close']
    TodaysAdjustedClose = Fields['adj_close'][(Fields['adj_close'].index.get_level_values('dat') == datadate)].reset_index('dat')['adj_close']
    Volume90 = Fields['volume'][(Fields['volume'].index.get_level_values('dat') >= TradingDays[TradingDays.index(datadate)-60])].groupby(['ticker','exchange']).mean()
    TodaysVolume = Fields['volume'][Fields['volume'].index.get_level_values('dat') == datadate].reset_index('dat').drop(columns=['dat'])['volume']

    dataday2 = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=2)
    DailyPCTChange = dataday2.sort_index(level=['ticker','exchange','dat']).groupby(['ticker','exchange']).pct_change().dropna().reset_index('dat').drop(columns=['dat'])['adj_close']

    Data5 = datadateslice(Fields['adj_close'],End_Date=datadate,Days=365*5)
    PeaksTroughs = _PeakTroughs(Data5)
    #PeaksTroughs = Fields[['Peak 5 Year','Trough 5 Year']][Fields.index.get_level_values('dat') == datadate].reset_index('dat')[['Peak 5 Year','Trough 5 Year']]
    #PeaksTroughs = PeaksTroughs.rename(columns={'Peak 5 Year':'Peak','Trough 5 Year':'Trough'})
    PeaksTroughs['Peak']['VBIV']['NASDAQ'] = numpy.nan
    PeaksTroughs['Trough']['VBIV']['NASDAQ'] = numpy.nan
    PotentialLoss = (TodaysAdjustedClose-(PeaksTroughs['Trough']))/(TodaysAdjustedClose)
    PotentialGain = (PeaksTroughs['Peak']*.9-TodaysAdjustedClose)/TodaysAdjustedClose

    Fields = Fields.reset_index()
    Fields['date'] = Fields['dat']
    Fields.set_index(['ticker', 'exchange'], inplace=True)
    Fields['Peak'] = PeaksTroughs['Peak']
    Fields['Trough'] = PeaksTroughs['Trough']
    Fields.reset_index(inplace=True)
    Fields.set_index(['ticker', 'exchange', 'dat'], inplace=True)
    Fields['Price Position'] = (Fields['adj_close'] - Fields['Trough']) / (Fields['Peak'] - Fields['Trough'])

    if len(PeaksTroughs[(~PeaksTroughs['Peak'].isna()) & (~PeaksTroughs['Trough'].isna())]) > 0:
        PeakTrough = Fields[(Fields['Price Position'] <= .1) & (Fields['Price Position'] >= -.69) | (Fields['Price Position'] >= .9) & (Fields['Price Position'] <= 1.69)]
        PeakTrough['diff'] = PeakTrough['Price Position'].groupby(['ticker', 'exchange']).diff().abs()
        PeakTrough['rdiff'] = PeakTrough['Price Position'].groupby(['ticker', 'exchange']).diff()
        temp = PeakTrough.groupby(['ticker', 'exchange']).head(1)
        temp = temp.append(PeakTrough[PeakTrough['diff'] >= .8]).sort_index(level=['ticker', 'exchange', 'dat'])
        temp['Date diff'] = pandas.to_datetime(temp['date']).groupby(['ticker','exchange']).diff().dt.days
        AveragePeakTroughTurnover = temp[temp['rdiff'] > 0]['Date diff'].groupby(['ticker','exchange']).mean()
        PeakTroughCycles = temp.groupby(['ticker','exchange']).size()

        MostRecentPeak = temp[temp['Price Position'] >= .9].groupby(['ticker','exchange']).tail(1).reset_index('dat')['date']
        MostRecentTrough = temp[temp['Price Position'] <= .1].groupby(['ticker','exchange']).tail(1).reset_index('dat')['date']
        CurrentCycleDuration = (datetime.strptime(datadate,'%Y-%m-%d')-pandas.to_datetime(MostRecentTrough)).dt.days
    else:
        AveragePeakTroughTurnover = numpy.nan
        PeakTroughCycles = numpy.nan
        MostRecentPeak = numpy.nan
        MostRecentTrough = numpy.nan
        CurrentCycleDuration = numpy.nan

    if first_calc:
        data30 = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=60)
        RSI = _RSI(data30, datadate,first_calc, Period=60)
        Portfolio['Max Drawdown'] = None
    else:
        prevgain = YSignals['prevgain']
        prevloss = YSignals['prevloss']
        data2 = datadateslice(Fields['adj_close'], End_Date=datadate, Trading_Days=2)
        RSI = _RSI(data2, datadate,first_calc, prevgain, prevloss, Period=60)
    prevgain = RSI['diffgain']
    prevloss = RSI['diffloss']

    Pipe = pandas.DataFrame(data={"Today's Close":TodaysClose,"Today's Adjusted Close":TodaysAdjustedClose,'Action':'Nothing','Allocation':.05,'Stop Limit':0.01,'Holding Period':'Infinite'})
    Pipe['prevgain'] = prevgain
    Pipe['prevloss'] = prevloss
    Pipe['Peak'] = PeaksTroughs['Peak']
    Pipe['Trough'] = PeaksTroughs['Trough']
    Pipe['Potential Gain'] = PotentialGain
    Pipe['Potential Loss'] = PotentialLoss
    Pipe['Total Distance'] = ((Pipe['Peak']-Pipe['Trough'])/Pipe['Trough']).round(2)
    Pipe['Price Position'] = (Pipe["Today's Adjusted Close"]-Pipe['Trough'])/(Pipe['Peak']-Pipe['Trough'])
    Pipe['Gain to Loss Ratio'] = PotentialGain/PotentialLoss
    Pipe['Most Recent Trough'] = MostRecentTrough
    Pipe['Most Recent Peak'] = MostRecentPeak
    Pipe['Average Peak Trough Turnover'] = AveragePeakTroughTurnover
    Pipe['Current Cycle Duration'] = CurrentCycleDuration
    Pipe['Cycle Position'] = Pipe['Current Cycle Duration']/Pipe['Average Peak Trough Turnover']
    Pipe['Peak Trough Cycles'] = PeakTroughCycles
    Pipe['Recent Peak or Trough'] = None
    Pipe['Recent Peak or Trough'][Pipe['Most Recent Peak'] >= Pipe['Most Recent Trough']] = 'Peak'
    Pipe['Recent Peak or Trough'][Pipe['Most Recent Trough'] >= Pipe['Most Recent Peak']] = 'Trough'
    Pipe['Past Support or Ressitance'] = Fields[(Fields['Price Position'] >= 1.1) | (Fields['Price Position'] <= -.1)].groupby(['ticker','exchange']).size()/Fields['Price Position'].groupby(['ticker','exchange']).size()
    Pipe['Past Support or Ressitance'][Pipe['Past Support or Ressitance'].isna()] = 0
    Pipe['Largest PCT Change'] = Fields['adj_close'].pct_change().groupby(['ticker', 'exchange']).max()
    Pipe['Total Size'] = Fields['adj_close'].groupby(['ticker','exchange']).size()
    Pipe['Daily PCT Change'] = DailyPCTChange
    Pipe["Today's Volume"] = TodaysVolume
    Pipe['90 Day Average Volume'] = Volume90
    Pipe['RSI'] = RSI['RSI']
    DurrationScore = ((20/600)*Pipe['Current Cycle Duration']).round(0)
    DurrationScore[DurrationScore > 20] = 20
    PricePositionScore = ((30/60)*((Pipe['Price Position'])*100)).round(0)
    PricePositionScore[PricePositionScore > 30] = 30
    RSIScore = ((20/100)*RSI['RSI']).round(0)
    RSIScore[RSIScore > 20] = 20
    DistanceScore = (30-((30/2.5)*Pipe['Total Distance'])).round(0)
    DistanceScore[DistanceScore > 30] = 30
    Pipe['Success Score'] = DurrationScore + PricePositionScore + DistanceScore + RSIScore
    SuccessScore = ((35/100)*(Pipe['Success Score'])).round(0)
    SuccessScore[SuccessScore > 35] = 35
    GainScore = ((30/75)*((Pipe['Potential Gain'])*100)).round(0)
    GainScore[GainScore > 30] = 30
    LossScore = (35-((35/60)*((Pipe['Potential Loss'])*100))).round(0)
    LossScore[LossScore < 0] = 0
    Pipe['Test Allocation'] = (((SuccessScore+LossScore+GainScore)/100)*.1)
    Pipe['Duration Score'] = DistanceScore
    Pipe['Price Position Score'] = PricePositionScore
    Pipe['RSI Score'] = RSIScore
    Pipe['Distance Score'] = DistanceScore
    Pipe['Alloc Success Score'] = SuccessScore
    Pipe['Gain Score'] = GainScore
    Pipe['Loss Score'] = LossScore
    InPortfolio = Pipe.index.isin(Portfolio.index)
    if len(Portfolio.index) > 0:
        if 'Peak on Buy' in Portfolio.columns.values:
            Pipe['Peak'][(Pipe['Peak'].isna()) & InPortfolio] = Portfolio['Peak on Buy'][Portfolio.index.isin(Pipe.index)][(Pipe['Peak'][InPortfolio].isna())]
        if 'Trough on Buy' in Portfolio.columns.values:
            Pipe['Trough'][(Pipe['Trough'].isna()) & InPortfolio] = Portfolio['Trough on Buy'][Portfolio.index.isin(Pipe.index)][(Pipe['Trough'][InPortfolio].isna())]
    if first_calc is False:
        if 'Buy Price' in Portfolio.columns.values:
            Pipe['Total Gain'] = ((Pipe["Today's Close"][InPortfolio] - Portfolio['Buy Price'])/Portfolio['Buy Price'])
        else:
            Pipe['Total Gain'] = numpy.nan
    else:
        Pipe['Total Gain'] = 0
    Gain = Pipe['Total Gain'] >= .01
    #Potential Gain and Loss & RSI & Current Cycle Duration & Price Position & Total Distance & Cycle Position & Cycle Duration Should incorpate into a weighted average also should change RSI period to longer
    BuyFilter = (Pipe['Peak Trough Cycles'] >= 3) & (Pipe['Recent Peak or Trough'] == 'Trough')
    SellFilter = (Pipe['Trough'].isna()) | (Pipe['Peak'].isna()) | (Pipe["Today's Adjusted Close"] >= Pipe['Peak']*.9)
    Pipe['Stop Limit'][BuyFilter & ((Pipe["Trough"]*.9) > (Pipe["Today's Adjusted Close"] * .7))] = Pipe["Trough"]*.9
    Pipe['Stop Limit'][BuyFilter & ((Pipe["Today's Adjusted Close"] * .7) >= Pipe["Trough"]*.9)] = Pipe["Today's Adjusted Close"] * .7
    Pipe["Quantity"] = ((net_worth*.05)/Pipe["Today's Adjusted Close"]).round(0)
    Pipe['Action'][BuyFilter] = 'buy'
    Pipe['Action'][SellFilter] = 'close'
    Pipe['Action'][Pipe.index.isin(Pipe.head(1000).index)] = 'buy'
    Pipe.sort_values('Potential Gain',ascending=False,inplace=True)
    return Pipe
Backtest(UpTrend,End_Date='2017-04-16',Start_Date='2016-04-09',StartingOffset=365,SC=100000,BacktestTitle='PeakTroughP60MKSTPLMT')
