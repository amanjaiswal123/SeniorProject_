import pandas
from datetime import datetime,timedelta
def Analyze(FileName=str):
    Backtest = pandas.read_csv(FileName).set_index(['Trade #'])
    Dates = sorted(list(Backtest['Date Sold']))
    Years = []
    for x in Dates:
        Year = x[0:4]
        if Year not in Years:
            Years.append(Year)
    StartDate = Backtest['Date Sold'][1]
    StartBalance = round(Backtest['Balance After Trade'][1]/(1+Backtest['Absolute Gains'][1]))
    for Year in Years[1:len(Years)-1]:
        Day = Year+StartDate[4:10]
        while True:
            Day = str(datetime.strptime(Day,'%Y-%m-%d') - timedelta(1))[0:10]
            if Day in Dates:
                Balance = list(Backtest[Backtest['Date Sold'] == Day]['Balance After Trade'])[0]
                if Year == Years[1]:
                    Gains = (Balance-StartBalance)/StartBalance
                else:
                    Gains = (Balance-PrevBalance)/PrevBalance
                PrevBalance = Balance

                break