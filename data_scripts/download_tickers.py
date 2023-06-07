import requests
import pandas
from source.Commons import notify
from bs4 import BeautifulSoup
import cloudscraper

def  get_nasdaq_tickers_nasdaq_trader():
    nasdaq_listing = 'ftp://ftp.nasdaqtrader.com/symboldirectory/nasdaqlisted.txt'  # Nasdaq only
    nasdaq = pandas.read_csv(nasdaq_listing, sep='|')
    nasdaq = nasdaq.copy()
    # Remove test listings
    nasdaq = nasdaq[nasdaq['Test Issue'] == 'N']

    # Create New Column w/ Just Company Name
    nasdaq['Company Name'] = nasdaq['Security Name'].apply(lambda x: x.split('-')[0]) #nasdaq file uses - to separate stock type
    #df['Company Name'] =

    # Move Company Name to 2nd Col
    cols = list(nasdaq.columns)
    cols.insert(1, cols.pop(-1))
    nasdaq = nasdaq.loc[:, cols]

    # Create a few other data sets
    nasdaq_symbols = nasdaq[['Symbol','Company Name']] # Nasdaq  w/ 2 columns

    # (dataframe, filename) datasets we will put in schema & create csv
    datasets = [(nasdaq,'nasdaq-listed'), (nasdaq_symbols,'nasdaq-listed-symbols')]

    for df, filename in datasets:
        return df

def get_tickers_advfn(): #Get tickers from advfn
    tickers = pandas.DataFrame(columns=['ticker', 'exchange']) #The website has alphabetical pagination so we must iterate through the pages using the alphabet to get all the tickers
    alpabhet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u','v', 'w', 'x', 'y', 'z'] #The website has alphabetical pagination so we must iterate through the pages using the alphabet to get all the tickers
    try:
        for letter in alpabhet: #iterate through the alphabet
            scraper = cloudscraper.create_scraper()  # returns a CloudScraper instance
            source = scraper.get('https://www.advfn.com/nasdaq/nasdaq.asp?companies=' + letter.upper()).text
            soup = BeautifulSoup(source, 'lxml')
            # the html class with the data is ts0 and ts1. By iterating through these we can find all the tickers with a little more scrubbing
            for x in soup.find_all('tr', class_="ts0"): #iterating through ts0 class tags
                if len(x.text.split()) > 1: #Sometimes the data in these tags is not a ticker, usually if its shorter than 1 its not at ticker. The value of x.text.split() is the number words seperated by spaces. EX its a bat = 3
                    ticker = pandas.DataFrame.from_dict({'ticker': [x.contents[1].text], 'exchange': ["NASDAQ"]}) #create a dataframe from the ticker, all tickers are from the nasdaq.
                    tickers = tickers.append(ticker) #append that df to the overall df
            #Do the same thing for the ts1 class
            for x in soup.find_all('tr', class_="ts1"):
                if len(x.text.split()) > 1:
                    ticker = pandas.DataFrame.from_dict({'ticker': [x.text.split()[len(x.text.split()) - 1]], 'exchange': ["NASDAQ"]})
                    tickers = tickers.append(ticker)
    except Exception as e:
        print("Could not get tickers for NYSE from wikipedia")
        notify("Could not get tickers for NYSE from wikipedia")
        raise e
    return tickers #Return tickers as a dataframe

def get_tickers_wikipedia(): #Get tickers from wikipeida
    tickers = pandas.DataFrame(columns=['ticker', 'exchange'])
    alpabhet = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't','u', 'v', 'w', 'x', 'y', 'z']#The website has alphabetical pagination so we must iterate through the pages using the alphabet to get all the tickers
    try:
        for letter in alpabhet:
            try:
                source = requests.get('https://en.wikipedia.org/wiki/Companies_listed_on_the_New_York_Stock_Exchange_(' + letter.upper() + ')').text #Get html from website
            except Exception as e:
                print("Could not connect to wikipedia for NYSE tickers")
                notify("Could not connect to wikipedia for NYSE tickers")
                raise e
            soup = BeautifulSoup(source, 'lxml')
            for x in soup.find_all('a', class_="external text"): #All the tickers are in the a html tag. We can iterate through it to find the tickers
                ticker = pandas.DataFrame(data={'ticker': [x.text], 'exchange': ["NYSE"]}) #Get ticker from website. Create dataframe, these tickers are only for the NYSE
                tickers = tickers.append(ticker) #Append ticker to overall
    except Exception as e:
        print("Could not get tickers for NYSE from wikipedia")
        notify("Could not get tickers for NYSE from wikipedia")
        raise e
    return tickers #return the tickers


