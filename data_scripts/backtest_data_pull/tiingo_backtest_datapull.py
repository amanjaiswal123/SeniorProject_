from data_scripts.get_data import tiingo_nyse_nasdaq_download
from source.Commons import upload_to_rds_table,datadateslice
from datetime import datetime
try:
    Today = str(datetime.today().date())
    data = tiingo_nyse_nasdaq_download(end_date=Today, days=21900)
    data.to_csv('tiingodata.csv')
except Exception as e:
    data.to_csv('tiingodata.csv')
    raise e
data.to_csv('tiingodata.csv')
if "dat" in data.columns.values:
    data = data.rename(columns={'dat', 'date'})
data.to_csv('tiingodata.csv')
upload_to_rds_table(data,'tiingo')
