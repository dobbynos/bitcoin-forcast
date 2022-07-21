# import statements
import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine
import os
uri = os.environ.get('URI')

# obtaining the data
url = 'https://api.blockchain.info/charts/transactions-per-second?timespan=all&sampled=false&metadata=false&cors=true&format=json'
resp = requests.get(url)
data = pd.DataFrame(resp.json()['values'])

# parsing the date
data['x'] = [datetime.utcfromtimestamp(x).strftime('%Y-%m-%d %H:%M:%S') for x in data['x']]
data['x'] = pd.to_datetime(data['x'])

# reading the last real date from the database
engine = create_engine(uri)
query = engine.execute('SELECT MAX(reality_date) FROM reality;')
last_reality_date = query.fetchall()[0][0]
query.close()

# reading the last prediction from the database
engine = create_engine(uri)
query = engine.execute('SELECT MIN(prediction_date), MAX(prediction_date) FROM predictions;')
prediction_date= query.fetchall()[0]
query.close()

first_prediction_date = prediction_date[0]
last_prediction_date = prediction_date[1]

if last_reality_date is None:
    date_extract = first_prediction_date

elif  last_reality_date <= last_prediction_date:
    date_extract = last_reality_date

else:
    date_extract = last_reality_date

# rounding hours to get hourly data
data['x'] = data['x'].dt.round('H')

# getting the number of transactions per hour
data_grouped = data.groupby('x').sum().reset_index()

# getting the data from the last data available in the database
data_grouped = data_grouped.loc[data_grouped['x'] >= date_extract,:]

# preparing the data to upload it to the database
upload_data = list(zip(data_grouped['x'], round(data_grouped['y'],4)))
upload_data[:3]

# inserting the data in the database
for upload_day in upload_data:
    timestamp, reality= upload_day
    result = engine.execute(f"INSERT INTO reality(reality_date, reality) VALUES('{timestamp}', '{reality}') ON CONFLICT (reality_date) DO UPDATE SET reality_date = '{timestamp}', reality= '{reality}';")
    result.close()