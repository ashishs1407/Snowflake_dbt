from datetime import datetime

def read_json_file():
    import pandas as pd
  
    #Load the JSON file into Snowflake
    df_json = pd.read_json("//opt//airflow//data//data.json")
    return df_json
    


def historic():
    import requests
    h_date = datetime(2022,1,1).date()
    base = 'EUR'

    url = f"https://api.apilayer.com/exchangerates_data/{h_date}?base={base}"

    payload = {}
    headers= {
    "apikey": "gtvh3qlwUEQtBu2PzFUjBRRySc87tALx"
    }

    response = requests.request("GET", url, headers=headers, data = payload)

    status_code = response.status_code
    result = response.text

    return result




def stock_data():
    import yfinance as yahooFinance
    obj = yahooFinance.Ticker('TSLA')
    df = obj.history(period="max")
    df.reset_index(inplace=True)

    # Convert the DataFrame to a JSON object and return it

    df = df.to_json()
    # Save the JSON object to a file named TSLA.json
    with open('/opt/airflow/data/TSLA.json', 'w') as f:
        f.write(df)

import yfinance as yahooFinance
import pyarrow.parquet as pq
import pyarrow as pa

def stock_data_par():
  obj = yahooFinance.Ticker('TSLA')
  df = obj.history(period="max")
  df.reset_index(inplace=True)

  # Convert the DataFrame to a Parquet object and return it
  table = pa.Table.from_pandas(df)
  pq.write_table(table, '/opt/airflow/data/TSLA.parquet')
  

















