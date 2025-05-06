import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
import logging

# ----------------- CONFIG -----------------
API_KEY_1 = "59139c4865706057d2a1fcafea08d583"  # 🛑 Replace this
API_KEY_2 = "your_weatherapi_key_here"  # 🛑 Replace this


DB_CONFIG = {
    "username": "postgres",
    "password": "1234",  # 🛑 Replace this
    "host": "localhost",
    "port": 5432,
    "database": "test_1"    # 🛑 Replace this
}

TABLE_NAME = "stock_exchage_header"

# ----------------- LOGGING -----------------
logging.basicConfig(
    filename=r"C:\Users\User\Documents\practice\api_data_extraction\stock_exchage_header.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ----------------- FLATTEN FUNCTION -----------------
flatdata=[]
def flattendata_stock_exchanges(data):
    
    for i in range(0,65):
     flatdata.append({
      "name":data['data'][i]['name'],
      "acronym":data['data'][i]['acronym'],
      "country": data['data'][i]['country'],
      "country_code": data['data'][i]['country_code'],
      "city": data['data'][i]['city'],
      "website": data['data'][i]['website'],
      "timezone": data['data'][i]['timezone']['timezone'],
      "timezone_abbr":data['data'][i]['timezone']['abbr'],
      "timezone_abbr_dst": data['data'][i]['timezone']['abbr_dst'],
      "currency_code": data['data'][i]['currency']['code'],
      "currency_symbol": data['data'][i]['currency']['symbol'],
      "currency_name": data['data'][i]['currency']['name']

     })
    return flatdata

# ----------------- MAIN ETL LOGIC -----------------
def fetch_and_flatten_stockdata():
 
    
    try:
            url = f"http://api.marketstack.com/v1/exchanges?access_key={API_KEY_1}"
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                stock_exchanges = flattendata_stock_exchanges(data)
                
                logging.info(f"Fetched weather for ")
            else:
                logging.warning(f"Failed to fetch for : {response.status_code}")
    except Exception as e:
            logging.error(f"Error fetching weather for : {e}")
    
    return pd.DataFrame(stock_exchanges)

print(fetch_and_flatten_stockdata())

url = f"http://api.marketstack.com/v1/exchanges?access_key={API_KEY_1}"
response = requests.get(url)
data = response.json()
print(flattendata_stock_exchanges(data))

def load_to_postgresql(df):
    try:
        db_url = f"postgresql://{DB_CONFIG['username']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(db_url)
        df.to_sql(TABLE_NAME, engine, if_exists='append', index=False , method='multi',
    chunksize=100 )
        logging.info(f"Loaded {len(df)} rows to PostgreSQL table '{TABLE_NAME}'")
    except Exception as e:
        logging.error(f"Failed to load to PostgreSQL: {e}")

# ----------------- RUN SCRIPT -----------------
if __name__ == "__main__":
    logging.info("=====  ETL STARTED =====")
    df = fetch_and_flatten_stockdata()
    if not df.empty:
        load_to_postgresql(df)
    else:
        logging.warning("No data fetched — skipping load.")
    logging.info("=====  ETL FINISHED =====")
