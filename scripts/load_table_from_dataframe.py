import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# 1. Setup Credentials & Client
KEY_PATH = "config/my-data-portfolio-481622-400a944d1804.json"
PROJECT_ID = "my-data-portfolio-481622"

credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

# 2. Create sample data (or use your stock API data here)
data = {
    "trade_date": ["2025-12-18", "2025-12-17"],
    "ticker": ["TSLA", "TSLA"],
    "open_price": [250.50, 248.00],
    "close_price": [255.20, 251.10],
    "volume": [1200000, 1150000]
}
df = pd.DataFrame(data)

# Ensure date column is the correct type
df['trade_date'] = pd.to_datetime(df['trade_date']).dt.date

# 3. Define Table ID and Config
table_id = f"{PROJECT_ID}.stock_market_data.raw_stock_data"

job_config = bigquery.LoadJobConfig(
    # WRITE_APPEND adds new data; WRITE_TRUNCATE replaces the table
    write_disposition="WRITE_APPEND",
    # BigQuery will try to guess types, but defining them in SQL first is safer
    autodetect=True, 
)

# 4. Push to BigQuery
job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
job.result()  # Wait for the job to complete

print(f"Successfully loaded {len(df)} rows to {table_id}")
