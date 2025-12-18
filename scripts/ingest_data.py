import yfinance as yf
from google.cloud import bigquery
import pandas as pd

# 1. Fetch Data
ticker = "TSLA"
data = yf.download(ticker, period="1mo", interval="1d")
data.reset_index(inplace=True)

# 2. BigQuery Setup
client = bigquery.Client()
table_id = "your-project.your_dataset.raw_stock_data"

# 3. Load to BigQuery
job = client.load_table_from_dataframe(data, table_id)
job.result()  # Wait for the job to complete
print(f"Loaded {len(data)} rows to {table_id}")
