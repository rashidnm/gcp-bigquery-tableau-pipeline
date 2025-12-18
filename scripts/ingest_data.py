import yfinance as yf
from google.cloud import bigquery
import pandas as pd

import os
from google.cloud import bigquery

# Tell Python where your bot's "key" is
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "config/my-data-portfolio-481622-400a944d1804.json"

client = bigquery.Client()
# Now 'client' acts as your Service Account bot!

# 1. Fetch Data
ticker = "TSLA"
data = yf.download(ticker, period="1mo", interval="1d")
data.reset_index(inplace=True)

# 2. BigQuery Setup
# client = bigquery.Client()
table_id = "your-project.your_dataset.raw_stock_data"

# 3. Load to BigQuery
job = client.load_table_from_dataframe(data, table_id)
job.result()  # Wait for the job to complete
print(f"Loaded {len(data)} rows to {table_id}")
