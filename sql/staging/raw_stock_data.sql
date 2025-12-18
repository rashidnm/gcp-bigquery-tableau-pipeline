CREATE OR REPLACE TABLE `my-data-portfolio-481622.stock_market_data.raw_stock_data` (
  trade_date DATE,
  ticker STRING,
  open_price FLOAT64,
  close_price FLOAT64,
  volume INT64
)
PARTITION BY trade_date; -- Highly recommended for cost optimization
