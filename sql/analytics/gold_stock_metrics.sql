CREATE OR REPLACE TABLE `your-project.your_dataset.gold_stock_metrics`
PARTITION BY DATE(trade_date)
CLUSTER BY ticker AS

WITH daily_calculations AS (
  SELECT
    Date AS trade_date,
    Open AS open_price,
    High AS high_price,
    Low AS low_price,
    Close AS close_price,
    Volume,
    -- Calculate Daily Price Change
    (Close - LAG(Close) OVER (ORDER BY Date)) / LAG(Close) OVER (ORDER BY Date) * 100 AS daily_return_pct,
    -- 7-Day Moving Average for Trend Lines
    AVG(Close) OVER (ORDER BY Date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS moving_avg_7d
  FROM 
    `your-project.your_dataset.raw_stock_data`
)

SELECT
  *,
  -- Add a simple Volatility Flag
  CASE 
    WHEN ABS(daily_return_pct) > 3 THEN 'High Volatility'
    ELSE 'Stable'
  END AS volatility_status,
  -- Record metadata for data lineage
  CURRENT_TIMESTAMP() AS last_updated_at
FROM 
  daily_calculations
WHERE 
  trade_date IS NOT NULL;
