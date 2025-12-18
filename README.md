# End-to-End Stock Market Analytics Pipeline (GCP + Tableau)

## 🎯 Overview
Automated data pipeline that ingests daily stock market data into **Google BigQuery**, performs SQL transformations, and visualizes volatility and sentiment in **Tableau**.

## 🏗️ Architecture
- **Infastructure:** Google Cloud Platform (GCP)
- **Ingestion:** Python (yfinance API)
- **Warehouse:** BigQuery (Partitioned by Date)
- **Visualization:** Tableau Public

## 🛠️ Data Engineering Highlights
- **Cost Optimization:** Implemented table partitioning to reduce query scan costs.
- **Automation:** Scripted ingestion for hands-off data updates.
- **Scalability:** Designed with a Medallion Architecture (Raw -> Staging -> Gold).
