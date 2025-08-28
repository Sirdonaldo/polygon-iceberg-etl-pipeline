# polygon-iceberg-etl-pipeline

**Homework assignment for DataExpert.io Data Engineering Bootcamp (Version 5)**

## Project Overview

This project demonstrates how to build an ETL pipeline using the Polygon.io API and Apache Iceberg with Tabular.  
It fetches historical stock price data, transforms it into a compatible format, and loads it into an Iceberg table.

---

## Technologies Used

- **Python**
- **PyIceberg**
- **Polygon.io API**
- **Tabular REST Catalog**
- **AWS Secrets Manager**
- **dotenv**

---

## Key Features

- Fetches MAANG stock price data from Polygon.io
- Parses and transforms JSON data for Iceberg format
- Writes transformed data to Apache Iceberg tables via Tabular
- Uses secure credential handling with `.env` and AWS Secrets Manager

---

## Project Structure

```bash
├── .gitignore               # Ignore cache, .env, and virtual environment files
├── README.md                # You're reading it!
├── stock_prices.py          # Main ETL pipeline script


## Setup and How to Run
# 1. Clone the repo
git clone https://github.com/Sirdonaldo/polygon-iceberg-etl-pipeline.git
cd polygon-iceberg-etl-pipeline

# 2. Set up environment variables
# Either create a .env file or use AWS Secrets Manager to store:
# - POLYGON_API_KEY
# - TABULAR_CREDENTIAL
# - CATALOG_NAME

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the ETL script
python stock_prices.py

