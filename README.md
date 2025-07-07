# Stock Market Data Processing with Medallion Architecture

## Project Overview

This project implements a **Medallion Architecture** for processing stock market data using PySpark. The system ingests data from multiple sources (MySQL and MongoDB), processes it through Bronze, Silver, and Gold layers, and produces aggregated analytics at various time granularities (hourly, daily, monthly, and quarterly).

### Architecture Overview

```
Bronze Layer (Raw Data) → Silver Layer (Cleaned & Structured) → Gold Layer (Aggregated Analytics)
```

- **Bronze Layer**: Raw transaction data from MongoDB, company info and market index from MySQL
- **Silver Layer**: Cleaned, enriched, and structured hourly stock data
- **Gold Layer**: Business-ready aggregations (daily, monthly, quarterly summaries)

## Prerequisites

- Python 3.7+
- Apache Spark 3.x
- MySQL 8.0+
- MongoDB 4.4+
- Java 8 or 11 (required for Spark)

### Required Python Packages

```bash
pip install pyspark
pip install pymongo
pip install mysql-connector-python
pip install python-dotenv
pip install pyyaml
```

## Project Structure

```
stock-market-project/
├── src/
│   ├── data_processor.py    # Core data processing logic
│   └── main.py              # Main orchestration script
├── data/
│   ├── stock_market.sql     # MySQL database dump
│   └── transactions.json    # MongoDB transaction data
├── output/                  # Generated CSV outputs
├── .env.example            # Environment variables template
├── config.yaml             # Configuration file
└── README.md              # This file
```

## Setup Instructions

### 1. MySQL Database Setup

#### Step 1.1: Create Database
```bash
# Login to MySQL
mysql -u root -p

# Create database
CREATE DATABASE stock_market;
EXIT;
```

#### Step 1.2: Import Data
```bash
# Import the SQL dump
mysql -u root -p stock_market < data/stock_market.sql
```

#### Step 1.3: Verify Data Import
```sql
# Login to MySQL
mysql -u root -p

# Check tables
USE stock_market;
SHOW TABLES;

# Expected output:
# +------------------------+
# | Tables_in_stock_market |
# +------------------------+
# | company_info          |
# | market_index          |
# +------------------------+

# Verify data
SELECT COUNT(*) FROM company_info;
SELECT COUNT(*) FROM market_index;
```

### 2. MongoDB Setup

#### Step 2.1: Start MongoDB Service
```bash
# On Linux/Mac
sudo systemctl start mongod
# or
brew services start mongodb-community

# On Windows
# Start MongoDB from Services
```

#### Step 2.2: Import Transaction Data
```bash
# Import JSON data
mongoimport --db stock_market --collection transactions --file data/transactions.json --jsonArray
```

#### Step 2.3: Verify Data Import
```bash
# Open MongoDB shell
mongosh

# Check data
use stock_market
db.transactions.countDocuments()
db.transactions.findOne()
```

### 3. Environment Configuration

#### Step 3.1: Copy Environment Template
```bash
cp .env.example .env
```

#### Step 3.2: Edit .env File
```bash
# MySQL Configuration
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_mysql_password
MYSQL_DATABASE=stock_market

# MongoDB Configuration
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DATABASE=stock_market
MONGO_COLLECTION=transactions

# Spark Configuration
MYSQL_CONNECTOR_PATH=/path/to/mysql-connector-java-8.0.33.jar

# Output Configuration
OUTPUT_PATH=./output
```

#### Step 3.3: Download MySQL Connector JAR
```bash
# Download MySQL Connector JAR (required for Spark)
wget https://dev.mysql.com/get/Downloads/Connector-J/mysql-connector-java-8.0.33.jar

# Place it in a known location and update MYSQL_CONNECTOR_PATH in .env
```

### 4. Configuration File Setup

Create or verify `config.yaml`:
```yaml
# MySQL Configuration
mysql_url: "jdbc:mysql://${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}"
mysql_user: "${MYSQL_USER}"
mysql_password: "${MYSQL_PASSWORD}"
mysql_connector_path: "${MYSQL_CONNECTOR_PATH}"

# MongoDB Configuration
mongodb_uri: "mongodb://${MONGO_HOST}:${MONGO_PORT}/"
mongo_db: "${MONGO_DATABASE}"
mongo_collection: "${MONGO_COLLECTION}"

# Table Names
company_info_table: "company_info"
market_index_table: "market_index"

# Output Configuration
output_path: "${OUTPUT_PATH}"
```

## Running the Project

### Step 1: Ensure All Services Are Running
```bash
# Check MySQL
mysql -u root -p -e "SELECT 1"

# Check MongoDB
mongosh --eval "db.runCommand({ ping: 1 })"
```

### Step 2: Run the Main Script
```bash
# Navigate to project directory
cd stock-market-project

# Run the main script
python src/main.py
```

### Expected Console Output
```
Initializing Spark Session...
Loading data from MySQL...
Loading data from MongoDB...
Processing hourly transactions...
Hourly Data Preview:
+-------------------+------+---------------+---------+------+-------------+
|datetime           |ticker|company_name   |avg_price|volume|market_index |
+-------------------+------+---------------+---------+------+-------------+
|2024-01-01 00:00:00|AAPL  |Apple Inc.     |175.74   |560   |10234.56     |
|2024-01-01 00:00:00|AMZN  |Amazon.com Inc.|145.35   |350   |10234.56     |
...

Processing daily aggregations...
Daily Data Preview:
...

Processing monthly aggregations...
Monthly Data Preview:
...

Processing quarterly aggregations...
Quarterly Data Preview:
...
```

## Data Processing Pipeline

### Bronze Layer (Raw Data Ingestion)

#### 1. MySQL Data Loading
```python
# Company Information
company_info = spark.read.jdbc(
    url=mysql_url,
    table="company_info",
    properties={"user": mysql_user, "password": mysql_password}
)

# Market Index Data
market_index = spark.read.jdbc(
    url=mysql_url,
    table="market_index",
    properties={"user": mysql_user, "password": mysql_password}
)
```

#### 2. MongoDB Data Loading
```python
# Transaction Data
transactions = spark.read.format("mongo").load()
# Explode nested transactions array
transactions = transactions.select(
    explode("transactions").alias("transaction")
).select(
    "transaction.timestamp",
    "transaction.ticker",
    "transaction.action",
    "transaction.shares",
    "transaction.price"
)
```

### Silver Layer (Data Transformation)

#### Hourly Aggregation Logic
```python
# Group by hour and ticker
# Calculate volume (sum of shares)
# Calculate weighted average price
hourly_data = transactions.groupBy(
    date_trunc("hour", "timestamp").alias("datetime"),
    "ticker"
).agg(
    sum("shares").alias("volume"),
    (sum(col("shares") * col("price")) / sum("shares")).alias("avg_stock_price")
)

# Join with company info and market index
final_hourly = hourly_data.join(company_info, "ticker").join(market_index_hourly, "datetime")
```

### Gold Layer (Business Aggregations)

#### Daily Aggregation
- Groups hourly data by day
- Calculates daily average price and total volume
- Maintains market index average

#### Monthly Aggregation
- Groups hourly data by month
- Calculates monthly average price and total volume
- Formats output as "YYYY-MM"

#### Quarterly Aggregation
- Groups hourly data by quarter
- Calculates quarterly metrics
- Formats output as "YYYY Q#"

## Output Files

All output files are saved in the `OUTPUT_PATH` directory:

```
output/
├── hourly_stock_data.csv      # Hourly aggregated data
├── daily_stock_data.csv       # Daily summaries
├── monthly_stock_data.csv     # Monthly summaries
└── quarterly_stock_data.csv   # Quarterly summaries
```

### Output Schema

#### Hourly Data
| Column | Type | Description |
|--------|------|-------------|
| datetime | timestamp | Hour timestamp (YYYY-MM-DD HH:00:00) |
| ticker | string | Stock ticker symbol |
| company_name | string | Full company name |
| avg_price | decimal(10,2) | Weighted average price |
| volume | integer | Total shares traded |
| market_index | decimal(10,2) | Market index value |

#### Daily/Monthly/Quarterly Data
Similar schema with appropriate time granularity in the date column.

## Troubleshooting

### Common Issues

#### 1. Spark Session Initialization Error
```
Error: Java gateway process exited before sending its port number
```
**Solution**: Ensure Java is installed and JAVA_HOME is set:
```bash
java -version
export JAVA_HOME=/path/to/java
```

#### 2. MySQL Connection Error
```
Error: Communications link failure
```
**Solution**: 
- Verify MySQL is running: `sudo systemctl status mysql`
- Check credentials in .env file
- Ensure MySQL allows connections on specified port

#### 3. MongoDB Connection Error
```
Error: ServerSelectionTimeoutError
```
**Solution**:
- Verify MongoDB is running: `sudo systemctl status mongod`
- Check MongoDB connection string in config

#### 4. Missing MySQL Connector JAR
```
Error: No suitable driver found for jdbc:mysql://
```
**Solution**:
- Download the MySQL Connector JAR
- Update MYSQL_CONNECTOR_PATH in .env
- Ensure the path is absolute, not relative

### Verification Steps

1. **Verify Spark Installation**:
```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("test").getOrCreate()
spark.sql("SELECT 1").show()
```

2. **Test Database Connections**:
```python
# Test MySQL
import mysql.connector
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="your_password",
    database="stock_market"
)

# Test MongoDB
from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017/")
db = client.stock_market
print(db.transactions.count_documents({}))
```

## Performance Optimization

### Spark Configuration
For better performance with large datasets:

```python
spark = SparkSession.builder \
    .appName("StockDataWarehouse") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
```

### Data Partitioning
Consider partitioning output data by date for better query performance:
```python
df.write.partitionBy("year", "month").mode("overwrite").csv(output_path)
```

## License and Usage

Copyright © 2025 Zimeng Lyu. All rights reserved.

This project is for educational purposes as part of RIT DSCI-644 course. Usage as teaching material requires explicit permission from the author.