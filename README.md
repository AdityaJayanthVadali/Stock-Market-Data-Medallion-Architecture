# Stock Market Data Processing Project

This project processes stock market data from multiple sources using PySpark, combining data from MySQL and MongoDB databases.

## Setup Instructions

### 1. MySQL Database Setup

1. Ensure you have MySQL installed on your machine
2. Open your terminal and login to MySQL:
   ```bash
   mysql -u your_username -p
   ```
3. Create a new database:
   ```sql
   CREATE DATABASE stock_market;
   ```
4. Exit MySQL and import the data:
   ```bash
   mysql -u your_username -p stock_market < data/stock_market.sql
   ```
5. Verify the data is loaded:
   ```sql
   USE stock_market;
   SHOW TABLES;
   ```
   You should see tables: company_info, market_index

### 2. MongoDB Setup

1. Ensure MongoDB is installed and running
2. Import the transaction data:
   ```bash
   mongoimport --db stock_market --collection transactions --file data/transactions.json --jsonArray
   ```
3. Verify the data:
   ```bash
   mongosh
   use stock_market
   db.transactions.find().limit(1)
   ```

### 3. Environment Setup

1. Create a Python virtual environment (optional):
   ```bash
   python -m venv venv
   source venv/bin/activate 
   ```


2. Copy the environment template:
   ```bash
   cp .env.example .env
   ```

3. Edit `.env` with your database credentials and paths:


### 4. Running the Code

1. Implement the required methods in `src/data_processor.py` and `src/main.py`

2. Run the main script:
   ```bash
   python src/main.py
   ```

3. Check the output:
   - The processed data will be saved to the path specified in OUTPUT_PATH
   - The console will show progress and data samples at each step

## Project Structure

- `src/`
  - `data_processor.py`: Implement your data processing methods here
  - `main.py`: Main script orchestrating the data processing pipeline
- `data/`
  - `stock_market.sql`: MySQL database dump
  - `transactions.json`: MongoDB transaction data
- `.env.example`: Template for environment variables
- `config.yaml`: Configuration file for environment

## License and Usage Restrictions

Copyright © 2025 Zimeng Lyu. All rights reserved.

This project was developed by Zimeng Lyu for the RIT DSCI-644 course, originally designed for GitLab CI/CD integration. It is shared publicly for portfolio and learning purposes.

🚫 **Usage Restriction:**  
This project **may not** be used as teaching material in any form (including lectures, assignments, or coursework) without explicit written permission from the author.

📩 For inquiries regarding usage permissions, please contact Zimeng Lyu at zimenglyu@mail.rit.edu.
