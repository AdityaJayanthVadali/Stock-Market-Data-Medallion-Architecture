from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    hour,
    sum,
    avg,
    to_timestamp,
    date_trunc,
    round as spark_round,
    explode,
    abs as spark_abs,
    quarter,
    year,
    concat,
    lit,
    count,
    month,
    date_format,
)

from typing import Dict, Tuple, Optional
import glob
import shutil
import os


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    # Note: all functions are helper functions, you do not have to use them in the main code

    # Ingest Data From MYSQL and MongoDB
    def ingest_data(
        self, config: Dict[str, str]
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        # Load company_info from MySQL
        company_info = (
            self.spark.read.format("jdbc")
            .option("url", config["mysql_url"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", config["company_info_table"])
            .option("user", config["mysql_user"])
            .option("password", config["mysql_password"])
            .load()
        )

        # Load market_index from MySQL
        market_index = (
            self.spark.read.format("jdbc")
            .option("url", config["mysql_url"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", config["market_index_table"])
            .option("user", config["mysql_user"])
            .option("password", config["mysql_password"])
            .load()
        )

        # Load transaction_info from MongoDB
        transaction_info = (
            self.spark.read.format("com.mongodb.spark.sql.DefaultSource")
            .option("uri", config["mongodb_uri"])
            .option("database", config["mongo_db"])
            .option("collection", config["mongo_collection"])
            .load()
        )

        # Explode transactions
        transactions = transaction_info.withColumn(
            "transaction", explode("transactions")
        ).select(
            col("timestamp"),
            col("transaction.ticker"),
            col("transaction.action"),
            col("transaction.shares"),
            col("transaction.price"),
        )

        return company_info, market_index, transactions

    def aggregate_hourly_transactions(self, transactions: DataFrame) -> DataFrame:
        """
        Aggregate transactions by hour, calculating volume and weighted average price
        Note: this is helper function, you do not have to
        # Function breakdown:
        # 1. Groups transactions by hour and ticker using date_trunc
        # 2. For each group, calculates:
        #    - Total volume: sum of all shares traded
        #    - Weighted average price: (sum of shares * price) / total shares
        # 3. Returns DataFrame with columns: datetime, ticker, volume, avg_stock_price
        """
        return transactions.groupBy(
            date_trunc("hour", "timestamp").alias("datetime"), "ticker"
        ).agg(
            sum("shares").alias("volume"),
            (sum(col("shares") * col("price")) / sum("shares")).alias(
                "avg_stock_price"
            ),
        )

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def process_hourly_data(
        self,
        transactions: DataFrame,
        company_info: DataFrame,
        market_index: DataFrame,
        round_decimals: Optional[int] = None,
        output_path: Optional[str] = None,
    ) -> DataFrame:
        """
        Process hourly transactions to yield full format data, display preview, and optionally save to CSV.
        Function Breakdown:
        1. Use Provided aggregate_hourly_transactions process hourly transactions
        2. Process market_index to hourly format
        3. Join hourly_transactions, market_index_hourly and company_info
        4. If Statement for Conditional Rounding in the final data processing step
        5. Print Preview
        6. Save to CSV if Path is Provided
        """
        hourly_transactions = self.aggregate_hourly_transactions(transactions)
        market_index_hourly = market_index.select(
            date_trunc("hour", to_timestamp("timestamp")).alias("datetime"),
            col("index_value").alias("market_index"),
        )
        joined_data = hourly_transactions.join(company_info, "ticker").join(
            market_index_hourly, "datetime"
        )
        if round_decimals is not None:
            processed_data = joined_data.select(
                date_format("datetime", "yyyy-MM-dd HH:mm:ss").alias("datetime"),
                "ticker",
                col("company_name"),
                spark_round("avg_stock_price", round_decimals).alias("avg_price"),
                "volume",
                spark_round("market_index", round_decimals).alias("market_index"),
            ).orderBy("datetime", "ticker")
        else:
            processed_data = joined_data.select(
                date_format("datetime", "yyyy-MM-dd HH:mm:ss").alias("datetime"),
                "ticker",
                col("company_name"),
                col("avg_stock_price").alias("avg_price"),
                "volume",
                col("market_index"),
            ).orderBy("datetime", "ticker")
        print("Hourly Data Preview:")
        processed_data.show(5)
        if output_path:
            self.save_to_csv(processed_data, output_path, "hourly_stock_data.csv")
            print(f"Hourly data saved to: {output_path}/hourly_stock_data.csv")
        return processed_data

    def process_daily_data(self, hourly_data: DataFrame, output_path: str) -> DataFrame:
        """
        Process daily stock data from hourly data, display preview, and save to CSV.
        Function Breakdown:
        1. Take the Hourly Data and perform aggregation
        2. Display Preview
        3. Save to CSV
        """
        daily_data = (
            hourly_data.groupBy(
                date_trunc("day", to_timestamp("datetime")).alias("datetime"),
                "ticker",
                "company_name",
            )
            .agg(
                avg("avg_price").alias("avg_price"),
                sum("volume").alias("volume"),
                avg("market_index").alias("market_index"),
            )
            .select(
                date_format("datetime", "yyyy-MM-dd").alias("date"),
                "ticker",
                "company_name",
                spark_round("avg_price", 2).alias("avg_price"),
                "volume",
                spark_round("market_index", 2).alias("market_index"),
            )
            .orderBy("datetime", "ticker")
        )

        # Display preview of the processed data
        print("Daily Data Preview:")
        daily_data.show(5)

        # Save to CSV if output path is provided
        self.save_to_csv(daily_data, output_path, "daily_stock_data.csv")
        print(f"Daily data saved to: {output_path}/daily_stock_data.csv")

        return daily_data

    def process_monthly_data(
        self, hourly_data: DataFrame, output_path: str
    ) -> DataFrame:
        """
        Process Monthly stock data from hourly data, display preview, and save to CSV.
        Function Breakdown:
        1. Take the Hourly Data and perform aggregation
        2. Display Preview
        3. Save to CSV
        """
        monthly_data = (
            hourly_data.groupBy(
                date_trunc("month", to_timestamp("datetime")).alias("datetime"),
                "ticker",
                "company_name",
            )
            .agg(
                avg("avg_price").alias("avg_price"),
                sum("volume").alias("volume"),
                avg("market_index").alias("market_index"),
            )
            .select(
                date_format("datetime", "yyyy-MM").alias("month"),
                "ticker",
                "company_name",
                spark_round("avg_price", 2).alias("avg_price"),
                "volume",
                spark_round("market_index", 2).alias("market_index"),
            )
            .orderBy("datetime", "ticker")
        )
        print("Monthly Data Preview:")
        monthly_data.show(5)

        self.save_to_csv(monthly_data, output_path, "monthly_stock_data.csv")
        print(f"Monthly data saved to: {output_path}/monthly_stock_data.csv")
        return monthly_data

    def process_quarterly_data(
        self, hourly_data: DataFrame, output_path: str
    ) -> DataFrame:
        """
        Process Quarterly stock data from hourly data, display preview, and save to CSV.
        Function Breakdown:
        1. Take the Hourly Data and perform aggregation
        2. Display Preview
        3. Save to CSV
        """
        quarterly_data = (
            hourly_data.groupBy(
                date_trunc("quarter", to_timestamp("datetime")).alias("datetime"),
                "ticker",
                "company_name",
            )
            .agg(
                avg("avg_price").alias("avg_price"),
                sum("volume").alias("volume"),
                avg("market_index").alias("market_index"),
            )
            .select(
                concat(
                    year(col("datetime")), lit(" Q"), quarter(col("datetime"))
                ).alias("quarter"),
                "ticker",
                "company_name",
                spark_round("avg_price", 2).alias("avg_price"),
                "volume",
                spark_round("market_index", 2).alias("market_index"),
            )
            .orderBy("datetime", "ticker")
        )
        print("Quarterly Data Preview:")
        quarterly_data.show(5)

        self.save_to_csv(quarterly_data, output_path, "quarterly_stock_data.csv")
        print(f"Monthly data saved to: {output_path}/quarterly_stock_data.csv")

        return quarterly_data
