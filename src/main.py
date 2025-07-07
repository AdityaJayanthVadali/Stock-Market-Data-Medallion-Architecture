from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from typing import Dict, Tuple
from data_processor import DataProcessor
from dotenv import load_dotenv
import os
import yaml


def create_spark_session(mysql_connector_path: str, mongodb_uri: str) -> SparkSession:
    """
    Create and return a SparkSession with necessary configurations.

    :param mysql_connector_path: Path to MySQL JDBC connector JAR
    :param mongodb_uri: URI for MongoDB connection
    :return: Configured SparkSession
    """
    return (
        SparkSession.builder.appName("StockDataWarehouse")
        .config("spark.jars", mysql_connector_path)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.mongodb.input.uri", mongodb_uri)
        .getOrCreate()
    )


def main(config: Dict[str, str]) -> Tuple[DataFrame, SparkSession]:
    """
    Function Breakdown:
    1. Spark Session Created
    2. Data Ingested
    3. Data Aggregations across Levels Performed
    """
    # ----------------------------------------------------------------------------------------------------------------
    # SESSION CONFIG AND DATA INGEST
    # ----------------------------------------------------------------------------------------------------------------
    spark = create_spark_session(config["mysql_connector_path"], config["mongodb_uri"])
    data_processor = DataProcessor(spark)
    # Ingest Data From MYSQL and MongoDB and explode Transactions
    company_info, market_index, transactions = data_processor.ingest_data(config)
    # ----------------------------------------------------------------------------------------------------------------
    # HOURLY DATA
    # ----------------------------------------------------------------------------------------------------------------
    # Hourly Data Processed and Aggregated without rounding, to help process other Time Based Aggregations
    hourly_unround = data_processor.process_hourly_data(
        transactions, company_info, market_index
    )
    # Hourly Data Processed and Aggregated with 2 Decimals of rounding to ensure correct output
    hourly_data = data_processor.process_hourly_data(
        transactions,
        company_info,
        market_index,
        round_decimals=2,
        output_path=config["output_path"],
    )
    # ----------------------------------------------------------------------------------------------------------------
    # DAILY DATA
    # ----------------------------------------------------------------------------------------------------------------
    # Daily Data Processed and Aggregated
    daily_data = data_processor.process_daily_data(
        hourly_unround, output_path=config["output_path"]
    )
    # ----------------------------------------------------------------------------------------------------------------
    # MONTHLY DATA
    # ----------------------------------------------------------------------------------------------------------------
    # Monthly Data Processed and Aggregated
    monthly_data = data_processor.process_monthly_data(
        hourly_unround, output_path=config["output_path"]
    )
    # ----------------------------------------------------------------------------------------------------------------
    # QUARTERLY DATA
    # ----------------------------------------------------------------------------------------------------------------
    # Quarterly Data Processed and Aggregated
    quarterly_data = data_processor.process_quarterly_data(
        hourly_unround, output_path=config["output_path"]
    )
    # ----------------------------------------------------------------------------------------------------------------
    return None, spark


if __name__ == "__main__":
    # Load configuration
    load_dotenv()

    # Load and process config
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
        # Replace environment variables
        for key, value in config.items():
            if (
                isinstance(value, str)
                and value.startswith("${")
                and value.endswith("}")
            ):
                env_var = value[2:-1]
                env_value = os.getenv(env_var)
                if env_value is None:
                    print(f"Warning: Environment variable {env_var} not found")
                config[key] = env_value or value

    processed_df, spark = main(config)
    spark.stop()
