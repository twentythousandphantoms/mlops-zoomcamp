import requests
import datetime
import pandas as pd
import psycopg2
import logging
import os

from evidently import ColumnMapping
from evidently.report import Report
from evidently.metrics import ColumnQuantileMetric

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s]: %(message)s")

# Database configuration
# Get database details from environment variables
DATABASE_HOST = os.getenv("DATABASE_HOST", "localhost")
DATABASE_PORT = os.getenv("DATABASE_PORT", "5432")
DATABASE_USER = os.getenv("DATABASE_USER", "postgres")
DATABASE_PASSWORD = os.getenv("DATABASE_PASSWORD", "example")
DATABASE_NAME = os.getenv("DATABASE_NAME", "quantile_metrics")

DB_CONFIG = {
    "host": DATABASE_HOST,
    "port": DATABASE_PORT,
    "dbname": DATABASE_NAME,
    "user": DATABASE_USER,
    "password": DATABASE_PASSWORD
}

# Create the table statement
CREATE_TABLE_QUERY = """
DROP TABLE IF EXISTS quantile_metrics;
CREATE TABLE quantile_metrics (
    date DATE PRIMARY KEY,
    quantile_0_5 FLOAT
);
"""

# Define Evidently configuration
column_mapping = ColumnMapping(
    target=None,
    prediction=None,
    numerical_features=["fare_amount"]
)

# Function to create the table in the database
def prep_db():
    with psycopg2.connect(**DB_CONFIG) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_QUERY)
            logging.info("Database table 'quantile_metrics' prepared.")

# Function to insert metrics into the database
def insert_metrics(date, quantile_value):
    # Convert quantile_value to native Python float
    quantile_value = float(quantile_value)
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO quantile_metrics (date, quantile_0_5)
                VALUES (%s, %s)
                ON CONFLICT (date) DO UPDATE SET quantile_0_5 = EXCLUDED.quantile_0_5;
                """,
                (date, quantile_value)
            )
            logging.info(f"Inserted metrics for {date}: quantile_0_5 = {quantile_value}")


# Main function to calculate and store the quantile metric
def calculate_and_store_quantiles(data_path):
    # Load the data
    logging.info(f"Loading data from {data_path}")
    data = pd.read_parquet(data_path)

    # Add a 'date' column
    data['date'] = data['lpep_pickup_datetime'].dt.date

    # Filter for March 2024
    start_date = datetime.date(2024, 3, 1)
    end_date = datetime.date(2024, 3, 31)
    data = data[(data['date'] >= start_date) & (data['date'] <= end_date)]

    # Define Evidently report
    report = Report(metrics=[
        ColumnQuantileMetric(column_name="fare_amount", quantile=0.5)
    ])

    # Calculate metrics for each day
    for date, group in data.groupby("date"):
        logging.info(f"Processing data for {date}")
        report.run(reference_data=group, current_data=group, column_mapping=column_mapping)
        result = report.as_dict()

        # Extract the quantile metric
        quantile_value = result['metrics'][0]['result']['current']['value']
        logging.info(f"Quantile (0.5) for {date}: {quantile_value}")

        # Insert the result into the database
        insert_metrics(date, quantile_value)

# Entry point
if __name__ == "__main__":
    # Prepare the database
    prep_db()

    # Path to the March 2024 data file
    DATA_PATH = "data/green_tripdata_2024-03.parquet"

    # Calculate and store the metrics
    calculate_and_store_quantiles(DATA_PATH)
