from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging
import psycopg2

# File paths
CSV_INPUT = os.path.expanduser("~/Downloads/sales_data.csv")
CSV_OUTPUT = os.path.expanduser("~/Downloads/sales_data_processed.csv")

# Database connection details
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "Inventory_management"
DB_USER = "postgres"
DB_PASSWORD = "postgres"

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Extract function
def extract_data(**kwargs):
    try:
        logger.info(f"Extracting data from: {CSV_INPUT}")
        df = pd.read_csv(CSV_INPUT)
        logger.info(f"Data extracted successfully! Rows: {len(df)}")
        return CSV_INPUT  # Pass file path to the next task
    except Exception as e:
        logger.error(f"Error during extraction: {str(e)}")
        raise

# Transform function
def transform_data(**kwargs):
    try:
        logger.info(f"Transforming data from: {CSV_INPUT}")
        df = pd.read_csv(CSV_INPUT)
        df.dropna(inplace=True)  # Drop rows with missing values

        # Add a 'Total' column summing numeric columns
        if not df.empty:
            df["Total"] = df.select_dtypes(include=["number"]).sum(axis=1)
            logger.info(f"Transformation successful! Processed rows: {len(df)}")
        else:
            logger.warning("Input data is empty after dropping missing values!")

        df.to_csv(CSV_OUTPUT, index=False)  # Save the transformed file
        return CSV_OUTPUT  # Pass the output file path to the next task
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise

# Load function
def load_to_database(**kwargs):
    try:
        logger.info(f"Loading data into the database from: {CSV_OUTPUT}")

        # Read the processed CSV
        df = pd.read_csv(CSV_OUTPUT)

        # Connect to the PostgreSQL database
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )

        cursor = conn.cursor()

        # Create the table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS sales_data (
            order_id SERIAL PRIMARY KEY,
            customer_id INT,
            order_date DATE,
            product_id INT,
            quantity INT,
            price FLOAT,
            total FLOAT
        )
        """
        cursor.execute(create_table_query)

        # Insert the data into the table
        for _, row in df.iterrows():
            insert_query = """
            INSERT INTO sales_data (customer_id, order_date, product_id, quantity, price, total)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                row["customer_id"],
                row["order_date"],
                row["product_id"],
                row["quantity"],
                row["price"],
                row["Total"]
            ))

        conn.commit()
        cursor.close()
        conn.close()

        logger.info("Data loaded successfully into the database!")
    except Exception as e:
        logger.error(f"Error during loading to the database: {str(e)}")
        raise

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 3, 17),
}

# Define the DAG
dag = DAG(
    "custom_etl_pipeline_with_db",
    default_args=default_args,
    description="Custom ETL pipeline DAG with database loading",
    schedule_interval="0 6 * * *",  # Runs daily at 6 AM
    catchup=False,
)

# Define tasks
extract_task = PythonOperator(
    task_id="extract_data_task",
    python_callable=extract_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data_task",
    python_callable=transform_data,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_to_database_task",
    python_callable=load_to_database,
    dag=dag,
)

# Set task dependencies
extract_task >> transform_task >> load_task
