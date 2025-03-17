from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging

# Default file paths (can be parameterized)
CSV_INPUT = os.path.expanduser("~/Downloads/sales_data.csv")
CSV_OUTPUT = os.path.expanduser("~/Downloads/sales_data_processed.csv")

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Extract function
def extract_data(**kwargs):
    try:
        logger.info(f"Extracting data from: {CSV_INPUT}")
        df = pd.read_csv(CSV_INPUT)
        logger.info(f"Data extracted successfully! Rows: {len(df)}")
        return CSV_INPUT  # Pass file path to next task
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
        return df.to_dict()  # Optional: Return data as dict for Load task
    except Exception as e:
        logger.error(f"Error during transformation: {str(e)}")
        raise

# Load function
def load_data(**kwargs):
    try:
        logger.info(f"Loading transformed data to: {CSV_OUTPUT}")
        ti = kwargs["ti"]
        transformed_data = ti.xcom_pull(task_ids="transform_data_task")  # Retrieve data from previous task
        df = pd.DataFrame.from_dict(transformed_data)
        df.to_csv(CSV_OUTPUT, index=False)
        logger.info(f"Data loaded successfully! File saved at: {CSV_OUTPUT}")
    except Exception as e:
        logger.error(f"Error during loading: {str(e)}")
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
    "custom_etl_pipeline",
    default_args=default_args,
    description="A robust and customizable ETL pipeline for processing CSV data",
    schedule_interval="0 6 * * *",  # Runs daily at 6 AM
    catchup=False,
)

# Define tasks
extract_task = PythonOperator(
    task_id="extract_data_task",
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id="transform_data_task",
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id="load_data_task",
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Set dependencies
extract_task >> transform_task >> load_task
