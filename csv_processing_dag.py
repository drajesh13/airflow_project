from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os

# File paths (Make sure your CSV file is in Downloads)
CSV_INPUT = os.path.expanduser("~/Downloads/sales_data.csv")
CSV_OUTPUT = os.path.expanduser("~/Downloads/sales_data_processed.csv")

# Function to process CSV
def process_csv():
    df = pd.read_csv(CSV_INPUT)

    # Drop rows with missing values
    df.dropna(inplace=True)

    # Add a column 'Total' summing numeric columns
    if not df.empty:
        df["Total"] = df.select_dtypes(include=["number"]).sum(axis=1)

    # Save cleaned CSV
    df.to_csv(CSV_OUTPUT, index=False)
    print(f"Processed CSV saved at: {CSV_OUTPUT}")

# Default DAG settings
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 17),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "csv_processing_pipeline",
    default_args=default_args,
    description="DAG to process sales CSV file",
    schedule_interval="@daily",
    catchup=False,
)

# Define task
process_task = PythonOperator(
    task_id="process_csv_task",
    python_callable=process_csv,
    dag=dag,
)

# Set dependencies
process_task
