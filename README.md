# Airflow Project: ETL Pipeline for CSV Processing
Here I demonstrated the simple pipeline to understand the process. Becuase I cannot share my professional code base due to data privacy concern.

## Overview
This project demonstrates the implementation of an ETL (Extract, Transform, Load) pipeline using Apache Airflow. The pipeline processes a CSV file by performing data extraction, transformation, and loading operations. It is scheduled to run daily using Airflow's DAG (Directed Acyclic Graph).

## Features
- **Extract**: Read sales data from a CSV file.
- **Transform**: Clean the data by removing missing values and adding a new column `Total` to sum numeric values.
- **Load**: Save the transformed data to a new CSV file. Here we can also load this into a database or data warehouse.
## Project Setup and Execution

### 1. **Setup Python Environment**
1. Ensure you have Python 3.10 or later installed.
2. Create a virtual environment for the project:
   ```bash
   python3 -m venv venv
   source venv/bin/activate
Installed the required dependencies using : pip install apache-airflow pandas
Setup the Airflow using: export AIRFLOW_HOME=~/airflow
Initialize the Airflow database: airflow db init
Update Airflow configuartion files which added in .gitignore

## Airflow Services:
1) Start the Airflow webserver:airflow webserver --port
2) Start the Airflow scheduler: airflow scheduler
You can check your DAG in your Airflow web UI.
