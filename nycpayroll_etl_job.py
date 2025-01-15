from airflow import DAG
from airflow import settings
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
import os

from modules.helpers import get_postgres_engine
from modules.extract_data import extract_data
from modules.transform import transform_data
from modules.load import load_dataframe_to_pgdb, exec_prc
from dotenv import load_dotenv


load_dotenv(override=True)
engine = get_postgres_engine()

# Get the Airflow dags folder path
dags_folder = settings.DAGS_FOLDER

# Define the folder path (Dataset folder path)
dataset_folder = os.path.join(dags_folder, "Dataset")

# Dictionary to store previous file information (modification times)
previous_files_info = {}

# Dictionary to store previously created DataFrames
created_dataframes = {}

# Setting Default arguments

default_args = {
      'owner' :'kehinde_ayanbadejo',
      'start_date' : datetime(year=2025, month=1, day =8),
      'email_on_failure' : False,
      'email_on_retry': False,
      'retries': None,
      #'retry_delay' : timedelta(minutes=10)        
}

# Instantiate DAG

with DAG(
    'nycpayroll_etl_job', 
    default_args = default_args,
    description  = 'An ETL Pipeline for NYC Payroll from CSV Source Files to Postgres Databse',
    schedule_interval = '40 11 * * *',
    catchup = False
) as dag:
     # Define Task 1
     start_task = DummyOperator(
       task_id = 'Start_pipeline'
     )
     
     # Define Task 2
     extract_task = PythonOperator(
       task_id = 'extract_data',
       python_callable = extract_data,
       op_kwargs = {'folder_path':dataset_folder, 'previous_files_info':previous_files_info, 'created_dataframes': created_dataframes}
     )
     
     
     # Define Task 3
     transformation_task = PythonOperator(
       task_id = 'transform_data',
       python_callable = transform_data,
       op_kwargs = {}
     )
     
     # Define Task 4
     staging_load_task = PythonOperator(
       task_id = 'staging_load',
       python_callable = load_dataframe_to_pgdb,
       op_kwargs = {'engine' : engine, 'db_schema': 'Staging'}
     )
     
     
      # Define Task 5
     wait_task = BashOperator(
       task_id = 'wait',
       bash_command = 'sleep 300'
    )
     
          
     # Define Task 6
     exec_procedure_task = PythonOperator(
       task_id = 'exec_prc',
       python_callable = exec_prc,
       op_kwargs = {'engine' : engine}
     )
      
      
      # Define Task 7
     end_task = DummyOperator(
       task_id = 'End_Pipeline'     
     )
     
# Set Dependencies
start_task >> extract_task >> transformation_task >> staging_load_task >> wait_task >> exec_procedure_task >> end_task

