from airflow import DAG 
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta 

default_args = {
    'owner' : 'Qusay Q', 
    'depends_on_past' : False, # if there was no race on a sunday 
    'email_on_failure' : False, 
    'email_on_retry' : False, 
    'retries' : 2, 
    'retry_delay' : timedelta(minutes=5) 
    # 'end_data' :  no end data, run until there is f1? 
}   

with DAG ( 
    dag_id = 'f1_data_pipeline',
    default_args = default_args,
    description = 'F1 Quali, Race, Constructor & Driver Standings',
    schedule = '0 0 * * 0',
    start_date = datetime(2025, 3, 16), 
 ) as dag: 
    
    fetch_data = BashOperator(
        task_id = 'fetch-RESTAPI-data',
        bash_command='python3 /Users/qusayqadir/Documents/github/Formula-1-Telemetry-Pipeline/Formula-1-Telemetry-Pipeline/src/data-ingestion/grab_data.py',
    )

    spark_jobs = BashOperator (
        task_id = 'reformatJSON-CSVfriendly',
        bash_command='python3 /Users/qusayqadir/Documents/github/Formula-1-Telemetry-Pipeline/Formula-1-Telemetry-Pipeline/src/data-processing/sparkjobs(main).py', 
    )

    mergeCSV = BashOperator(
        task_id = 'merge_intolargeCSV', 
        bash_command='python3 /Users/qusayqadir/Documents/github/Formula-1-Telemetry-Pipeline/Formula-1-Telemetry-Pipeline/src/data-processing/consolidateCSVs.py', 
    )

    pushCloud = BashOperator(
        task_id = 'push-to-azure',
        bash_command='python3 /Users/qusayqadir/Documents/github/Formula-1-Telemetry-Pipeline/Formula-1-Telemetry-Pipeline/src/storage-azure/push-cloud.py',    
    )

    fetch_data >> spark_jobs >> mergeCSV >> pushCloud 

