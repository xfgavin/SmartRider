#!/usr/bin/env python3
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
from datetime import timedelta


# NYC taxi trip data is updated nearly monthly. So make a monthly dag to check new data.

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020,10,14),
    'email': '',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=15)
}

dag = DAG(
    dag_id = 'smartrider_monthly',
    description = 'check new taxi trip data, run ETL, and update rates',
    schedule_interval = timedelta(days=30),
    default_args = default_args
)

#This script will:
#1. check tripdata_filename table in database and figure out new filename for new data
#2. check if new data exists in s3
#3. submit spark job if needed
######
#need to leave a space at the end of bash_command, see: https://stackoverflow.com/questions/42147514/templatenotfound-error-when-running-simple-airflow-bashoperator
check_rec = BashOperator(
    task_id = 'check_rec',
    retries = 0,
    bash_command = 'ssh -i /opt/id_rsa ubuntu@10.0.0.8 /home/ubuntu/smartrider/data_processing/checkrec.sh ', 
    dag = dag
)

update_rate = BashOperator(
    task_id = 'update_rates',
    bash_command = 'ssh -i /opt/id_rsa ubuntu@10.0.0.9 "/home/ubuntu/smartrider/db/setup_db.sh -u" ', 
    dag = dag
)

# setting dependencies
check_rec >> update_rate
