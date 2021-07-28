from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pethub
import datetime

pethub_version = '0.0.1'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2021, 7, 29),
    'schedule_interval': '@daily',
    'email': ['aaron@aaronschlegel.me'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1)
}


def get_adoptable_animals():
    pethub.etl.petfinder.animals.get_animals()


dag = DAG(
    dag_id='petfinder_animals',
    description='Run ETL of adoptable animals at organizations listed in Petfinder.com',
    default_args=default_args
)

get_petfinder_animals = PythonOperator(
    task_id='get_adoptable_animals',
    python_callable=get_adoptable_animals,
    dag=dag
)
