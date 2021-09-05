import datetime
import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.docker_operator import DockerOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'email': 'aaron@aaronschlegel.me',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': datetime.timedelta(minutes=1)
}
with DAG(
    'petfinder',
    default_args=default_args,
    description='Run Petfinder ETL scripts',
    schedule_interval=datetime.timedelta(days=1),
    start_date=days_ago(1),
    tags=['petfinder'],
) as dag:
    t1 = DockerOperator(
        task_id='petfinder_etl',
        image='pethub',
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        command=["python", "pethub/petfinder/main.py"],
        environment={
            'PETFINDER_KEY': os.environ.get('PETFINDER_PETHUB_KEY'),
            'PETFINDER_SECRET_KEY': os.environ.get('PETFINDER_PETHUB_SECRET_KEY'),
            'AWS_SECRET_ID': os.environ.get('PETHUB_AWS_SECRET_ID'),
            'AWS_SECRET_ACCESS_KEY': os.environ.get('PETHUB_AWS_SECRET_ACCESS_KEY'),
            'DB_HOST': 'docker.for.mac.host.internal',
            'DB_NAME': os.environ.get('DB_NAME'),
            'DB_USER': os.environ.get('DB_USER'),
            'DB_PASSWORD': os.environ.get('DB_PASSWORD')
        }
    )

