from airflow import DAG
from airflow.operators.python import PythonOperator

import pethub
import datetime

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


def get_animals_at_organizations():
    pethub.etl.petfinder.animals.get_animals()


def get_animal_images():
    pethub.etl.petfinder.images.download_images('animals')


def get_organization_images():
    pethub.etl.petfinder.images.download_images('organizations')


dag = DAG(
    dag_id='petfinder_etl',
    description='Get animals at organizations in database, download any new animal images to s3 and update record',
    default_args=default_args
)

get_animals_at_organizations = PythonOperator(
    task_id='get_animals_at_organizations',
    python_callable=get_animals_at_organizations,
    dag=dag
)

get_animal_images = PythonOperator(
    task_id='get_animal_images',
    python_callable=get_animal_images,
    dag=dag
)

get_organization_images = PythonOperator(
    task_id='get_organization_images',
    python_callable=get_organization_images,
    dag=dag
)

get_animals_at_organizations >> [get_animal_images, get_organization_images]
