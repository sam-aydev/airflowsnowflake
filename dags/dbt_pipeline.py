from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# These paths match the 'volumes' mapping in docker-compose.yml
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_profiles"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 0,
}

dag = DAG(
    'crypto_dbt_docker',
    default_args=default_args,
    description='Runs dbt inside Docker every minute',
    schedule_interval='*/1 * * * *', # Run every minute
    catchup=False
)

dbt_run = BashOperator(
    task_id='dbt_run',
    bash_command=f'dbt run --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag,
)

dbt_test = BashOperator(
    task_id='dbt_test',
    bash_command=f'dbt test --project-dir {DBT_PROJECT_DIR} --profiles-dir {DBT_PROFILES_DIR}',
    dag=dag,
)

dbt_run >> dbt_test