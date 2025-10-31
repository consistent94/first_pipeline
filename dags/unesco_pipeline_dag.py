from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from extract.extract import fetch_unesco_sites, load_to_postgres

def run_extract_load():
    df = fetch_unesco_sites()
    load_to_postgres(df)

with DAG(
    dag_id="unesco_pipeline",
    start_date=datetime(2024,1,1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    extract_load = PythonOperator(
        task_id="extract_load",
        python_callable=run_extract_load
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /opt/airflow/dbt_project && dbt run"
    )

    extract_load >> dbt_run
