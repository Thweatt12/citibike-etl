from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.bash import BashOperator  # no providers

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="citibike_data_pipeline",
    description="Extract -> Load -> Build facts (no providers)",
    start_date=datetime(2025, 9, 5, tzinfo=timezone.utc),  # must be in the past
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["citibike", "etl"],
) as dag:

    create_tables = BashOperator(
        task_id="create_tables",
        bash_command="python -u /opt/airflow/dags/run_sql.py /opt/airflow/dags/bronze/create_tables.sql",
    )

    unzip = BashOperator(
        task_id="unzip_files",
        bash_command="python -u /opt/airflow/dags/bronze/unzip.py",
    )

    load = BashOperator(
        task_id="normalize_load_csvs",
        bash_command="python -u /opt/airflow/dags/silver/normalize_load.py",
    )

    build_dims_facts = BashOperator(
        task_id="build_dims_facts",
        bash_command="python -u /opt/airflow/dags/run_sql.py /opt/airflow/dags/gold/build_dims_facts.sql",
    )

    # DAG order
    unzip >> create_tables >> load >> build_dims_facts
