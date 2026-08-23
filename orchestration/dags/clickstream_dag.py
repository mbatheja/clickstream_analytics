from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'start_date': datetime(2026, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'clickstream_experiment_pipeline',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    catchup=False
) as dag:

    generate_events = BashOperator(
        task_id='generate_events',
        bash_command='python3 /app/src/pipeline/event_generator/event_generator.py'
    )

    dbt_build = BashOperator(
        task_id='dbt_build',
        bash_command='cd /app && dbt build'
    )

    evaluate_ab_test = BashOperator(
        task_id='evaluate_ab_test',
        bash_command='python3 /app/scripts/evaluate_experiment.py'
    )

    generate_events >> dbt_build >> evaluate_ab_test

