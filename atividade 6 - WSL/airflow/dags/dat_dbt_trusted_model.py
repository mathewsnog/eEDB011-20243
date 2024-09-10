from pendulum import datetime
from airflow.decorators import dag
from airflow.operators.bash import BashOperator


default_args = {
    'owner': 'Matheus',
    'start_date': datetime(2024, 8, 9),
    'retries': 1
}

@dag(
    start_date=datetime(2024, 9, 9),
    schedule_interval='30 1 * * *',
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    max_active_tasks=1,
)
def dag_dbt_trusted_model():
    dbt_run = BashOperator(
        task_id="dag_dbt_trusted_model",
        bash_command="cd '/mnt/c/Users/teu20/Documents/Poli/Repo/eEDB011-20243/atividade 6 - WSL/dbt_project/bancos_airflow' && source '/home/mathewsnog/.pyenv/versions/3.10.4/envs/dbt_env/bin/activate' && dbt run --models bancos_trusted",
    )


dag_dbt_trusted_model()