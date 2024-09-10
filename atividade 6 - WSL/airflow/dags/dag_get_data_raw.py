import airflow
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'Matheus',
    'start_date': datetime(2024, 8, 9),
    'retries': 1,
	  'retry_delay': timedelta(hours=1)
}
with airflow.DAG('get_data_raw',
                  default_args=default_args,
                  schedule_interval='00 23 * * *',
                  max_active_runs=1,
                  max_active_tasks=1,
                  catchup=False) as dag:
    task_get_data_raw = BashOperator(
        task_id='get_data_raw',
        bash_command="python '/mnt/c/Users/teu20/Documents/Poli/Repo/eEDB011-20243/atividade 6 - WSL/get_data_raw.py'",
    )