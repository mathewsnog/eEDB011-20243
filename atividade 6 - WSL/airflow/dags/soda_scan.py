from airflow.models.dag import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator

SODA_PATH=r"/home/mathewsnog/.pyenv/versions/3.10.4/envs/airflow_env/" # can be specified as an env variable

with DAG(
    dag_id="dag_soda_delivery_scan",
    schedule='@daily',
    start_date=datetime(2022,9,10),
    catchup=False
) as dag:

    soda_test = BashOperator(
        task_id="soda_delivery_scan",
        bash_command=f"soda scan -d mysql -c \
            {SODA_PATH}/configuration.yml {SODA_PATH}/checks.yml"
    )