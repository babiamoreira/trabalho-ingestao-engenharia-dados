from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'start_date': datetime.now(),
    'catchup': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'DAG_INGESTAO_DADOS_DISCIPLINAS',
    description='DAG que gera dados de disciplinas no datalake',
    default_args=default_args,
    schedule_interval='0 22 19 06 *'
)

produzir_dados_disciplinas = BashOperator(
    task_id='PRODUZIR_DADOS_DISCIPLINAS',
    bash_command='python3 /usr/local/airflow/scripts/ingestao_dados_disciplinas.py',
    dag=dag
)

produzir_dados_disciplinas