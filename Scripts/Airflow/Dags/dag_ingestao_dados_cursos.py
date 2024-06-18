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
    'DAG_INGESTAO_DADOS_CURSOS',
    description='DAG que gera dados de cursos no datalake',
    default_args=default_args,
    schedule_interval='0 22 17 06 *'
)

produzir_dados_cursos = BashOperator(
    task_id='PRODUZIR_DADOS_CURSOS',
    bash_command='python3 /usr/local/airflow/scripts/ingestao_dados_cursos.py',
    dag=dag
)

produzir_dados_cursos