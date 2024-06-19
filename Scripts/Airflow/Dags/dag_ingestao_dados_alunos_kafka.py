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
    'DAG_INGESTAO_DADOS_ALUNOS',
    description='DAG que gera dados alunos no apache Kafka',
    default_args=default_args,
    schedule_interval='*/50 * * * *'
)

produzir_dados_kafka = BashOperator(
    task_id='PRODUZIR_DADOS_KAFKA',
    bash_command='python3 /usr/local/airflow/scripts/produtor_dados_alunos.py',
    dag=dag
)

produzir_dados_kafka