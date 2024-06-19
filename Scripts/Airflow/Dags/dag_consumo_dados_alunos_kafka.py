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
    'DAG_CONSUMO_DADOS_ALUNOS',
    description='DAG que consome dados de alunos no apache Kafka',
    default_args=default_args,
    schedule_interval='*/60 * * * *'
)

consumir_dados_alunos_kafka = BashOperator(
    task_id='CONSUMIR_DADOS_ALUNOS_KAFKA',
    bash_command='python3 /usr/local/airflow/scripts/consumer_ingestao_raw_dados_alunos.py',
    dag=dag
)

consumir_dados_alunos_kafka