"""
DAG: wikipedia_flow
Descrição: Orquestra o fluxo de dados dos estádios da Wikipedia até o Azure Data Lake.
Fluxo: Extração -> Transformação -> Escrita (Carga)
"""

from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import sys

# Garante que o diretório raiz do projeto esteja no path para importar as funções da pipeline
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipelines.wikipedia_pipeline import extract_wikipedia_data, transform_wikipedia_data, write_wikipedia_data

# Definição básica da DAG
dag = DAG(
    dag_id='wikipedia_flow',
    default_args={
        'owner': 'Otavio',
        'start_date': datetime(2026, 4, 23),
    },
    schedule_interval=None, # Execução manual (on-demand)
    catchup=False
)

# Task 1: Extração de dados via Web Scraping
extract_data_from_wikipedia = PythonOperator(
    task_id='extract_data_from_wikipedia',
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={
        'url': 'https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity',
    },
    dag=dag
)

# Task 2: Limpeza, enriquecimento (geolocalização) e formatação
transform_wikipedia_data = PythonOperator(
    task_id='transform_wikipedia_data',
    provide_context=True,
    python_callable=transform_wikipedia_data,
    dag=dag
)

# Task 3: Carregamento dos dados processados no Azure Data Lake Storage Gen2
write_wikipedia_data = PythonOperator(
    task_id='write_wikipedia_data',
    provide_context=True,
    python_callable=write_wikipedia_data,
    dag=dag,
)

# Dependências entre as tasks (Sequencial)
extract_data_from_wikipedia >> transform_wikipedia_data >> write_wikipedia_data
