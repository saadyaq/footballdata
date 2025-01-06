from airflow import DAG
from datetime import datetime
import sys
import os

from airflow.operators.python import PythonOperator

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from pipelines.wikipedia_pipeline import get_wikipedia_page,extract_wikipedia_data

dag=DAG(
    dag_id='wikipedia_flow',
    default_args={
        "owner": "Saad Yaq",
        "start_date": datetime(2025, 1, 1),
    },
    schedule_interval=None,
    catchup=False
)

#Extraction

extract_data_from_wikipedia=PythonOperator(
    task_id="extract_data_from_wikipedia",
    python_callable=extract_wikipedia_data,
    provide_context=True,
    op_kwargs={
        "url":"https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"},
    dag=dag
)

#Preprocessing

#Write
