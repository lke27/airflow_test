from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator, BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

LOCATION="asia-east2"
BQ_PROJECT="de-zoomcamp-349913"
BQ_DATASET="trips_data_all"
BQ_TABLE="source_list"
FILE_NAME="source_list.json"
#GCS_URL="gs://dtc_data_lake_de-zoomcamp-349913/data/" + FILE_NAME
GCS_URL="gs://trips_data_all/data/" + FILE_NAME

def _process_obtained_data(ti):
    list_of_sources = ti.xcom_pull(task_ids='get_data')
    Variable.set(key='list_of_sources',
                 value=list_of_sources['source'], serialize_json=True)

def _read_table(bq_project_name, bq_dataset_name, bq_table_name, location):
    client = bigquery.Client(location=location, project=bq_project_name)
    source_list_query = f"select source from {BQ_DATASET}.{BQ_TABLE}"
    source_list_job = client.query(source_list_query)
    source_list = []
    for row in source_list_job:
        source_name = row["source"]
        source_list.append(source_name)
    source_str = '", "'.join(str(x) for x in source_list)
    data = json.loads('{"source": ["' + source_str + '"]}')
    # push to XCom using return
    return data

default_dag_args = {
    'start_date': datetime(2022, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'a_test_iteration',
    schedule_interval='@once',
    default_args = default_dag_args,
    catchup = False,
    max_active_runs = 1, 
    concurrency = 1, # Set 1 for tasks in the task group to run one by one
) as dag:

    get_data_task = PythonOperator(
        task_id='get_data',
        python_callable=_read_table,
        op_kwargs = {
            "bq_project_name": BQ_PROJECT,
            "bq_dataset_name": BQ_DATASET,
            "bq_table_name": BQ_TABLE,
           "location": LOCATION
        },
    )

    preparation_task = PythonOperator(
        task_id='preparation_task',
        python_callable=_process_obtained_data
    )

    end = BashOperator(
        task_id=f'end',
        bash_command=f'echo end',
    )

    iterable_list = Variable.get(
        'list_of_sources',
        default_var=['default_source'],
        deserialize_json=True
    )

    with TaskGroup(
        'dynamic_tasks_group',
        prefix_group_id=False,
    ) as dynamic_tasks_group:
        if iterable_list:
            for source in iterable_list:
                say_hello = BashOperator(
                    task_id=f'say_hello_from_{source}',
                    bash_command=f'echo hello from {source}.',
                )
                say_goodbye = BashOperator(
                    task_id=f'say_goodbye_from_{source}',
                    bash_command=f'echo goodbye from {source}.',
                )

                # TaskGroup level dependencies
                say_hello >> say_goodbye

# DAG level dependencies
get_data_task >> preparation_task >> dynamic_tasks_group >> end