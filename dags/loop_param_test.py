from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator, BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from google.cloud import bigquery

def puller(**kwargs):
    ti = kwargs['ti']
    pulled_value = ti.xcom_pull(key='sources', task_ids='push')
    return pulled_value

def loop_param_dag(
    dag,
    location,
    bq_project,
    bq_dataset,
    source
):
    with dag:
        
        load_others_param_task = BashOperator(
            task_id=f'{source}_param',
            bash_command=f'echo now is {source} parameter.',
        )

        #load_others_param_task        

default_dag_args = {
    'start_date': datetime(2022, 6, 9),
    #'end_date': datetime(2022, 6, 10),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}

loop_param_test_dag = DAG(
    dag_id = 'loop_param',
    schedule_interval = "00 07 * * *",
    default_args = default_dag_args,
    #template_searchpath = ["/home/airflow/gcs/data/all_price"],
    catchup=False,
    tags=['test'],
)

#sources = ['source1', 'source2']

for i in sources:
    loop_param_dag(
        dag=loop_param_test_dag,
        location="asia-east2",
        bq_project="de-zoomcamp-349913",
        bq_dataset="trips_data_all",
        source=i
    )