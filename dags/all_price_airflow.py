from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta

import json

from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDataOperator, BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
from google.cloud import bigquery

# Config variables
LOCATION = "asia-east2"
BQ_PROJECT = "de-zoomcamp-349913"
BQ_DATASET = "trips_data_all"

BQ_SOURCE_TABLE = "source_list"
BQ_WWX_SOURCE_TABLE = "wine_price"
BQ_OTHERS_SOURCE_TABLE = "wwx_selected_wine_price"
BQ_WORKING_1_TABLE = "all_price_working_1_airflow"
BQ_WORKING_2_TABLE = "all_price_working_2_airflow"
BQ_WORKING_3_TABLE = "all_price_working_3_airflow"
BQ_TARGET_TABLE = "all_price_airflow"

#TEMPLATE_SEARCH_PATH = "/home/airflow/gcs/data/all_price"
TEMPLATE_SEARCH_PATH = "/opt/airflow/data/all_price"

BATCH_DATE = (datetime.now(timezone(timedelta(hours=8))) - timedelta(days=1)).strftime('%Y-%m-%d')
SIX_MONTH_DATE = (datetime.now() - relativedelta(months=6)).strftime('%Y-%m-%d')

def read_table(ti, include_wwx, bq_project_name, bq_dataset_name, bq_table_name, location):
    client = bigquery.Client(location=location, project=bq_project_name)
    if include_wwx:
        source_list_query = f"select source from {bq_dataset_name}.{bq_table_name}"
    else:
        source_list_query = f"select source from {bq_dataset_name}.{bq_table_name} where source <> 'wwx'"
    source_list_job = client.query(source_list_query)
    source_list = []
    for row in source_list_job:
        source_name = row["source"]
        source_list.append(source_name)
    source_str = '", "'.join(str(x) for x in source_list)
    data = json.loads('{"source": ["' + source_str + '"]}')
    # push to XCom using return
    if include_wwx:
        ti.xcom_push("list_of_sources", data)
    else:
        ti.xcom_push("list_of_others_sources", data)
    return data

def process_obtained_data(ti, task_id, include_wwx):   
    if include_wwx:
        list_of_sources = ti.xcom_pull(task_id)
        Variable.set(key='list_of_sources',
                    value=list_of_sources['source'], serialize_json=True)
    else:
        list_of_others_sources = ti.xcom_pull(task_id)
        Variable.set(key='list_of_others_sources',
                    value=list_of_others_sources['source'], serialize_json=True)

default_dag_args = {
    'start_date': datetime(2022, 6, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    #'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id = 'all_price_airflow',
    # Continue to run DAG once per day
    schedule_interval = "@once",
    default_args = default_dag_args,
    template_searchpath = [TEMPLATE_SEARCH_PATH],
    catchup=False,
    max_active_runs = 1, 
    concurrency = 1, # Set 1 for tasks in the task group to run one by one
) as dag:

    start_email_task = EmailOperator(
        task_id = "send_email",
        to = ['lillian.ke@smartdatainstitute.com'],
        subject = "airflow task started",
        html_content = "<br>Job started at "+ datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S.%f') +".</br>",
    )

    ### Task 1: Delete recent 6 months data ###
    DELETE_BQ_TBL_QUERY = (
        f"DELETE FROM {BQ_DATASET}.{BQ_TARGET_TABLE} \
        WHERE PRICE_DATE >= '{SIX_MONTH_DATE}';"
    )

    bq_delete_data_task = BigQueryInsertJobOperator(
        task_id=f"bq_delete_data_task",
        configuration={
            "query": {
                "query": DELETE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    ### Task 2: Insert data from different sources ###
    ## WWX ##
    INSERT_BQ_TBL_QUERY = (
        f"""INSERT INTO {BQ_DATASET}.{BQ_TARGET_TABLE} 
        SELECT PRICE_DATE, WINE_ID, WINE_NAME, VINTAGE, PACKING_SIZE, 'wwx' AS SOURCE, PRICE_PER_BOTTLE, NULL, PRICE_FINAL_IS_FILLNA AS IS_FILLNA, ROW_NO_FOR_NULL, TRUE AS INCLUDE_FLAG, date('{BATCH_DATE}'), timestamp(%s) 
        FROM {BQ_DATASET}.{BQ_WWX_SOURCE_TABLE} 
        WHERE PRICE_DATE >= '{SIX_MONTH_DATE}';"""
    )%("'"+datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')+"'")

    bq_insert_wwx_into_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_insert_wwx_into_{BQ_TARGET_TABLE}_table_task",
        configuration={
            "query": {
                "query": INSERT_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    get_others_sources_list_task = PythonOperator(
        task_id='get_others_sources_list',
        python_callable=read_table,
        op_kwargs = {
            "include_wwx": False,
            "bq_project_name": BQ_PROJECT,
            "bq_dataset_name": BQ_DATASET,
            "bq_table_name": BQ_SOURCE_TABLE,
            "location": LOCATION
        },
    )

    process_others_source_list_task = PythonOperator(
        task_id='process_others_source_list',
        python_callable=process_obtained_data,
        op_kwargs = {
            "task_id": 'get_others_sources_list',
            "include_wwx": False,
        }
    )

    others_source_iterable_list = Variable.get(
        'list_of_others_sources',
        default_var=None,
        deserialize_json=True
    )

    with TaskGroup(
        'insert_others_into_table_tasks_group',
        prefix_group_id=False,
    ) as insert_others_into_table_tasks_group:
        if others_source_iterable_list:
            for source in others_source_iterable_list:
                bq_insert_others_into_table_task = BigQueryInsertJobOperator(
                    task_id=f"bq_insert_{source}_into_{BQ_TARGET_TABLE}_table_task",
                    configuration={
                        "query": {
                            "query": "{% include 'others_sources.sql' %}",
                            "useLegacySql": False,
                        }
                    },
                    location=LOCATION,
                    params={
                        "dataset": BQ_DATASET,
                        "source_table": BQ_OTHERS_SOURCE_TABLE,
                        "target_table": BQ_TARGET_TABLE,
                        "source": source,
                        "six_month_date": SIX_MONTH_DATE,
                        "batch_date": BATCH_DATE,
                    }
                )

                # TaskGroup level dependencies
                bq_insert_others_into_table_task

    ### Task 3: Insert into all_price_working_2 table ###
    TRUNCATE_BQ_TBL_QUERY = (
        f"TRUNCATE TABLE {BQ_DATASET}.{BQ_WORKING_2_TABLE};"
    )

    bq_truncate_working_2_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_truncate_{BQ_WORKING_2_TABLE}_table_task",
        configuration={
            "query": {
                "query": TRUNCATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    ## Insert data ##
    bq_insert_into_working_2_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_insert_{BQ_WORKING_2_TABLE}_table_task",
        configuration={
                "query": {
                    "query": "{% include 'all_price_working_2.sql' %}",
                    "useLegacySql": False,
                }
            },
            location=LOCATION,
            params={
                "dataset": BQ_DATASET,
                "working_table": BQ_WORKING_2_TABLE,
                "target_table": BQ_TARGET_TABLE,
                "six_month_date": SIX_MONTH_DATE,
                "batch_date": BATCH_DATE,
            }
    )
    
    # Task 4: Insert into all_price_working_3 table
    ## Truncate table ##
    TRUNCATE_BQ_TBL_QUERY = (
        f"TRUNCATE TABLE {BQ_DATASET}.{BQ_WORKING_3_TABLE};"
    )

    bq_truncate_working_3_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_truncate_{BQ_WORKING_3_TABLE}_table_task",
        configuration={
            "query": {
                "query": TRUNCATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    get_sources_list_task = PythonOperator(
        task_id='get_sources_list',
        python_callable=read_table,
        op_kwargs = {
            "include_wwx": True,
            "bq_project_name": BQ_PROJECT,
            "bq_dataset_name": BQ_DATASET,
            "bq_table_name": BQ_SOURCE_TABLE,
            "location": LOCATION
        },
    )

    process_source_list_task = PythonOperator(
        task_id='process_source_list',
        python_callable=process_obtained_data,
        op_kwargs = {
            "task_id": 'get_sources_list',
            "include_wwx": True,
        }
    )

    source_iterable_list = Variable.get(
        'list_of_sources',
        default_var=None,
        deserialize_json=True
    )

    with TaskGroup(
        'insert_into_table_tasks_group',
        prefix_group_id=False,
    ) as insert_into_table_tasks_group:
        if source_iterable_list:
            for source in source_iterable_list:
                bq_insert_into_table_task = BigQueryInsertJobOperator(
                    task_id=f"bq_insert_{source}_into_{BQ_WORKING_3_TABLE}_table_task",
                    configuration={
                        "query": {
                            "query": "{% include 'all_price_working_3.sql' %}",
                            "useLegacySql": False,
                        }
                    },
                    location=LOCATION,
                    params={
                        "dataset": BQ_DATASET,
                        "working_table": BQ_WORKING_3_TABLE,
                        "source_table": BQ_WORKING_2_TABLE,
                        "source": source,
                        "batch_date": BATCH_DATE,
                    }
                )

                # TaskGroup level dependencies
                bq_insert_into_table_task

    # Task 5: Insert into all_price_working_1 table
    ## Truncate table ##
    TRUNCATE_BQ_TBL_QUERY = (
        f"TRUNCATE TABLE {BQ_DATASET}.{BQ_WORKING_1_TABLE};"
    )

    bq_truncate_working_1_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_truncate_{BQ_WORKING_1_TABLE}_table_task",
        configuration={
            "query": {
                "query": TRUNCATE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    ## Insert data ##
    bq_insert_into_working_1_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_insert_{BQ_WORKING_1_TABLE}_table_task",
        configuration={
                "query": {
                    "query": "{% include 'all_price_working_1.sql' %}",
                    "useLegacySql": False,
                }
            },
            location=LOCATION,
            params={
                "dataset": BQ_DATASET,
                "working_table": BQ_WORKING_1_TABLE,
                "target_table": BQ_TARGET_TABLE,
                "batch_date": BATCH_DATE,
                "six_month_date": SIX_MONTH_DATE,
            }
    )

    bq_delete_data_again_task = BigQueryInsertJobOperator(
        task_id=f"bq_delete_data_again_task",
        configuration={
            "query": {
                "query": DELETE_BQ_TBL_QUERY,
                "useLegacySql": False,
            }
        }
    )

    # Task 6: Finalize all_price table
    bq_finalize_table_task = BigQueryInsertJobOperator(
        task_id=f"bq_finalize_table_task",
        configuration={
                "query": {
                    "query": "{% include 'all_price.sql' %}",
                    "useLegacySql": False,
                }
            },
            location=LOCATION,
            params={
                "dataset": BQ_DATASET,
                "target_table": BQ_TARGET_TABLE,
                "batch_date": BATCH_DATE,
                "working_table_1": BQ_WORKING_1_TABLE,
                "working_table_3": BQ_WORKING_3_TABLE,
            }
    )

# DAG level dependencies
start_email_task >> bq_delete_data_task \
>> bq_insert_wwx_into_table_task \
>> get_others_sources_list_task >> process_others_source_list_task >> insert_others_into_table_tasks_group \
>> bq_truncate_working_2_table_task >> bq_insert_into_working_2_table_task \
>> bq_truncate_working_3_table_task >> get_sources_list_task >> process_source_list_task >> insert_into_table_tasks_group \
>> bq_truncate_working_1_table_task >> bq_insert_into_working_1_table_task >> bq_delete_data_again_task >> bq_finalize_table_task