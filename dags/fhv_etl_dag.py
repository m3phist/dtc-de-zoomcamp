import logging
import os
import pandas as pd
import pyarrow

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from google.cloud import storage
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.task_group import TaskGroup

S3_BUCKET_NAME = 'nyc-tlc'
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
DOWNLOAD_TO_LOCAL_PATH = AIRFLOW_HOME + '/data/'
PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')


def download_from_s3(key: str, bucket_name: str, local_path: str) -> str:
    hook = S3Hook('s3_conn')
    file_name = hook.download_file(
        key=key, bucket_name=bucket_name, local_path=local_path)
    return file_name


def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0],
              dst=f"{downloaded_file_path}/{new_name}")


def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error(
            "Can only accept source files in CSV format, for the moment")
        return
    df = pd.read_csv(src_file, encoding = "ISO-8859-1")
    df["pickup_datetime"] = df["pickup_datetime"].astype("datetime64[s]")
    df["DOLocationID"] = df["DOLocationID"].astype("Int64")
    df["PULocationID"] = df["PULocationID"].astype("Int64")
    df["SR_Flag"] = df["SR_Flag"].astype("Int64")
    df["dropoff_datetime"] = pd.to_datetime(df['dropoff_datetime'],errors = 'coerce')
    pd.DataFrame.to_parquet(df, dest_file)


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

def download_parquetize_upload_bq_dag(
    dag,
    url_template,
    rename_template,
    local_csv_path_template,
    local_parquet_path_template,
    gcs_path_template,
    DATASET,
    COLOUR_RANGE,
    INPUT_PART,
    INPUT_FILETYPE,
):
    with dag:
        with TaskGroup(group_id='fhv_data_processing_tasks', prefix_group_id=False) as fhv_data_processing_tasks:
            download_from_s3_task = PythonOperator(
                task_id='download_from_s3',
                python_callable=download_from_s3,
                op_kwargs={
                    'key': url_template,
                    'bucket_name': S3_BUCKET_NAME,
                    'local_path': DOWNLOAD_TO_LOCAL_PATH
                }
            )
            rename_file_task = PythonOperator(
                task_id='rename_file',
                python_callable=rename_file,
                op_kwargs={
                    'new_name': rename_template
                }
            )
            format_to_parquet_task = PythonOperator(
                task_id="format_to_parquet_task",
                python_callable=format_to_parquet,
                op_kwargs={
                    "src_file": local_csv_path_template,
                    "dest_file": local_parquet_path_template
                },
            )
            local_to_gcs_task = PythonOperator(
                task_id="local_to_gcs_task",
                python_callable=upload_to_gcs,
                op_kwargs={
                    "bucket": BUCKET,
                    "object_name": gcs_path_template,
                    "local_file": local_parquet_path_template,
                },
            )
            rm_task = BashOperator(
                task_id="rm_task",
                bash_command=f"rm -f {local_csv_path_template} {local_parquet_path_template}",
            )
            download_from_s3_task >> rename_file_task >> format_to_parquet_task >> local_to_gcs_task >> rm_task

        with TaskGroup('fhv_load_2_bq') as fhv_load_2_bq:
            for colour, (ds_col, auto_detect) in COLOUR_RANGE.items():
                move_files_gcs_task = GCSToGCSOperator(
                        task_id=f'move_{colour}_{DATASET}_files_task',
                        source_bucket=BUCKET,
                        source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
                        destination_bucket=BUCKET,
                        destination_object=f'{colour}/{colour}_{DATASET}',
                        move_object=True
                    )
                bigquery_external_table_task = BigQueryCreateExternalTableOperator(
                        task_id=f"bq_{colour}_{DATASET}_external_table_task",
                        table_resource={
                            "tableReference": {
                                "projectId": PROJECT_ID,
                                "datasetId": BIGQUERY_DATASET,
                                "tableId": f"{colour}_{DATASET}_external_table",
                            },
                            "externalDataConfiguration": {
                                "autodetect": f"{auto_detect}",
                                "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                                "sourceUris": [f"gs://{BUCKET}/{colour}/*"],
                            },
                            "schema": {
                                "fields": [
                                    {
                                        "mode": "NULLABLE",
                                        "name": "dispatching_base_num",
                                        "type": "STRING"
                                    },
                                    {
                                        "mode": "NULLABLE",
                                        "name": "pickup_datetime",
                                        "type": "TIMESTAMP"
                                    },
                                    {
                                        "mode": "NULLABLE",
                                        "name": "dropoff_datetime",
                                        "type": "TIMESTAMP"
                                    },
                                    {
                                        "mode": "NULLABLE",
                                        "name": "PULocationID",
                                        "type": "INTEGER"
                                    },
                                    {
                                        "mode": "NULLABLE",
                                        "name": "DOLocationID",
                                        "type": "INTEGER"
                                    },
                                    {
                                        "mode": "NULLABLE",
                                        "name": "SR_Flag",
                                        "type": "INTEGER"
                                    }
                                ]
                            },
                        },
                    )
                CREATE_BQ_TBL_QUERY = (
                    f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{colour}_{DATASET} \
                    PARTITION BY DATE({ds_col}) \
                    AS \
                    SELECT * FROM {BIGQUERY_DATASET}.{colour}_{DATASET}_external_table;"
                )
                bq_create_partitioned_table_job = BigQueryInsertJobOperator(
                    task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
                    configuration={
                        "query": {
                            "query": CREATE_BQ_TBL_QUERY,
                            "useLegacySql": False,
                        }
                    }
                )
                move_files_gcs_task >> bigquery_external_table_task >> bq_create_partitioned_table_job

        fhv_data_processing_tasks >> fhv_load_2_bq


FHV_TAXI_URL_TEMPLATE = 'csv_backup/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv'
FHV_RENAME_FILE_TEMPLATE = "fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
FHV_CSV_FILE_TEMPLATE = AIRFLOW_HOME + \
    "/data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv"
FHV_PARQUET_FILE_TEMPLATE = AIRFLOW_HOME + \
    "/data/fhv_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.parquet"
FHV_TAXI_GCS_PATH_TEMPLATE = "raw/fhv_tripdata/{{ execution_date.strftime(\'%Y\') }}/{{ execution_date.strftime(\'%m\') }}.parquet"

FHV_DATASET = "tripdata"
FHV_COLOUR_RANGE = {'fhv': ('pickup_datetime', 'False')}
FHV_INPUT_PART = "raw"
FHV_INPUT_FILETYPE = "parquet"

fhv_etl_dag = DAG(
    dag_id="fhv_etl_dag",
    schedule_interval="0 8 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2021, 1, 1),
    default_args=default_args,
    catchup=True,
    max_active_runs=1,
    tags=['us-central1'],
) 
download_parquetize_upload_bq_dag(
    dag=fhv_etl_dag,
    url_template=FHV_TAXI_URL_TEMPLATE,
    rename_template=FHV_RENAME_FILE_TEMPLATE,
    local_csv_path_template=FHV_CSV_FILE_TEMPLATE,
    local_parquet_path_template=FHV_PARQUET_FILE_TEMPLATE,
    gcs_path_template=FHV_TAXI_GCS_PATH_TEMPLATE,
    DATASET=FHV_DATASET,
    COLOUR_RANGE=FHV_COLOUR_RANGE,
    INPUT_PART=FHV_INPUT_PART,
    INPUT_FILETYPE=FHV_INPUT_FILETYPE
)

