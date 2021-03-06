import os
import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
URL_PREFIX = "https://s3.amazonaws.com/nyc-tlc/trip+data"
GREEN_TAXI_URL_TEMPLATE = URL_PREFIX + "/green_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
PARQUET_FILE = "green_tripdata_{{ execution_date.strftime('%Y-%m') }}.parquet"
GREEN_TAXI_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/" + PARQUET_FILE
TABLE_NAME_TEMPLATE = "green_tripdata_{{ execution_date.strftime('%Y_%m') }}"
GREEN_TAXI_GCS_PATH_TEMPLATE = "raw/green_tripdata/{{ execution_date.strftime('%Y') }}/" + PARQUET_FILE


def upload_to_gcs(bucket, object_name, local_file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2019,1,1),
    "end_date": datetime(2021,1,1), 
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="green_ingestion_gcs_dag",
    schedule_interval="0 6 2 * *", #change to different settings if needed
    default_args=default_args,
    catchup=True,
    max_active_runs=2,
    tags=['green-taxi-data-2019-2020'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command = f'curl -sSLf {GREEN_TAXI_URL_TEMPLATE} > {GREEN_TAXI_OUTPUT_FILE_TEMPLATE}'
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": GREEN_TAXI_GCS_PATH_TEMPLATE,
            "local_file": GREEN_TAXI_OUTPUT_FILE_TEMPLATE,
        },
    )

    remove_temp_files = BashOperator(
        task_id="remove_temp_files",
        bash_command = f'rm {GREEN_TAXI_OUTPUT_FILE_TEMPLATE}'
    )

    download_dataset_task >> local_to_gcs_task >> remove_temp_files
