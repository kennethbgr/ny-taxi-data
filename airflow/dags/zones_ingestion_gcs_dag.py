import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
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
ZONES_URL = "https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv"
ZONE_CSV_FILE = "taxi+_zone_lookup.csv"
ZONE_PARQUET_FILE = "taxi_zone_lookup.parquet"
ZONE_CSV_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/" + ZONE_CSV_FILE
ZONE_PARQUET_OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/" + ZONE_PARQUET_FILE
ZONE_GCS_PATH_TEMPLATE = "raw/taxi_zone_lookup/" + ZONE_PARQUET_FILE

def format_to_parquet(src_file, dest_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, dest_file)


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
    "start_date": days_ago(1), 
    "depends_on_past": False,
    "retries": 1,
}


with DAG(
    dag_id="zones_ingestion_gcs_dag",
    schedule_interval="@once", #change to different settings if needed
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['taxi-zones-lookup'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command = f'curl -sSLf {ZONES_URL} > {ZONE_CSV_OUTPUT_FILE_TEMPLATE}'
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": ZONE_CSV_OUTPUT_FILE_TEMPLATE,
            "dest_file": ZONE_PARQUET_OUTPUT_FILE_TEMPLATE,
        },
    )
    
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": ZONE_GCS_PATH_TEMPLATE,
            "local_file": ZONE_PARQUET_OUTPUT_FILE_TEMPLATE,
        },
    )

    remove_temp_files = BashOperator(
        task_id="remove_temp_files",
        bash_command = f'rm {ZONE_CSV_OUTPUT_FILE_TEMPLATE} {ZONE_PARQUET_OUTPUT_FILE_TEMPLATE}'
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> remove_temp_files
