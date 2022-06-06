import os
import logging
from shutil import move

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator


PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')
AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")


DATASET = "tripdata"
TAXI_TYPES = {"yellow": "tpep_pickup_datetime", "fhv": "Pickup_datetime", "green": "lpep_pickup_datetime"}

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id="gcs_to_bg_dag2",
    schedule_interval="@daily", #change to different settings if needed
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['GCS-to-BigQuery'],
) as dag:

    for taxi_type, ds_col in TAXI_TYPES.items():

        gcs_to_gcs_task = GCSToGCSOperator(
            task_id = f"move_{taxi_type}_{DATASET}_files_task",
            source_bucket = BUCKET,
            source_object = f"raw/{taxi_type}_{DATASET}*.parquet",
            destination_bucket = BUCKET,
            destination_object = f"{taxi_type}/{taxi_type}_{DATASET}",
            move_object = True,
        )

        if taxi_type == "yellow":
            gcs_to_bg_ext_task = BigQueryCreateExternalTableOperator(
                    task_id=f"bq_{taxi_type}_{DATASET}_external_table_task",
                    table_resource={
                        "tableReference": {
                            "projectId": PROJECT_ID,
                            "datasetId": BIGQUERY_DATASET,
                            "tableId": f"{taxi_type}_{DATASET}_external_table",
                        },                
                        "externalDataConfiguration": {
                            # "autodetect": True,
                            "sourceFormat": "PARQUET",
                            "sourceUris": [f"gs://{BUCKET}/{taxi_type}/*"],
                            "schema": {
                                "fields": [
                                    {"name": 'VendorID', "type": 'INTEGER'},
                                    {"name": 'tpep_pickup_datetime', "type": 'TIMESTAMP'},
                                    {"name": 'tpep_dropoff_datetime', "type": 'TIMESTAMP'},
                                    {"name": 'passenger_count', "type": 'INTEGER'},
                                    {"name": 'trip_distance', "type": 'FLOAT'},
                                    {"name": 'RatecodeID', "type": 'INTEGER'},
                                    {"name": 'store_and_fwd_flag', "type": 'STRING'},
                                    {"name": 'PULocationID', "type": 'INTEGER'},
                                    {"name": 'DOLocationID', "type": 'INTEGER'},
                                    {"name": 'payment_type', "type": 'INTEGER'},
                                    {"name": 'fare_amount', "type": 'FLOAT'},
                                    {"name": 'extra', "type": 'FLOAT'},
                                    {"name": 'mta_tax', "type": 'FLOAT'},
                                    {"name": 'tip_amount', "type": 'FLOAT'},
                                    {"name": 'tolls_amount', "type": 'FLOAT'},
                                    {"name": 'improvement_surcharge', "type": 'FLOAT'},
                                    {"name": 'total_amount', "type": 'FLOAT'},
                                    {"name": 'congestion_surcharge', "type": 'FLOAT'},
                                    {"name": 'airport_fee', "type": 'FLOAT'},
                                ],
                            },
                        },
                        
                    },
                )
        elif taxi_type == "fhv":
            gcs_to_bg_ext_task = BigQueryCreateExternalTableOperator(
                    task_id=f"bq_{taxi_type}_{DATASET}_external_table_task",
                    table_resource={
                        "tableReference": {
                            "projectId": PROJECT_ID,
                            "datasetId": BIGQUERY_DATASET,
                            "tableId": f"{taxi_type}_{DATASET}_external_table",
                        },
                        "externalDataConfiguration": {
                            # "autodetect": True,
                            "sourceFormat": "PARQUET",
                            "sourceUris": [f"gs://{BUCKET}/{taxi_type}/*"],
                            "parquetOptions": {"enumAsString": True},
                            "decimalTargetTypes": ["NUMERIC", "BIGNUMERIC", "STRING"],
                            "schema": {
                                "fields": [
                                    {"name": 'dispatching_base_num', "type": 'INTEGER'},
                                    {"name": 'pickup_datetime', "type": 'TIMESTAMP'},
                                    {"name": 'dropOff_datetime', "type": 'TIMESTAMP'},
                                    {"name": 'PULocationID', "type": 'INTEGER'},
                                    {"name": 'DOLocationID', "type": 'INTEGER'},                            
                                    {"name": 'SR_Flag', "type": 'INTEGER'},
                                ],
                            },
                        },   
                    },
                )
        if taxi_type == "green":
            gcs_to_bg_ext_task = BigQueryCreateExternalTableOperator(
                    task_id=f"bq_{taxi_type}_{DATASET}_external_table_task",
                    table_resource={
                        "tableReference": {
                            "projectId": PROJECT_ID,
                            "datasetId": BIGQUERY_DATASET,
                            "tableId": f"{taxi_type}_{DATASET}_external_table",
                        },                    
                        "externalDataConfiguration": {
                            # "autodetect": True,
                            "sourceFormat": "PARQUET",
                            "sourceUris": [f"gs://{BUCKET}/{taxi_type}/*"],
                            "parquetOptions": {"enumAsString": True},
                            "decimalTargetTypes": ["NUMERIC", "BIGNUMERIC", "STRING"],
                            "schema": {
                                "fields": [
                                    {"name": 'VendorID', "type": 'INTEGER'},
                                    {"name": 'lpep_pickup_datetime', "type": 'TIMESTAMP'},
                                    {"name": 'lpep_dropoff_datetime', "type": 'TIMESTAMP'},
                                    {"name": 'passenger_count', "type": 'INTEGER'},
                                    {"name": 'trip_distance', "type": 'FLOAT'},
                                    {"name": 'RatecodeID', "type": 'INTEGER'},
                                    {"name": 'store_and_fwd_flag', "type": 'STRING'},
                                    {"name": 'PULocationID', "type": 'INTEGER'},
                                    {"name": 'DOLocationID', "type": 'INTEGER'},
                                    {"name": 'payment_type', "type": 'INTEGER'},
                                    {"name": 'fare_amount', "type": 'FLOAT'},
                                    {"name": 'extra', "type": 'FLOAT'},
                                    {"name": 'mta_tax', "type": 'FLOAT'},
                                    {"name": 'tip_amount', "type": 'FLOAT'},
                                    {"name": 'tolls_amount', "type": 'FLOAT'},
                                    {"name": 'improvement_surcharge', "type": 'FLOAT'},
                                    {"name": 'total_amount', "type": 'FLOAT'},                        
                                    {"name": 'trip_type', "type": 'INTEGER'},
                                ],
                            },
                        },
                        
                    },
                )
        CREATE_BQ_TBL_QUERY = (
            f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{taxi_type}_{DATASET}\
            PARTITION BY DATE({ds_col})\
            AS\
            SELECT * FROM {BIGQUERY_DATASET}.{taxi_type}_{DATASET}_external_table;")
        
        bg_ext_to_part_task = BigQueryInsertJobOperator(
            task_id=f"bq_create_{taxi_type}_{DATASET}_partioned_table_task",
            configuration={
                "query": {
                    "query": CREATE_BQ_TBL_QUERY,
                    "useLegacySql": False,
                }
            }
        )

        gcs_to_gcs_task >> gcs_to_bg_ext_task >> bg_ext_to_part_task
