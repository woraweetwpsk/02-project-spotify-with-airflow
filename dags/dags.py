from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator

from datetime import datetime, timedelta

from transform import transform_data
from check_dataset import check_and_create_dataset_table

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 30),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='spotify_dag',
    default_args=default_args,
    description='cleansing_spotify_data',
    schedule_interval=None
):

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id = "upload_to_gcs",
        src = "/opt/airflow/data/raw/*.csv",
        dst = "most-streamed-spotify-songs-2024.csv",
        bucket = "raw-data-spotify",
        gcp_conn_id = "gcp_conn"
    )
    
    transform_data = PythonOperator(
        task_id = "transfrom_data",
        python_callable = transform_data
    )
    
    check_and_create_dataset_table = PythonOperator(
        task_id = "cheack_and_create_dataset_table",
        python_callable = check_and_create_dataset_table,
        op_kwargs = {"dataset_id": "spotify",
                    "table_id": "most-streamed-spotify"}
    )
    
    gcs_to_bigquery = GCSToBigQueryOperator(
        task_id = "gcs_to_bigquery",
        bucket = "transform-data-spotify",
        source_objects = ["*.parquet"],
        source_format="PARQUET",
        destination_project_dataset_table = "spotify.most-streamed-spotify",
        write_disposition ="WRITE_TRUNCATE",
        gcp_conn_id = "gcp_conn"
    )

upload_to_gcs >> transform_data >> check_and_create_dataset_table >> gcs_to_bigquery
    
    
    
    