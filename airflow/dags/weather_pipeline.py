from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from include.scripts.extract_weather import fetch_weather_data, upload_to_gcs
import pendulum
from datetime import timedelta
import logging
import os

logger = logging.getLogger(__name__)


default_args = {
    'owner': 'Santiago',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='open_weather_to_gcs',
    default_args=default_args,
    description='A DAG to fetch weather data for Brazilian capitals and upload to GCS',
    schedule='@hourly',
    start_date=pendulum.datetime(2026, 3, 20, tz="UTC"),
    catchup=False,
    tags=['weather', 'gcs', 'bronze'],
)

def weather_pipeline():

    gcp_project = os.getenv("GCP_PROJECT") or Variable.get("GCP_PROJECT", default_var=None)
    bq_dataset = os.getenv("BQ_DATASET") or Variable.get("BQ_DATASET", default_var=None)
    bucket_name = os.getenv("GCP_MAIN_BUCKET") or Variable.get("GCP_MAIN_BUCKET", default_var=None)

    @task()
    def extract():
        api_key = os.getenv("OPEN_WEATHER_API_KEY") or Variable.get("OPEN_WEATHER_API_KEY", default_var=None)
        
        if not api_key:
            logger.error("API Key não encontrada no .env nem no Airflow Variables.")
            raise ValueError("OPEN_WEATHER_API_KEY is missing!")
            
        return fetch_weather_data(api_key)
    
    @task()
    def load(data):
        bucket = os.getenv("GCP_MAIN_BUCKET") or Variable.get("GCP_MAIN_BUCKET", default_var=None)
        if not bucket:
            raise ValueError("GCP_MAIN_BUCKET is missing!")
                
        return upload_to_gcs(data, bucket)
    

    
    create_external_table = BigQueryCreateExternalTableOperator(
        task_id='create_external_table',
        destination_project_dataset_table=(
            "{{ var.value.GCP_PROJECT }}."
            "{{ var.value.BQ_DATASET }}."
            "raw_weather_data"
        ),
        bucket="{{ var.value.GCP_MAIN_BUCKET }}",
        source_objects=["raw/*/*/*/*.json"],
        source_format='NEWLINE_DELIMITED_JSON',
        gcp_conn_id="google_cloud_default",
        autodetect=True,
    )

    data = extract()
    upload_done = load(data)
    
    upload_done >> create_external_table

weather_pipeline()