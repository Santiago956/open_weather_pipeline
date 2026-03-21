from airflow.sdk import dag, task
from datetime import timedelta
import pendulum
from airflow.models import Variable
from include.scripts.extract_weather import fetch_weather_data, upload_to_gcs
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

    @task()
    def extract():
        api_key = os.getenv("OPEN_WEATHER_API_KEY") or Variable.get("OPEN_WEATHER_API_KEY", default_var=None)
        
        if not api_key:
            logger.error("API Key não encontrada no .env nem no Airflow Variables.")
            raise ValueError("OPEN_WEATHER_API_KEY is missing!")
            
        return fetch_weather_data(api_key)
    
    @task()
    def load(data):
        bucket_name = os.getenv("GCP_MAIN_BUCKET") or Variable.get("GCP_MAIN_BUCKET", default_var=None)
        if not bucket_name:
            logger.error("GCP_MAIN_BUCKET is not set in environment variables or Airflow Variables.")
            raise ValueError("GCP_MAIN_BUCKET is missing!")

        upload_to_gcs(data, bucket_name)

    weather_data = extract()
    load(weather_data)

weather_pipeline()