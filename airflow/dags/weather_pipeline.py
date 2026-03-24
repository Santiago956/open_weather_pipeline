from airflow.sdk import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryUpsertTableOperator
from include.scripts.extract_weather import fetch_weather_data, upload_to_gcs
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig, ExecutionConfig # Adicione ExecutionConfig
from cosmos.constants import InvocationMode 
from cosmos.profiles import GoogleCloudServiceAccountFileProfileMapping # Importação correta
import pendulum
from datetime import timedelta
import logging
import os

project_config = ProjectConfig(
    dbt_project_path="/usr/local/airflow/dags/dbt/open_weather_transform",
)


profile_config = ProfileConfig(
    profile_name="google_cloud_bigquery",
    target_name="dev",
    profile_mapping=GoogleCloudServiceAccountFileProfileMapping(
        conn_id="google_cloud_default",
        profile_args={
            "project": os.getenv("GCP_PROJECT"),
            "dataset": os.getenv("BQ_DATASET"),
            # Forçamos o dbt a ler este arquivo específico
            "keyfile": "/usr/local/airflow/include/gcp_credentials.json",
        },
    ),
)
execution_config = ExecutionConfig(
    dbt_executable_path="/usr/local/pyenv/dbt-venv/bin/dbt",
    invocation_mode=InvocationMode.SUBPROCESS,
)

render_config = RenderConfig(
    emit_datasets=True,
    test_behavior="after_each"
)

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
    def extract_from_api():
        api_key = os.getenv("OPEN_WEATHER_API_KEY") or Variable.get("OPEN_WEATHER_API_KEY", default_var=None)
        
        if not api_key:
            logger.error("API Key não encontrada no .env nem no Airflow Variables.")
            raise ValueError("OPEN_WEATHER_API_KEY is missing!")
            
        return fetch_weather_data(api_key)
    
    @task()
    def transform_load_gcs(data):
        bucket = os.getenv("GCP_MAIN_BUCKET") or Variable.get("GCP_MAIN_BUCKET", default_var=None)
        if not bucket:
            raise ValueError("GCP_MAIN_BUCKET is missing!")
                
        return upload_to_gcs(data, bucket)
    
    
    load_big_query = BigQueryUpsertTableOperator(
        task_id='load_to_external_table',
        dataset_id=os.getenv("BQ_DATASET", "weather_bronze"),
        project_id=os.getenv("GCP_PROJECT", "open-weather-pipeline"),
        table_resource={
            "tableReference": {"tableId": "raw_weather_data"},
            "externalDataConfiguration": {
                "sourceUris": [f"gs://{os.getenv('GCP_MAIN_BUCKET')}/raw/*.json"],
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "autodetect": False, 
                "ignoreUnknownValues": True,
                "schema": {
                    "fields": [
                        {"name": "id", "type": "INTEGER"},
                        {"name": "name", "type": "STRING"},
                        {"name": "state_abbreviation", "type": "STRING"},
                        {"name": "extracted_at", "type": "TIMESTAMP"},
                        {"name": "dt", "type": "INTEGER"},
                        {"name": "main", "type": "RECORD", "fields": [
                            {"name": "temp", "type": "FLOAT"},
                            {"name": "feels_like", "type": "FLOAT"},
                            {"name": "temp_min", "type": "FLOAT"},
                            {"name": "temp_max", "type": "FLOAT"},
                            {"name": "pressure", "type": "INTEGER"},
                            {"name": "humidity", "type": "INTEGER"}
                        ]},
                        {"name": "wind", "type": "RECORD", "fields": [
                            {"name": "speed", "type": "FLOAT"},
                            {"name": "deg", "type": "INTEGER"}
                        ]},
                        {"name": "clouds", "type": "RECORD", "fields": [
                            {"name": "all", "type": "INTEGER"}
                        ]},
                        {"name": "weather", "type": "RECORD", "mode": "REPEATED", "fields": [
                            {"name": "main", "type": "STRING"},
                            {"name": "description", "type": "STRING"}
                        ]},
                        # Mudamos para JSON para aceitar o campo "1h" internamente
                        {"name": "rain", "type": "JSON", "mode": "NULLABLE"},
                        {"name": "snow", "type": "JSON", "mode": "NULLABLE"}
                    ]
                }
            },
        },
        gcp_conn_id="google_cloud_default",
    )

    transform_data = DbtTaskGroup(
        group_id="dbt_transformation",
        project_config=project_config, # Use a variável definida acima
        profile_config=profile_config,
        execution_config=execution_config,
        render_config=render_config,
    )
    data = extract_from_api()
    upload_done = transform_load_gcs(data)
    
    upload_done >> load_big_query >> transform_data

weather_pipeline()