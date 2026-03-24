import requests
import json
from datetime import datetime
import os
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import logging

logger = logging.getLogger(__name__)

BRAZILIAN_CAPITALS = [
    {"cidade": "Aracaju", "estado": "SE", "lat": -10.9472, "lon": -37.0731},
    {"cidade": "Belém", "estado": "PA", "lat": -1.4550, "lon": -48.4902},
    {"cidade": "Belo Horizonte", "estado": "MG", "lat": -19.9167, "lon": -43.9345},
    {"cidade": "Boa Vista", "estado": "RR", "lat": 2.8235, "lon": -60.6758},
    {"cidade": "Brasília", "estado": "DF", "lat": -15.7801, "lon": -47.9292},
    {"cidade": "Campo Grande", "estado": "MS", "lat": -20.4428, "lon": -54.6464},
    {"cidade": "Cuiabá", "estado": "MT", "lat": -15.6010, "lon": -56.0974},
    {"cidade": "Curitiba", "estado": "PR", "lat": -25.4284, "lon": -49.2733},
    {"cidade": "Florianópolis", "estado": "SC", "lat": -27.5948, "lon": -48.5482},
    {"cidade": "Fortaleza", "estado": "CE", "lat": -3.7172, "lon": -38.5433},
    {"cidade": "Goiânia", "estado": "GO", "lat": -16.6869, "lon": -49.2648},
    {"cidade": "João Pessoa", "estado": "PB", "lat": -7.1150, "lon": -34.8631},
    {"cidade": "Macapá", "estado": "AP", "lat": 0.0340, "lon": -51.0660},
    {"cidade": "Maceió", "estado": "AL", "lat": -9.6658, "lon": -35.7350},
    {"cidade": "Manaus", "estado": "AM", "lat": -3.1190, "lon": -60.0217},
    {"cidade": "Natal", "estado": "RN", "lat": -5.7945, "lon": -35.2110},
    {"cidade": "Palmas", "estado": "TO", "lat": -10.2128, "lon": -48.3603},
    {"cidade": "Porto Alegre", "estado": "RS", "lat": -30.0346, "lon": -51.2177},
    {"cidade": "Porto Velho", "estado": "RO", "lat": -8.7612, "lon": -63.9039},
    {"cidade": "Recife", "estado": "PE", "lat": -8.0539, "lon": -34.8811},
    {"cidade": "Rio Branco", "estado": "AC", "lat": -9.9740, "lon": -67.8070},
    {"cidade": "Rio de Janeiro", "estado": "RJ", "lat": -22.9068, "lon": -43.1729},
    {"cidade": "Salvador", "estado": "BA", "lat": -12.9714, "lon": -38.5014},
    {"cidade": "São Luís", "estado": "MA", "lat": -2.5307, "lon": -44.3068},
    {"cidade": "São Paulo", "estado": "SP", "lat": -23.5505, "lon": -46.6333},
    {"cidade": "Teresina", "estado": "PI", "lat": -5.0920, "lon": -42.8034},
    {"cidade": "Vitória", "estado": "ES", "lat": -20.3155, "lon": -40.3128}
]

def fetch_weather_data(api_key):

    all_data = []
    timestamp = datetime.now().isoformat()

    for capital in BRAZILIAN_CAPITALS:
        url = f"https://api.openweathermap.org/data/2.5/weather?lat={capital['lat']}&lon={capital['lon']}&appid={api_key}&units=metric"

        try:
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            
            data = response.json()

            if 'rain' in data:
                data['rain']['last_1h'] = data['rain'].pop('1h', 0.0)
            else:
                data['rain'] = {'last_1h': 0.0}

            if 'snow' in data:
                data['snow']['last_1h'] = data['snow'].pop('1h', 0.0)
            else:
                data['snow'] = {'last_1h': 0.0}


            data['extracted_at'] = timestamp
            data['state_abbreviation'] = capital['estado']
            all_data.append(data)
            logger.info(f"Successfully fetched data for {capital['cidade']}")

        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for {capital['cidade']}: {e}")
            continue
    
    logger.info(f"Fetched weather data for {len(all_data)} out of {len(BRAZILIAN_CAPITALS)} capitals.")
    return all_data

def upload_to_gcs(data, bucket_name, gcp_conn_id="google_cloud_default"):
    if not data:
        logger.warning("No data to upload to GCS.")
        return
    
    try:
        hook = GCSHook(gcp_conn_id=gcp_conn_id)
        now = datetime.now()
        object_name = f"raw/{now.strftime('%Y/%m/%d')}/weather_data_{now.strftime('%H%M')}.json"
        
        
        jsonl_data = "\n".join([json.dumps(record) for record in data])

        hook.upload(
            bucket_name=bucket_name,
            object_name=object_name,
            data=jsonl_data,
            mime_type='application/json'
        )
        logger.info(f"Data successfully uploaded to GCS: {object_name}")
        return bucket_name
    except Exception as e:
        logger.error(f"Error uploading data to GCS: {e}")
        raise