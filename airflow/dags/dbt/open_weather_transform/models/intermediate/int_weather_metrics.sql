{{ config(
    materialized='table',
    partition_by={
      "field": "extracted_at_utc",
      "data_type": "timestamp",
      "granularity": "day"
    }
) }}

WITH staging AS (
    SELECT * FROM {{ ref('stg_weather_data') }}
),

flattened AS (
    SELECT
        city_id,
        city_name,
        state,
        extracted_at_utc,
        -- Conversão para fuso horário de Brasília (essencial para o BI)
        datetime(extracted_at_utc, "America/Sao_Paulo") AS extracted_at_br,
        
        CAST(main.temp AS FLOAT64) AS temp_celsius,
        CAST(main.feels_like AS FLOAT64) AS feels_like_celsius,
        CAST(main.temp_min AS FLOAT64) AS temp_min_celsius,
        CAST(main.temp_max AS FLOAT64) AS temp_max_celsius,
        CAST(main.pressure AS INT64) AS pressure_hpa,
        CAST(main.humidity AS INT64) AS humidity_percent,

        CAST(wind.speed AS FLOAT64) AS wind_speed_mps,
        CAST(wind.deg AS INT64) AS wind_direction_deg,
        CAST(clouds.all AS INT64) AS cloudiness_percent,

        CAST(rain.last_1h AS FLOAT64) AS rain_1h_mm,
        CAST(snow.last_1h AS FLOAT64) AS snow_1h_mm,

        weather[SAFE_OFFSET(0)].main AS weather_main,
        weather[SAFE_OFFSET(0)].description AS weather_description

    FROM staging
)       

SELECT * FROM flattened