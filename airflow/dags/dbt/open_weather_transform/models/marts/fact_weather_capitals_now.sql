WITH intermediate AS (
    SELECT * FROM {{ ref('int_weather_metrics') }}
),

latest_records AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY extracted_at_utc DESC) as row_num
    FROM intermediate
)

SELECT
    city_name,
    state,
    extracted_at_br,
    temp_celsius,
    feels_like_celsius,
    humidity_percent,
    wind_speed_mps,
    cloudiness_percent,
    rain_1h_mm,
    snow_1h_mm, 
    weather_main,
    weather_description,

    CASE 
        WHEN temp_celsius > 35 THEN 'Extreme Heat'
        WHEN temp_celsius < 15 THEN 'Cold'
        WHEN rain_1h_mm > 0 THEN 'Rainy' 
        WHEN snow_1h_mm > 0 THEN 'Snowy'
        ELSE 'Normal'
    END AS weather_alert

FROM latest_records
WHERE row_num = 1
ORDER BY city_name ASC