{% set column_names = adapter.get_columns_in_relation(source('open_weather_raw', 'raw_weather_data')) | map(attribute='name') | list %}

WITH source AS (
    SELECT * FROM {{ source('open_weather_raw', 'raw_weather_data') }}
),

renamed AS (
    SELECT
        CAST(id AS STRING) AS city_id,
        name AS city_name,
        state_abbreviation AS state,
        CAST(extracted_at AS TIMESTAMP) AS extracted_at_utc,
        main,
        wind,
        weather,
        clouds,
        dt AS api_timestamp_dt,

        rain,
        snow

    FROM source
)

SELECT * FROM renamed