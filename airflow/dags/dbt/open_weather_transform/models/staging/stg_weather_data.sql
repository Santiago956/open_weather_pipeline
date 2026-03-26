{% set column_names = adapter.get_columns_in_relation(source('open_weather_raw', 'raw_weather_data')) | map(attribute='name') | list %}

WITH source AS (
    SELECT * FROM {{ source('open_weather_raw', 'raw_weather_data') }}
    WHERE id IN (
        3471872, 3405968, 3470127, 3664980, 3469058, 3467748,
        3465038, 3466537, 3463237, 3399415, 3462377, 3397273,
        3396016, 3395981, 3663517, 3394023, 3455543, 3452925,
        3662762, 3390760, 3662995, 3451190, 3450554, 3388368,
        3448439, 3386496, 3444924)
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