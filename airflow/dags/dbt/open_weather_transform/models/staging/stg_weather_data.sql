{% set column_names = adapter.get_columns_in_relation(source('open_weather_raw', 'raw_weather_data')) | map(attribute='name') | list %}

WITH source AS (
    SELECT * FROM {{ source('open_weather_raw', 'raw_weather_data') }}
    WHERE id IN (
        3471355, 3405870, 3470127, 3664980, 3469058, 3467747, 3465038, 
        3464975, 3463237, 3399415, 3462377, 3397277, 3396016, 3395981, 
        3754131, 3394023, 3474574, 3452925, 3662762, 3390760, 3662574, 
        3451190, 6321026, 3388368, 3458611, 3386496, 3445026
    )
),

renamed AS (
    SELECT
        CAST(id AS STRING) AS city_id,
        CASE 
            WHEN id = 3471355 THEN 'Aracaju'
            WHEN id = 3754131 THEN 'Manaus'
            WHEN id = 3458611 THEN 'São Paulo'
            WHEN id = 3445026 THEN 'Vitória'
            WHEN id = 3474574 THEN 'Palmas'
            WHEN id = 6321026 THEN 'Salvador'
            WHEN id = 3467747 THEN 'Campo Grande'
            ELSE name 
        END AS city_name,
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