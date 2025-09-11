{{ config(materialized='table') }}

source_data AS (
    SELECT *
    FROM {{ ref("11_DENV_map_pos_direct_cities_h") }}
),

last_epiweek AS (
    SELECT MAX("Semanas epidemiologicas") as epiweek_enddate
    FROM source_data
)

SELECT *
FROM source_data
WHERE 
    "Semanas epidemiologicas" >= (SELECT epiweek_enddate FROM last_epiweek)