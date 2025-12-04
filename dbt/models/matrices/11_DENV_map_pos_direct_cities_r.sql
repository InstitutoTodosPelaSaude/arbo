{{ config(materialized='table') }}

WITH
source_data AS (
    SELECT *
    FROM {{ ref("11_DENV_map_pos_direct_cities_h") }}
),

last_epiweek AS (
    SELECT MAX("Semana epidemiológica") as epiweek_enddate
    FROM source_data
)

SELECT *
FROM source_data
WHERE 
    "Semana epidemiológica" >= (SELECT epiweek_enddate FROM last_epiweek)