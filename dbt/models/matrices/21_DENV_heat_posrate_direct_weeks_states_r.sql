{{ config(materialized='table') }}

{% set last_year_days_threshold = 364 %} -- 52 weeks

WITH source_data AS (
    SELECT
        *
    FROM {{ ref("21_DENV_heat_posrate_direct_weeks_states") }}
),

last_epiweek AS (
    SELECT MAX("semanas_epidemiologicas") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "Completude final" IN ('Apenas 12 meses', 'Alta qualidade') AND
    "semanas_epidemiologicas" >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}