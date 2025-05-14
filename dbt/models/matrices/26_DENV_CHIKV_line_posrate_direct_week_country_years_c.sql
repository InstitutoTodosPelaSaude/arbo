{{ config(materialized='table') }}

{% set last_year_days_threshold = 70 %} -- 10 weeks

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("26_DENV_CHIKV_line_posrate_direct_week_country_years_h") }}
),

last_epiweek AS (
    SELECT MAX("semana") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "semana" >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}  