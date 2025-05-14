{{ config(materialized='table') }}

{% set last_year_days_threshold = 364 %} -- 52 weeks

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("26_DENV_CHIKV_line_posrate_direct_week_country_years") }}
),

last_epiweek AS (
    SELECT MAX("epiweek_enddate") as epiweek_enddate
    FROM source_data
)

SELECT
    "semana",
    "Dengue",
    "Chikungunya"
FROM source_data
WHERE 
    "epiweek_enddate" >= (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }} 
ORDER BY epiweek_enddate 