{{ config(materialized='table') }}

{% set last_year_days_threshold = 364 %} -- 52 weeks

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("15_CHIKV_line_bar_posrate_posneg_direct_indirect_week_country_h") }}
),

last_epiweek AS (
    SELECT MAX("Semanas epidemiológicas") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "Semanas epidemiológicas" > (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}  