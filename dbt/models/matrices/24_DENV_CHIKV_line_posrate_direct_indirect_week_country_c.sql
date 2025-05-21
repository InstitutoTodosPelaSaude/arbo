{{ config(materialized='table') }}

{% set last_year_days_threshold = 70 %} -- 10 weeks

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("24_DENV_CHIKV_line_posrate_direct_indirect_week_country_h") }}
),

last_epiweek AS (
    SELECT MAX("Semana") as epiweek_enddate
    FROM source_data
)

SELECT
    *
FROM source_data
WHERE 
    "Semana" > (SELECT epiweek_enddate FROM last_epiweek) - {{ last_year_days_threshold }}  