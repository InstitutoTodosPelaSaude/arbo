{{ config(materialized='table') }}

WITH 
source_data AS (
    SELECT
        *
    FROM {{ ref("26_DENV_CHIKV_line_posrate_direct_week_country_years") }}
)

SELECT
    "semana",
    "Dengue",
    "Chikungunya"
FROM source_data
ORDER BY epiweek_enddate