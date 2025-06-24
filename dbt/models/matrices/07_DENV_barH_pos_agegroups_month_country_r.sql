{{ config(materialized='table') }}

{% set last_year_month_threshold = 6 %} -- 6 months

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("07_DENV_barH_pos_agegroups_month_country_h") }}
),

months_ranked AS (
    SELECT 
        "month",
        RANK() OVER (
            ORDER BY "month" DESC
        ) AS month_rank
    FROM source_data
    GROUP BY "month"
),

months_to_include AS (
    SELECT 
        "month"
    FROM months_ranked
    WHERE month_rank <= {{ last_year_month_threshold }}
)

SELECT
    *
FROM source_data
WHERE 
    "month" IN (SELECT "month" FROM months_to_include)  