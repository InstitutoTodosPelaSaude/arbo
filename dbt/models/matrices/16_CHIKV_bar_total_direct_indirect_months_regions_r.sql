{{ config(materialized='table') }}

{% set last_year_month_threshold = 12 %} -- 12 months

WITH 

source_data AS (
    SELECT
        *
    FROM {{ ref("16_CHIKV_bar_total_direct_indirect_months_regions_h") }}
),

months_ranked AS (
    SELECT 
        "Ano-Mês",
        RANK() OVER (
            ORDER BY "Ano-Mês" DESC
        ) AS month_rank
    FROM source_data
    GROUP BY "Ano-Mês"
),

months_to_include AS (
    SELECT 
        "Ano-Mês"
    FROM months_ranked
    WHERE month_rank <= {{ last_year_month_threshold }}
),

SELECT
    *
FROM source_data
WHERE 
    "Ano-Mês" IN (SELECT "Ano-Mês" FROM months_to_include)  