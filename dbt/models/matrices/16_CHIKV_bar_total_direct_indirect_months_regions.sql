{{ config(materialized='table') }}

{% set month_start = '2022-01' %}

WITH source_data AS (
    SELECT
        "month",
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        "CHIKV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'igm_serum', 'chikv_pcr') AND 
        region != 'NOT REPORTED' AND
        "month" >= '{{ month_start }}'
    GROUP BY "month", region, pathogen
    ORDER BY "month", region, pathogen
),

transformation AS (
    SELECT
        "month",
        region,
        SUM(CASE WHEN pathogen = 'CHIKV' THEN totaltests ELSE 0 END) AS "CHIKV"
    FROM source_data
    GROUP BY "month", region
    ORDER BY "month", region
)

SELECT
    "month" as "Ano-Mês",
    MAX(CASE WHEN region = 'Centro-Oeste' THEN "CHIKV" ELSE 0 END) as "Centro-Oeste",
    MAX(CASE WHEN region = 'Nordeste' THEN "CHIKV" ELSE 0 END) as "Nordeste",
    MAX(CASE WHEN region = 'Norte' THEN "CHIKV" ELSE 0 END) as "Norte",
    MAX(CASE WHEN region = 'Sudeste' THEN "CHIKV" ELSE 0 END) as "Sudeste",
    MAX(CASE WHEN region = 'Sul' THEN "CHIKV" ELSE 0 END) as "Sul"
FROM transformation
GROUP BY "month"
ORDER BY "month"
    