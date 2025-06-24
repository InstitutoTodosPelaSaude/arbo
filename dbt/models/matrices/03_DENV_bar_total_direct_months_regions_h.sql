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
        "DENV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'ns1_antigen', 'denv_pcr') AND 
        region != 'NOT REPORTED' AND
        "month" >= '{{ month_start }}'
    GROUP BY "month", region, pathogen
    ORDER BY "month", region, pathogen
),

transformation AS (
    SELECT
        "month",
        region,
        SUM(CASE WHEN pathogen = 'DENV' THEN totaltests ELSE 0 END) AS "DENV"
    FROM source_data
    GROUP BY "month", region
    ORDER BY "month", region
)

SELECT
    "month" as "Ano-Mês",
    MAX(CASE WHEN region = 'Centro-Oeste' THEN "DENV" ELSE 0 END) as "Centro-Oeste",
    MAX(CASE WHEN region = 'Nordeste' THEN "DENV" ELSE 0 END) as "Nordeste",
    MAX(CASE WHEN region = 'Norte' THEN "DENV" ELSE 0 END) as "Norte",
    MAX(CASE WHEN region = 'Sudeste' THEN "DENV" ELSE 0 END) as "Sudeste",
    MAX(CASE WHEN region = 'Sul' THEN "DENV" ELSE 0 END) as "Sul"
FROM transformation
GROUP BY "month"
ORDER BY "month"
