{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        "CHIKV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'igm_serum', 'chikv_pcr')
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
),

chikv_data AS (
    SELECT
        epiweek_enddate as "Semanas epidemiológicas",
        MAX(CASE WHEN pathogen = 'CHIKV' THEN "posrate" * 100 ELSE NULL END) as "Positividade (%)",
        SUM(CASE WHEN pathogen = 'CHIKV' THEN "Pos" ELSE 0 END)::int as "Positivos",
        SUM(CASE WHEN pathogen = 'CHIKV' THEN "Neg" ELSE 0 END)::int as "Negativos"
    FROM source_data
    GROUP BY epiweek_enddate
)

SELECT
    "Semanas epidemiológicas",
    "Positividade (%)",
    "Positivos",
    "Negativos",
    ("Positivos" + "Negativos")::int as "Total de casos"
FROM chikv_data
ORDER BY "Semanas epidemiológicas"