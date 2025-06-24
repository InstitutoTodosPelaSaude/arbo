{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        "DENV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'ns1_antigen', 'denv_pcr')
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
)

SELECT
    epiweek_enddate as "Semanas epidemiológicas",
    MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" * 100 ELSE NULL END) as "Positividade (%)",
    SUM(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) as "Positivos",
    SUM(CASE WHEN pathogen = 'DENV' THEN "Neg" ELSE 0 END) as "Negativos"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate