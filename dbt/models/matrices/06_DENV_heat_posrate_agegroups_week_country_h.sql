{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        "DENV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'ns1_antigen', 'denv_pcr') AND 
        age_group != 'NOT REPORTED'
    GROUP BY epiweek_enddate, age_group, pathogen
    ORDER BY epiweek_enddate, age_group, pathogen
)

SELECT
    age_group as "faixas etárias",
    epiweek_enddate as "semana epidemiológica",
    MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" * 100 ELSE NULL END) as "percentual"
FROM source_data
GROUP BY epiweek_enddate, age_group
ORDER BY epiweek_enddate, age_group desc
    