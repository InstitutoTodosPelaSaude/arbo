{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        "month",
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        "DENV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'ns1_antigen', 'denv_pcr') AND 
        age_group != 'NOT REPORTED'
    GROUP BY "month", age_group, pathogen
    ORDER BY "month", age_group, pathogen
)

SELECT
    "month",
    age_group AS "faixas_etárias",
    SUM(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) AS "Dengue"
FROM source_data
GROUP BY "month", age_group
ORDER BY "month", age_group
    