{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        "month",
        age_group,
        result,
        pathogen
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE -- FILTER USEFUL TEST KITS FOR EACH PATHOGEN
        CASE 
            WHEN "DENV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('arbo_pcr_3', 'ns1_antigen', 'denv_pcr')
            WHEN "MAYV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('mayv_pcr')
            WHEN "OROV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('orov_pcr')
            WHEN "CHIKV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('chikv_pcr')
            WHEN "ZIKV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('zika_pcr')
            ELSE TRUE
        END
)
SELECT
    "month",
    age_group,
    pathogen,
    {{ matrices_metrics('result') }}
FROM source_data
GROUP BY "month", age_group, pathogen
ORDER BY "month", age_group, pathogen