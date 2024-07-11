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
            WHEN "MAYV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('mayv_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "OROV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('orov_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "CHIKV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('chikv_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "ZIKV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('zika_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "WNV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('ns1_antigen', 'igm_serum', 'arbo_pcr_3')
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