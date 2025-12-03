{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        "month",
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE -- FILTER USEFUL TEST KITS FOR EACH PATHOGEN
        CASE 
            WHEN "MAYV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('mayv_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "OROV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('orov_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "CHIKV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('chikv_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "ZIKV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('zika_pcr', 'ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "WNV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('ns1_antigen', 'igm_serum', 'arbo_pcr_3')
            WHEN "YFV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('yfv_pcr', 'igm_serum')
            ELSE TRUE
        END
    AND age_group != 'NOT REPORTED'
    GROUP BY "month", age_group, pathogen
    ORDER BY "month", age_group, pathogen
)

SELECT
    "month",
    age_group AS "faixas_etárias",
    SUM(CASE WHEN pathogen = 'CHIKV' THEN "Pos" ELSE 0 END) AS "Chikungunya",
    SUM(CASE WHEN pathogen = 'OROV' THEN "Pos" ELSE 0 END) AS "Oropouche",
    SUM(CASE WHEN pathogen = 'ZIKV' THEN "Pos" ELSE 0 END) AS "Zika",
    SUM(CASE WHEN pathogen = 'YFV' THEN "Pos" ELSE 0 END) AS "Febre amarela",
    SUM(CASE WHEN pathogen = 'MAYV' THEN "Pos" ELSE 0 END) AS "Mayaro",
    SUM(CASE WHEN pathogen = 'WNV' THEN "Pos" ELSE 0 END) AS "West Nile"
FROM source_data
GROUP BY "month", age_group
ORDER BY "month", age_group

    