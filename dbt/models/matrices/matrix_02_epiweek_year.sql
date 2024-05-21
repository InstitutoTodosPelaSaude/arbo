{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        CONCAT(
            'SE', 
            TO_CHAR(epiweek_number, 'fm00'), 
            ' - ', 
            {{ get_month_name_from_epiweek_number('epiweek_number') }}
        ) as epiweek_month,
        EXTRACT('Year' FROM epiweek_enddate) as epiweek_year,
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
    epiweek_month,
    epiweek_year,
    pathogen,
    {{ matrices_metrics('result') }}
FROM source_data
GROUP BY epiweek_month, epiweek_year, pathogen
ORDER BY epiweek_year, epiweek_month, pathogen