{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        location_ibge_code,
        location,
        state,
        lat,
        long,
        result,
        pathogen
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE -- FILTER USEFUL TEST KITS FOR EACH PATHOGEN
        CASE 
            WHEN "DENV_test_result" IN ('Pos', 'Neg') THEN test_kit NOT IN ('igg_serum', 'igm_serum')
            WHEN "MAYV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('mayv_pcr')
            WHEN "OROV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('orov_pcr')
            WHEN "CHIKV_test_result" IN ('Pos', 'Neg') THEN test_kit NOT IN ('igg_serum')
            WHEN "ZIKV_test_result" IN ('Pos', 'Neg') THEN test_kit IN ('zika_pcr')
            ELSE TRUE
        END
)
SELECT
    epiweek_enddate,
    location_ibge_code,
    location,
    state,
    lat,
    long,
    pathogen,
    {{ matrices_metrics('result') }}
FROM source_data
GROUP BY epiweek_enddate, location_ibge_code, location, state, lat, long, pathogen
ORDER BY epiweek_enddate, location, pathogen