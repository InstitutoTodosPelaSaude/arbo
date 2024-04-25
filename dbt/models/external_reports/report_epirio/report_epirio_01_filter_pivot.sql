{{ config(materialized='table') }}

WITH source_data AS(
    SELECT * 
    FROM {{ ref("combined_for_reports") }}
    WHERE location_ibge_code = 3304557 -- FILTER ONLY RIO DE JANEIRO CITY
)

SELECT
    combined.date_testing,
    combined.test_kit,
    combined.age_group,
    combined.location,
    combined.state_code,
    combined.location_ibge_code,
    combined_pivoted.pathogen,
    combined_pivoted.result
FROM
    source_data combined
CROSS JOIN LATERAL (
    VALUES
        (combined."DENV_test_result",  'DENV'),
        (combined."ZIKV_test_result",  'ZIKV'),
        (combined."CHIKV_test_result", 'CHIKV'),
        (combined."YFV_test_result",   'YFV'),
        (combined."MAYV_test_result",  'MAYV'),
        (combined."OROV_test_result",  'OROV'),
        (combined."WNV_test_result",   'WNV')
) AS combined_pivoted(result, pathogen)
WHERE combined_pivoted.result <> 'NT'