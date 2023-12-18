{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        sample_id,
        test_kit,
        epiweek_enddate,
        combined_pivoted.*
    FROM
    FROM {{ ref("combined_05_location") }}
),
SELECT
    *
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