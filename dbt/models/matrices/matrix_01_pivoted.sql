{{ config(materialized='view') }}

WITH source_data AS (
    SELECT
        sample_id,
        test_kit,
        epiweek_enddate,
        epiweek_number,
        lab_id,
        "month",
        date_testing,
        location_ibge_code,
        CASE WHEN location IS NULL THEN 'NOT REPORTED' ELSE location END AS location,
        CASE WHEN region IS NULL THEN 'NOT REPORTED' ELSE region END AS region,
        CASE WHEN state IS NULL THEN 'NOT REPORTED' ELSE state END AS state,
        CASE WHEN state_code IS NULL THEN 'NOT REPORTED' ELSE state_code END AS state_code,
        lat,
        long,
        CASE WHEN country IS NULL THEN 'NOT REPORTED' ELSE country END AS country,
        CASE WHEN age_group IS NULL THEN 'NOT REPORTED' ELSE age_group END AS age_group,
        "DENV_test_result",
        "ZIKV_test_result",
        "CHIKV_test_result",
        "YFV_test_result",
        "MAYV_test_result",
        "OROV_test_result",
        "WNV_test_result"
    FROM {{ ref("combined_05_location") }}
    WHERE epiweek_enddate < CURRENT_DATE AND epiweek_enddate >= '2022-01-01'
)
SELECT
    combined.*,
    combined_pivoted.*
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