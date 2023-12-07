{{config(materialized='table')}}

WITH source_data AS (
    SELECT
        *,
        ROW_NUMBER() OVER(
            PARTITION BY sample_id
            ORDER BY sample_id
        ) AS row_number
    FROM
    {{ ref("hlagyn_03_fill_results") }}
)
SELECT
    sample_id,
    test_id,
    patient_id,
    test_kit,
    age,
    sex,
    date_testing,
    state,
    location,
    DENV_test_result,
    ZIKV_test_result,
    CHIKV_test_result,
    YFV_test_result,
    MAYV_test_result,
    OROV_test_result,
    WNV_test_result,
    qty_original_lines,
    file_name
FROM source_data
-- This column is used to filter out duplicate rows
WHERE row_number = 1