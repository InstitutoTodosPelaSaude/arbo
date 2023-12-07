{{ 
    config(
        materialized='table',
    ) 
}}

WITH source_data AS(
    SELECT
    *,
    ROW_NUMBER() OVER(
        PARTITION BY sample_id
        ORDER BY sample_id
    ) AS row_number
    FROM {{ ref("einstein_04_fill_results") }}
)
SELECT
    
    sample_id,
    test_id,
    test_kit,
    sex,
    age,
    location,
    state,
    --result,
    date_testing,
    file_name,
    denv_test_result,
    zikv_test_result,
    chikv_test_result,
    yfv_test_result,
    mayv_test_result,
    orov_test_result,
    wnv_test_result,
    qty_original_lines

FROM source_data
-- This column is used to filter out duplicate rows
WHERE row_number = 1
