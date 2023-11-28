{{config(materialized='table')}}

WITH source_data AS (
    SELECT
    *
    FROM
    {{ ref("hlagyn_02_fix_values") }}
)
SELECT
    sample_id,
    test_id,
    patient_id,
    test_kit,
    age,
    gender,
    date_testing,
    state,
    location,
    dengue_result AS DENV_test_result,
    zika_result AS ZIKV_test_result,
    chikungunya_result AS CHIKV_test_result,
    NULL AS YFV_test_result,
    NULL AS MAYV_test_result,
    NULL AS OROV_test_result,
    NULL AS WNV_test_result,
    1 AS qty_original_lines,
    file_name

FROM source_data