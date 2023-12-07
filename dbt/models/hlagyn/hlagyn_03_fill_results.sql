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
    sex,
    date_testing,
    state,
    location,
    CASE
        WHEN dengue_result=1 THEN 'Pos' 
        WHEN dengue_result=0 THEN 'Neg'
        WHEN dengue_result=-1 THEN 'NT'
        ELSE NULL
    END AS DENV_test_result,
    CASE
        WHEN zika_result=1 THEN 'Pos' 
        WHEN zika_result=0 THEN 'Neg'
        WHEN zika_result=-1 THEN 'NT'
        ELSE NULL
    END AS ZIKV_test_result,
    CASE
        WHEN chikungunya_result=1 THEN 'Pos' 
        WHEN chikungunya_result=0 THEN 'Neg'
        WHEN chikungunya_result=-1 THEN 'NT'
        ELSE NULL
    END AS CHIKV_test_result,
    'NT' AS YFV_test_result,
    'NT' AS MAYV_test_result,
    'NT' AS OROV_test_result,
    'NT' AS WNV_test_result,
    1 AS qty_original_lines,
    file_name

FROM source_data