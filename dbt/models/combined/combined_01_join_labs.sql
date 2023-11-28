

{{ config(materialized='table') }}

{%
    set columns = [
        'sample_id',
        'test_id',
        'test_kit',
        'gender',
        'age',
        'location',
        'date_testing',
        'state',
        'patient_id',
        'file_name',
        'denv_test_result',
        'zikv_test_result',
        'chikv_test_result',
        'yfv_test_result',
        'mayv_test_result',
        'orov_test_result',
        'wnv_test_result',
        'qty_original_lines',
        'created_at',
        'updated_at'
    ]
%}

WITH source_data AS (

    SELECT 
    'EINSTEIN' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("einstein_06_final") }}
    
    UNION

    SELECT 
    'HILAB' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("hilab_05_final") }}
)
SELECT
    *
FROM source_data