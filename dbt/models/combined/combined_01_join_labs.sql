

{{ config(materialized='table') }}

{%
    set columns = [
        'sample_id',
        'test_id',
        'test_kit',
        'sex',
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
    
    UNION
    
    SELECT 
    'HLAGYN' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("hlagyn_05_final") }}

    UNION

    SELECT
    'SABIN' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("sabin_07_final") }}

    UNION

    SELECT
    'FLEURY' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("fleury_06_final") }}

    UNION

    SELECT
    'DBMOL' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("dbmol_final") }}

    UNION

    SELECT
    'TARGET' as lab_id,
    {{ columns | join(', ') }}
    FROM {{ ref("target_final") }}
    
)
SELECT
    *
FROM source_data