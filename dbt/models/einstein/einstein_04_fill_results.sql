{{ 
    config(
        materialized='table',
    ) 
}}

{%set test_result_columns = [
        'denv_test_result',
        'zikv_test_result',
        'chikv_test_result',
        'yfv_test_result',
        'mayv_test_result',
        'orov_test_result',
        'wnv_test_result'
    ]
%}

WITH source_data AS(
    SELECT
    *
    FROM {{ ref("einstein_03_pivot_results") }}
)
SELECT
    -- add all coluns EXEPT test_result_columns and id_columns
    -- to the id_columns list
    sample_id,
    test_id,	
    test_kit,
    sex,
    age,
    location,
    state,
    result,
    date_testing,
    file_name,

    -- Mapping test results
    --  1: Pos if AT LEAST ONE of the results is positive
    --  0: Neg if there is no POS results and there is at least one NEG result
    -- -1: NT if there is no POS or NEG results
    {% for pathogen in test_result_columns %}
        CASE 
            WHEN MAX({{pathogen}}) OVER( PARTITION BY sample_id) = 1 THEN 'Pos'
            WHEN MAX({{pathogen}}) OVER( PARTITION BY sample_id) = 0 THEN 'Neg'
            WHEN MAX({{pathogen}}) OVER( PARTITION BY sample_id) = -1 THEN 'NT'
        END AS {{pathogen}}
        {% if not loop.last %}
            ,
        {% endif %}
    {% endfor %}
    ,

    -- Count the number of lines in each sample_id
    COUNT(*) OVER(
        PARTITION BY sample_id
    ) AS qty_original_lines

FROM source_data
