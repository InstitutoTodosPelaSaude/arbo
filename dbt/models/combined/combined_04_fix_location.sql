

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * 
    FROM {{ ref("combined_03_dates") }}

)
SELECT
    source_data.lab_id,	
    source_data.sample_id,	
    source_data.test_id,	
    source_data.test_kit,	
    source_data.gender,	
    source_data.age,
    CASE
        WHEN fl.source_location IS NULL THEN source_data.location
        ELSE fl.target_location
    END AS location,
    source_data.date_testing,	
    source_data.state,	
    source_data.patient_id,	
    source_data.file_name,	
    source_data.denv_test_result,	
    source_data.zikv_test_result,	
    source_data.chikv_test_result,	
    source_data.yfv_test_result,	
    source_data.mayv_test_result,	
    source_data.orov_test_result,	
    source_data.wnv_test_result,	
    source_data.qty_original_lines,	
    source_data.created_at,	
    source_data.updated_at,	
    source_data.age_group,	
    source_data.epiweek,	
    source_data.month
FROM source_data
LEFT JOIN {{ ref('fix_location') }} AS fl ON (
    source_data.location LIKE fl."source_location"
    AND source_data.state LIKE fl."source_state"
)