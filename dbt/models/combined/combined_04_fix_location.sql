

{{ config(materialized='table') }}

{% set column_names = dbt_utils.get_filtered_columns_in_relation(from=ref('combined_03_dates'), except=["location", "state"]) %}

WITH source_data AS (

    SELECT
        {% for column_name in column_names %}
            "{{ column_name }}",
        {% endfor %}
        {{ normalize_text("location") }} as "location",
        {{ normalize_text("state") }} as "state"
    FROM {{ ref("combined_03_dates") }}

)
SELECT
    source_data.lab_id,	
    source_data.sample_id,	
    source_data.test_id,	
    source_data.test_kit,
    source_data.test_method,
    source_data.sex,	
    source_data.age,
    CASE
        WHEN fl.source_location IS NULL THEN source_data.location -- If the location is correct, keep the original value
        ELSE fl.target_location
    END AS location,
    CASE
        WHEN fs.source_state IS NULL THEN source_data.state -- If the state is correct, keep the original value
        ELSE fs.target_state
    END AS state,
    source_data.date_testing,
    source_data.patient_id,	
    source_data.file_name,	
    source_data.denv_test_result as "DENV_test_result",	
    source_data.zikv_test_result as "ZIKV_test_result",	
    source_data.chikv_test_result as "CHIKV_test_result",
    source_data.yfv_test_result as "YFV_test_result",
    source_data.mayv_test_result as "MAYV_test_result",
    source_data.orov_test_result as "OROV_test_result",
    source_data.wnv_test_result as "WNV_test_result",
    source_data.qty_original_lines,
    source_data.created_at,	
    source_data.updated_at,	
    source_data.age_group,	
    source_data.epiweek_enddate,
    source_data.epiweek_number,
    source_data.month
FROM source_data
LEFT JOIN {{ ref('fix_state') }} AS fs ON source_data.state ILIKE fs."source_state"
LEFT JOIN {{ ref('fix_location') }} AS fl ON (
    source_data.location ILIKE fl."source_location"
    AND (
        source_data.state ILIKE fl."source_state" -- Original state name
        OR
        fs.target_state ILIKE fl."source_state"   -- Fixed state name
    )
)