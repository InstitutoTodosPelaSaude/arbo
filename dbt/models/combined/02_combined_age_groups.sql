

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT * 
    FROM {{ ref("01_combined_join_labs") }}

)
SELECT 
    source_data.*,
    ag.age_group
FROM source_data
LEFT JOIN {{ ref('age_groups') }} AS ag ON source_data.age >= ag." min_age" AND source_data.age <=  ag." max_age"

