

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * 
    FROM {{ ref("combined_02_age_groups") }}

)
SELECT 
    source_data.*,
    ew.week_num as epiweek,
    EXTRACT(MONTH FROM source_data.date_testing) as month
FROM source_data
LEFT JOIN {{ ref('epiweeks') }} AS ew ON source_data.date_testing >= ew.start_date AND source_data.date_testing <= ew.end_date

