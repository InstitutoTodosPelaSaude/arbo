

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT * 
    FROM {{ ref("01_combined_join_labs") }}

)
SELECT
    *
FROM source_data

-- age_group

-- epiweek
-- month

-- country
-- region
-- macroregion
-- macroregion_code
-- state_code
-- state_ibge_code
-- location_ibge_code
-- lat
-- long