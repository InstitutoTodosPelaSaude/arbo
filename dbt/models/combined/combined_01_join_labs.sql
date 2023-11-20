

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT 
    'EINSTEIN' as lab_id,
    * 
    FROM {{ ref("06_einstein_final") }}

)
SELECT
    *
FROM source_data