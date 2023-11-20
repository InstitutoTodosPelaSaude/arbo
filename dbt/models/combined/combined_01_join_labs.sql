

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT 
    'EINSTEIN' as lab_id,
    * 
    FROM {{ ref("einstein_06_final") }}

)
SELECT
    *
FROM source_data