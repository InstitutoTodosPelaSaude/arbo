

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT * FROM {{ ref("06_einstein_final") }}

)
SELECT
    *
FROM source_data