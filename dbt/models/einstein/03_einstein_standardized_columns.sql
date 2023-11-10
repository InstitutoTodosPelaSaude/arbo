{{ config(materialized='view') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref("02_einstein_fix_values") }}

)
SELECT * FROM source_data