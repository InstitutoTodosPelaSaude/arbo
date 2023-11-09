{{ config(materialized='view') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref("einstein_fix_values") }}

)
SELECT * FROM source_data