

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "hlagyn_raw") }}

)
SELECT
    * -- TODO
FROM source_data