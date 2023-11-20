

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "hilab_raw") }}

)
SELECT
    * -- TODO
FROM source_data