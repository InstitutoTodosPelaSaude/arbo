{{ config(materialized='table') }}
WITH source_data AS (
    SELECT * FROM
    {{ source("dagster", "infodengue_raw") }}
)
SELECT 1 AS example