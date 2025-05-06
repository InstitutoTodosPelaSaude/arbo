{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        *
    FROM {{ ref("21_DENV_heat_posrate_direct_weeks_states") }}
)

SELECT
    *
FROM source_data
WHERE 
    "Completude final" IN ('Apenas histórico', 'Alta qualidade')