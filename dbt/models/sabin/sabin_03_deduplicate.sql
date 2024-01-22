{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        *
    FROM {{ ref("sabin_02_fix_values") }}
)
SELECT * FROM source_data

-- REMOVER DENGUEIG SE VIER ACOMPANHADO DE DENGUE IGG
-- REMOVER SAMPLE ID DUPLICADOS