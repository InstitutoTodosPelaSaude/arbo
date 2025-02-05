{{
    config(
        materialized='incremental',
        unique_key=['"SE"', 'state_code', 'disease'],
        incremental_strategy='merge',
        merge_exclude_columns = ['created_at']
    )
}}
WITH source_data AS(
    SELECT
    *
    FROM {{ ref("infodengue_01_convert_types") }}
)
SELECT
    *,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS created_at,
    CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo' AS updated_at
FROM source_data