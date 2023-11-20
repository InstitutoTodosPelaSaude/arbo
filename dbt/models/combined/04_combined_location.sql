

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT * 
    FROM {{ ref("03_combined_dates") }}

)
SELECT 
    source_data.*,
    'BRASIL' as country,
    lc."REGIAO" as region,
    NULL as macroregion,
    NULL as macroregion_code,
    lc."SIGLA_UF" as state_code,
    NULL as state_ibge_code,
    lc."CD_MUN" as location_ibge_code,
    lc.lat as lat,
    lc.long as long
FROM source_data
LEFT JOIN {{ ref('municipios') }} AS lc ON (
    source_data.location LIKE lc."NM_MUN_NORM"
    AND source_data.state LIKE lc."NM_UF_NORM"
)