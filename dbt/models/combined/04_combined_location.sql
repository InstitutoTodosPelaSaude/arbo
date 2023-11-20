

{{ config(materialized='view') }}

WITH source_data AS (

    SELECT * 
    FROM {{ ref("03_combined_dates") }}

)
SELECT 
    source_data.*,
    'BRASIL' as country,
    '' as region,
    '' as macroregion,
    '' as macroregion_code,
    '' as state_code,
    '' as state_ibge_code,
    '' as location_ibge_code,
    '' as lat,
    '' as long
FROM source_data
-- LEFT JOIN {{ ref('municipios') }} AS lc ON source_data.location = lc.NM_MUN_NORM