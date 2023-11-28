

{{ config(materialized='table') }}

WITH 
source_data AS (

    SELECT * 
    FROM {{ ref("combined_04_fix_location") }}

),

municipios AS (

    SELECT *
    FROM {{ ref("municipios") }}

),

macroregions AS (

    SELECT *
    FROM {{ ref("macroregions") }}
)


SELECT 
    source_data.*,
    'BRASIL' as country,
    municipios."REGIAO" as region,
    macroregions."DS_NOMEPAD_macsaud" as macroregion,
    macroregions."CO_MACSAUD" as macroregion_code,
    municipios."SIGLA_UF" as state_code,
    municipios."CD_UF" as state_ibge_code,
    municipios."CD_MUN" as location_ibge_code,
    municipios.lat as lat,
    municipios.long as long
FROM source_data
LEFT JOIN municipios ON (
    source_data.location LIKE municipios."NM_MUN_NORM"
    AND source_data.state LIKE municipios."NM_UF_NORM"
)
LEFT JOIN macroregions ON (
    municipios."CD_UF" = macroregions."CO_UF"
    AND ('BR' || municipios."CD_MUN"::TEXT) = macroregions."ADM2_PCODE"
)