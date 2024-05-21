{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        region,
        SUM(CASE WHEN pathogen = 'DENV' THEN "totaltests" ELSE 0 END) AS "DENV"
    FROM {{ ref("matrix_02_epiweek_regions") }}
    GROUP BY epiweek_enddate, region
)
SELECT
    epiweek_enddate as "Semanas epidemiológicas",
    MAX(CASE WHEN region = 'Centro-Oeste' THEN "DENV" ELSE 0 END) as "Centro-Oeste",
    MAX(CASE WHEN region = 'Nordeste' THEN "DENV" ELSE 0 END) as "Nordeste",
    MAX(CASE WHEN region = 'Norte' THEN "DENV" ELSE 0 END) as "Norte",
    MAX(CASE WHEN region = 'Sudeste' THEN "DENV" ELSE 0 END) as "Sudeste",
    MAX(CASE WHEN region = 'Sul' THEN "DENV" ELSE 0 END) as "Sul"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    