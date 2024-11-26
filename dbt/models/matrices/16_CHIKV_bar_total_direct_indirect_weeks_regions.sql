{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        "CHIKV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'igm_serum', 'chikv_pcr') AND 
        region != 'NOT REPORTED'
    GROUP BY epiweek_enddate, region, pathogen
    ORDER BY epiweek_enddate, region, pathogen
),

transformation AS (
    SELECT
        epiweek_enddate,
        region,
        SUM(CASE WHEN pathogen = 'CHIKV' THEN totaltests ELSE 0 END) AS "CHIKV"
    FROM source_data
    GROUP BY epiweek_enddate, region
    ORDER BY epiweek_enddate, region
)

SELECT
    epiweek_enddate as "Semanas epidemiológicas",
    MAX(CASE WHEN region = 'Centro-Oeste' THEN "CHIKV" ELSE 0 END) as "Centro-Oeste",
    MAX(CASE WHEN region = 'Nordeste' THEN "CHIKV" ELSE 0 END) as "Nordeste",
    MAX(CASE WHEN region = 'Norte' THEN "CHIKV" ELSE 0 END) as "Norte",
    MAX(CASE WHEN region = 'Sudeste' THEN "CHIKV" ELSE 0 END) as "Sudeste",
    MAX(CASE WHEN region = 'Sul' THEN "CHIKV" ELSE 0 END) as "Sul"
FROM transformation
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate
    