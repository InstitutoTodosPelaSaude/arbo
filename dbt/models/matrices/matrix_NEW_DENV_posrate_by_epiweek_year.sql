{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_month,
        epiweek_year,
        MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" * 100 ELSE NULL END) AS "DENV"
    FROM {{ ref("matrix_02_epiweek_year") }}
    GROUP BY epiweek_month, epiweek_year
)
SELECT
    epiweek_month as "year",
    MAX(CASE WHEN epiweek_year = 2022 THEN "DENV" ELSE NULL END) as "2022",
    MAX(CASE WHEN epiweek_year = 2023 THEN "DENV" ELSE NULL END) as "2023",
    MAX(CASE WHEN epiweek_year = 2024 THEN "DENV" ELSE NULL END) as "2024"
FROM source_data
GROUP BY epiweek_month
ORDER BY epiweek_month
    