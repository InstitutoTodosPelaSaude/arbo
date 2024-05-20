{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        "month",
        age_group,
        SUM(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) AS "DENV",
        SUM(CASE WHEN pathogen = 'ZIKV' THEN "Pos" ELSE 0 END) AS "ZIKV",
        SUM(CASE WHEN pathogen = 'CHIKV' THEN "Pos" ELSE 0 END) AS "CHIKV",
        SUM(CASE WHEN pathogen = 'YFV' THEN "Pos" ELSE 0 END) AS "YFV",
        SUM(CASE WHEN pathogen = 'MAYV' THEN "Pos" ELSE 0 END) AS "MAYV",
        SUM(CASE WHEN pathogen = 'OROV' THEN "Pos" ELSE 0 END) AS "OROV",
        SUM(CASE WHEN pathogen = 'WNV' THEN "Pos" ELSE 0 END) AS "WNV"
    FROM {{ ref("matrix_02_month_agegroups") }}
    GROUP BY "month", age_group
)
SELECT
    "month",
    age_group AS "faixas_etárias",
    "CHIKV",
    "DENV",
    "OROV",
    "ZIKV"
FROM source_data
ORDER BY "month", age_group
    