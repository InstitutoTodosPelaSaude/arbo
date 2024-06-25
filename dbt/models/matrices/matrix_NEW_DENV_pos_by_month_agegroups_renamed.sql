{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        "month",
        age_group,
        SUM(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) AS "DENV"
    FROM {{ ref("matrix_02_month_agegroups") }}
    GROUP BY "month", age_group
)
SELECT
    "month",
    age_group AS "faixas_etárias",
    "DENV" AS "Dengue"
FROM source_data
ORDER BY "month", age_group
    