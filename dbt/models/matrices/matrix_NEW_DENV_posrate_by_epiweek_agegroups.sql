{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" ELSE NULL END) AS "DENV"
    FROM {{ ref("matrix_02_epiweek_agegroups") }}
    GROUP BY epiweek_enddate, age_group
)
SELECT
    age_group as "faixas etárias",
    epiweek_enddate as "semana epidemiológica",
    "DENV" as "percentual"
FROM source_data
ORDER BY epiweek_enddate, age_group
    