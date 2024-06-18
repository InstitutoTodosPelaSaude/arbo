{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" ELSE NULL END) AS "DENV_posrate",
        SUM(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) AS "DENV_pos",
        SUM(CASE WHEN pathogen = 'DENV' THEN "Neg" ELSE 0 END) AS "DENV_neg"
    FROM {{ ref("matrix_02_epiweek") }}
    GROUP BY epiweek_enddate
)
SELECT
    epiweek_enddate as "Semanas epidemiológicas",
    "DENV_posrate" as "Positividade (%)",
    "DENV_pos" as "Positivos",
    "DENV_neg" as "Negativos"
FROM source_data
ORDER BY epiweek_enddate
    