{{ config(materialized='table') }}

{% set date_testing_start = '2022-01-01' %}

WITH source_data AS (
    SELECT
        TO_CHAR("date_testing", 'YYYY - Q"º Trimestre"') AS trimestre,
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        test_kit IN ('arbo_pcr_3', 'igm_serum', 'orov_pcr', 'mayv_pcr', 'yfv_pcr', 'zikv_pcr') AND 
        age_group != 'NOT REPORTED' AND
        date_testing >= '{{ date_testing_start }}'
    GROUP BY trimestre, age_group, pathogen
),
faixas_etarias AS (
    SELECT DISTINCT age_group FROM source_data
),
trimestres AS (
    SELECT DISTINCT trimestre FROM source_data
),
combinacoes AS (
    SELECT t.trimestre, f.age_group
    FROM trimestres t
    CROSS JOIN faixas_etarias f
),
aggregated_data AS (
    SELECT
        trimestre,
        age_group AS "faixas_etarias",
        SUM(CASE WHEN pathogen = 'OROV' THEN "Pos" ELSE 0 END)::int AS "Oropouche",
        SUM(CASE WHEN pathogen = 'ZIKV' THEN "Pos" ELSE 0 END)::int AS "Zika",
        SUM(CASE WHEN pathogen = 'YFV' THEN "Pos" ELSE 0 END)::int AS "Febre amarela",
        SUM(CASE WHEN pathogen = 'MAYV' THEN "Pos" ELSE 0 END)::int AS "Mayaro",
        SUM(CASE WHEN pathogen = 'WNV' THEN "Pos" ELSE 0 END)::int AS "West Nile"
    FROM source_data
    GROUP BY trimestre, age_group
)
SELECT
    c.trimestre,
    c.age_group AS "faixas_etarias",
    COALESCE(a."Oropouche"::int, null)::int AS "Oropouche",
    COALESCE(a."Zika"::int, null)::int AS "Zika",
    COALESCE(a."Febre amarela"::int, null)::int AS "Febre amarela",
    COALESCE(a."Mayaro"::int, null)::int AS "Mayaro",
    COALESCE(a."West Nile"::int, null)::int AS "West Nile"
FROM combinacoes c
LEFT JOIN aggregated_data a
    ON c.trimestre = a.trimestre AND c.age_group = a.faixas_etarias
ORDER BY c.trimestre ASC, c.age_group DESC