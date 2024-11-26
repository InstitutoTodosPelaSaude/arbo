{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        age_group,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE 
        "CHIKV_test_result" IN ('Pos', 'Neg') AND 
        test_kit IN ('arbo_pcr_3', 'igm_serum', 'chikv_pcr') AND 
        age_group != 'NOT REPORTED'
    GROUP BY epiweek_enddate, age_group, pathogen
    ORDER BY epiweek_enddate, age_group, pathogen
),

chikv_data AS (
    SELECT
        age_group as "faixas etárias",
        epiweek_enddate as "semana epidemiológica",
        MAX(CASE WHEN pathogen = 'CHIKV' THEN "posrate" * 100 ELSE NULL END) as "percentual",
        SUM(CASE WHEN pathogen = 'CHIKV' THEN "Pos" ELSE 0 END)::int as "pos",
        SUM(CASE WHEN pathogen = 'CHIKV' THEN "Neg" ELSE 0 END)::int as "neg"
    FROM source_data
    GROUP BY epiweek_enddate, age_group
)

SELECT
    "semana epidemiológica" as "Semana epidemiológica",
    "faixas etárias" as "Faixa etária",
    "percentual" as "Positividade (%)",
    "pos" as "Positivos",
    "neg" as "Negativos",
    ("pos" + "neg")::int as "Total de casos"
FROM chikv_data
ORDER BY "semana epidemiológica", "faixas etárias"
    