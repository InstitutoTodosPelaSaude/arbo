{{ config(materialized='table') }}

{% set epiweek_start = '2022-01-08' %}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE
        epiweek_enddate >= '{{ epiweek_start }}' AND
        CASE
            WHEN "DENV_test_result"  IN ('Neg', 'Pos') THEN test_kit IN ('arbo_pcr_3', 'denv_pcr', 'ns1_antigen')
            WHEN "CHIKV_test_result"   IN ('Neg', 'Pos') THEN test_kit IN ('arbo_pcr_3', 'chikv_pcr', 'igm_serum')
            ELSE FALSE
        END
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
)

SELECT
    epiweek_enddate as "semana_correta",
    TO_CHAR(MAKE_DATE(2022, EXTRACT(MONTH FROM epiweek_enddate)::int, EXTRACT(DAY FROM epiweek_enddate)::int), 'YYYY-MM-DD') as semana_ajustada,
    TO_CHAR(epiweek_enddate, 'DD') || '/' ||
        CASE TO_CHAR(epiweek_enddate, 'MM')
            WHEN '01' THEN 'Jan'
            WHEN '02' THEN 'Fev'
            WHEN '03' THEN 'Mar'
            WHEN '04' THEN 'Abr'
            WHEN '05' THEN 'Mai'
            WHEN '06' THEN 'Jun'
            WHEN '07' THEN 'Jul'
            WHEN '08' THEN 'Ago'
            WHEN '09' THEN 'Set'
            WHEN '10' THEN 'Out'
            WHEN '11' THEN 'Nov'
            WHEN '12' THEN 'Dez'
        END AS "Semana",
        
    MAX(CASE WHEN pathogen = 'DENV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2022' THEN "posrate" * 100 ELSE NULL END) as "Dengue - 2022",
    MAX(CASE WHEN pathogen = 'DENV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2023' THEN "posrate" * 100 ELSE NULL END) as "Dengue - 2023",
    MAX(CASE WHEN pathogen = 'DENV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2024' THEN "posrate" * 100 ELSE NULL END) as "Dengue - 2024",
    MAX(CASE WHEN pathogen = 'DENV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2025' THEN "posrate" * 100 ELSE NULL END) as "Dengue - 2025",

    MAX(CASE WHEN pathogen = 'CHIKV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2022' THEN "posrate" * 100 ELSE NULL END) as "Chikungunya - 2022",
    MAX(CASE WHEN pathogen = 'CHIKV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2023' THEN "posrate" * 100 ELSE NULL END) as "Chikungunya - 2023",
    MAX(CASE WHEN pathogen = 'CHIKV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2024' THEN "posrate" * 100 ELSE NULL END) as "Chikungunya - 2024",
    MAX(CASE WHEN pathogen = 'CHIKV' AND TO_CHAR(epiweek_enddate, 'YYYY') = '2025' THEN "posrate" * 100 ELSE NULL END) as "Chikungunya - 2025"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY semana_ajustada