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
    epiweek_enddate as "Semana",
    MAX(CASE WHEN pathogen = 'DENV' THEN "posrate" * 100 ELSE NULL END) as "Dengue",
    MAX(CASE WHEN pathogen = 'CHIKV' THEN "posrate" * 100 ELSE NULL END) as "Chikungunya"
FROM source_data
GROUP BY epiweek_enddate
ORDER BY epiweek_enddate