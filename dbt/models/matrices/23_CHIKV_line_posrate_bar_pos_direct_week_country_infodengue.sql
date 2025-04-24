{{ config(materialized='table') }}

WITH 
source_data AS (
    SELECT
        epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE
        "CHIKV_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('arbo_pcr_3', 'chikv_pcr', 'igm_serum')
    GROUP BY epiweek_enddate, pathogen
    ORDER BY epiweek_enddate, pathogen
),

source_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        MAX(CASE WHEN sc.pathogen = 'CHIKV' THEN sc."posrate" * 100 ELSE NULL END) as "Positividade (Lab. parceiros)"
    FROM source_data sc
    GROUP BY sc.epiweek_enddate
    ORDER BY sc.epiweek_enddate
),

infodengue_pos AS (
    SELECT
        epiweek_enddate as "Semanas epidemiológicas",
        sum(casos_estimados) as "Casos estimados de chikungunya (InfoDengue)"
    FROM {{ ref("matrix_01_infodengue") }}
    WHERE
        disease = 'chikungunya'
    GROUP BY epiweek_enddate
    ORDER BY epiweek_enddate
)

SELECT 
    COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas") AS "Semanas epidemiológicas",
    sp."Positividade (Lab. parceiros)",
    svp."Casos estimados de chikungunya (InfoDengue)"
FROM source_posrate sp
FULL OUTER JOIN infodengue_pos svp
ON sp."Semanas epidemiológicas" = svp."Semanas epidemiológicas"
ORDER BY "Semanas epidemiológicas"

