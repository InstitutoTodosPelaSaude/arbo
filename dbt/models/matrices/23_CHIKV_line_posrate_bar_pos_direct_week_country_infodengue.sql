{{ config(materialized='table') }}

{% set epiweek_start = '2022-01-01' %}
{% set states = dbt_utils.get_column_values(
    table=ref('matrix_01_infodengue'),
    column='state',
    where="epiweek_enddate >= '" ~ epiweek_start ~ "'"
) 
   | reject('equalto', None) 
   | list
   | sort
%}

WITH 
source_data AS (
    SELECT
        epiweek_enddate,
        region,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE
        "CHIKV_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('arbo_pcr_3', 'chikv_pcr', 'igm_serum')
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, region, pathogen
    ORDER BY epiweek_enddate, region, pathogen
),

infodengue_data AS (
    SELECT
        epiweek_enddate,
        region,
        state,
        sum(casos_estimados) as "Pos"
    FROM {{ ref("matrix_01_infodengue") }}
    WHERE
        disease = 'chikungunya' AND
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, region, state
),

source_total AS (
    SELECT
	    epiweek_enddate,
        pathogen,
        {{ matrices_metrics('result') }}
    FROM {{ ref("matrix_01_pivoted") }}
    WHERE
        "CHIKV_test_result" IN ('Pos', 'Neg') AND
        test_kit IN ('arbo_pcr_3', 'chikv_pcr', 'igm_serum')
        epiweek_enddate >= '{{ epiweek_start }}'
    GROUP BY epiweek_enddate, pathogen
),

source_total_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        MAX(CASE WHEN sc.pathogen = 'CHIKV' THEN sc."posrate" * 100 ELSE NULL END) as "Positividade Total"
    FROM source_total sc
    GROUP BY sc.epiweek_enddate
),

source_posrate AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        sc.region,
        MAX(CASE WHEN sc.pathogen = 'CHIKV' THEN sc."posrate" * 100 ELSE NULL END) as "Positividade (%, Lab. parceiros)"
    FROM source_data sc
    GROUP BY sc.epiweek_enddate, sc.region
),

infodengue_pos AS (
    SELECT
        sc.epiweek_enddate as "Semanas epidemiológicas",
        sc.region,
        sc.state,
        SUM(sc."Pos")::int AS "infodengue_pos"
    FROM infodengue_data sc
    GROUP BY sc.epiweek_enddate, sc.region, sc.state
)

SELECT 
    COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas", stp."Semanas epidemiológicas") AS "Semanas epidemiológicas",

    MAX(stp."Positividade Total") as "Brasil (Positividade)",

    MAX(CASE WHEN sp.region = 'Centro-Oeste' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Centro-Oeste (Positividade)",
    MAX(CASE WHEN sp.region = 'Nordeste' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Nordeste (Positividade)",
    MAX(CASE WHEN sp.region = 'Norte' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Norte (Positividade)",
    MAX(CASE WHEN sp.region = 'Sudeste' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Sudeste (Positividade)",
    MAX(CASE WHEN sp.region = 'Sul' THEN "Positividade (%, Lab. parceiros)" ELSE NULL END) as "Sul (Positividade)",

    SUM(CASE WHEN svp.region = 'Centro-Oeste' THEN "infodengue_pos" ELSE 0 END) as "Centro-Oeste (InfoDengue)",
    SUM(CASE WHEN svp.region = 'Nordeste' THEN "infodengue_pos" ELSE 0 END) as "Nordeste (InfoDengue)",
    SUM(CASE WHEN svp.region = 'Norte' THEN "infodengue_pos" ELSE 0 END) as "Norte (InfoDengue)",
    SUM(CASE WHEN svp.region = 'Sudeste' THEN "infodengue_pos" ELSE 0 END) as "Sudeste (InfoDengue)",
    SUM(CASE WHEN svp.region = 'Sul' THEN "infodengue_pos" ELSE 0 END) as "Sul (InfoDengue)",
    {% for st in states %}
      SUM(CASE WHEN svp.state = '{{ st | replace("'", "''") }}' 
               THEN svp."infodengue_pos" ELSE 0 END) 
      AS "{{ st }} (InfoDengue)"{{ "," if not loop.last }}
    {% endfor %}
FROM source_posrate sp
FULL OUTER JOIN infodengue_pos svp ON sp."Semanas epidemiológicas" = svp."Semanas epidemiológicas" and sp.region = svp.region
LEFT JOIN source_total_posrate stp ON sp."Semanas epidemiológicas" = stp."Semanas epidemiológicas"
GROUP BY COALESCE(sp."Semanas epidemiológicas", svp."Semanas epidemiológicas", stp."Semanas epidemiológicas")
ORDER BY "Semanas epidemiológicas"