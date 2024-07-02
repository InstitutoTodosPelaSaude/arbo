{{ config(materialized='table') }}

WITH source_data AS (
    SELECT
        epiweek_enddate,
        location_ibge_code,
        location,
        state,
        lat,
        long,
        SUM(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) AS "DENV"
    FROM {{ ref("matrix_02_epiweek_location") }}
    WHERE epiweek_enddate < CURRENT_DATE AND epiweek_enddate >= '2023-10-29'
    AND location != 'NOT REPORTED' AND state != 'NOT REPORTED'
    GROUP BY epiweek_enddate, location_ibge_code, location, state, lat, long
),

cumulative_cases AS (
    SELECT
        epiweek_enddate,
        location_ibge_code,
        location,
        state,
        lat,
        long,
        SUM("DENV") OVER (PARTITION BY location ORDER BY epiweek_enddate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_denv
    FROM source_data
)

SELECT
    epiweek_enddate as "end_date",
    location_ibge_code,
    location,
    state,
    lat,
    long,
    cumulative_denv as cases
FROM cumulative_cases
ORDER BY epiweek_enddate, location
    