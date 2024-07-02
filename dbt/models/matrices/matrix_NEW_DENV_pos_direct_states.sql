{{ config(materialized='table') }}


WITH source_data AS (
    SELECT
        epiweek_enddate,
        state,
        SUM(CASE WHEN pathogen = 'DENV' THEN "Pos" ELSE 0 END) AS "DENV"
    FROM {{ ref("matrix_02_epiweek_state_name") }}
    WHERE epiweek_enddate < CURRENT_DATE AND epiweek_enddate >= '2023-10-29'
    AND state != 'NOT REPORTED'
    GROUP BY epiweek_enddate, state
),

cumulative_cases AS (
    SELECT
        epiweek_enddate,
        state,
        SUM("DENV") OVER (PARTITION BY state ORDER BY epiweek_enddate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_denv
    FROM source_data
)

SELECT
    epiweek_enddate as "end_date",
    state,
    cumulative_denv as cases
FROM cumulative_cases
ORDER BY epiweek_enddate, state
    