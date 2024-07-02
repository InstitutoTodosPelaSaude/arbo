{{ config(materialized='table') }}


WITH source_data AS (
    SELECT
        epiweek_enddate,
        state,
        SUM(CASE WHEN pathogen = 'CHIKV' THEN "Pos" ELSE 0 END) AS "CHIKV"
    FROM {{ ref("matrix_02_epiweek_state_name") }}
    WHERE epiweek_enddate < CURRENT_DATE AND epiweek_enddate >= '2023-10-29'
    AND state != 'NOT REPORTED'
    GROUP BY epiweek_enddate, state
),

cumulative_cases AS (
    SELECT
        epiweek_enddate,
        state,
        SUM("CHIKV") OVER (PARTITION BY state ORDER BY epiweek_enddate ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_chikv
    FROM source_data
)

SELECT
    epiweek_enddate as "end_date",
    state,
    cumulative_chikv as cases
FROM cumulative_cases
ORDER BY epiweek_enddate, state
    