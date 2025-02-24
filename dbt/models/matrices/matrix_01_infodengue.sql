{{ config(materialized='view') }}

WITH 
epiweeks AS (
    SELECT *
    FROM {{ ref("epiweeks") }}
),

source_data_raw AS(
    SELECT
    *
    FROM {{ ref("infodengue_final") }}
),

source_data AS(
    SELECT 
        source_data_raw.*,
        ew.end_date as epiweek_enddate,
        ew.week_num as epiweek_number,

        TO_CHAR(source_data_raw."data_fimSE", 'YYYY-MM') as month
        
    FROM source_data_raw
    LEFT JOIN epiweeks AS ew ON source_data_raw."data_fimSE" >= ew.start_date AND source_data_raw."data_fimSE" <= ew.end_date
)
 
SELECT
    disease,
    epiweek_enddate,
    epiweek_number,
    month,
    "data_fimSE",
    casos_estimados,
    state_code,
    state,
    region
FROM source_data
WHERE 
    epiweek_enddate < CURRENT_DATE AND
    epiweek_enddate >= '2022-01-01'
