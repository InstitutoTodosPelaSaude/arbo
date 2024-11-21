{{ config(materialized='table') }}
WITH source_data AS (
    SELECT
        "SE"::TEXT,
        "casos_est"::INT,
        "casos_est_min"::INT,
        "casos_est_max"::INT,
        "casos"::INT,
        "state_code",
        "state",
        "region",
        to_date("data_iniSE", 'YYYY-MM-DD') AS "data_iniSE",
        to_date("data_fimSE", 'YYYY-MM-DD') AS "data_fimSE",
        "file_name"
    FROM
    {{ source("dagster", "infodengue_raw") }}
)
SELECT * FROM source_data