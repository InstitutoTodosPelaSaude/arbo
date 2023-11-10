

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "einstein_raw") }}

)
SELECT
    "ACCESSION" AS test_id,
    "SEXO" AS gender,
    "IDADE"::INT AS age,
    "EXAME" AS exame,
    "DETALHE_EXAME" AS detalhe_exame,
    "DH_COLETA"::DATE AS date_testing,
    "MUNICÍPIO" AS location,
    "ESTADO" AS state,
    "PATOGENO" AS pathogen,
    "RESULTADO" AS result,
    file_name
FROM source_data