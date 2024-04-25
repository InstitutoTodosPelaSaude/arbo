{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "fleury_raw") }}

)
SELECT
    "CODIGO REQUISICAO" AS test_id,
    TO_DATE("DATA COLETA", 'DD/MM/YYYY') AS date_testing,
    "PACIENTE" AS patient_id,
    "SEXO" AS sex,
    "IDADE" AS age,
    "MUNICIPIO" AS location,
    "ESTADO" AS state_code,
    "EXAME" AS exame,
    {{ normalize_text("PATOGENO") }} AS pathogen,
    {{ normalize_text("RESULTADO") }} AS result,
    file_name
FROM source_data