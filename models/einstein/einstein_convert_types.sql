

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "einstein_raw") }}

)
SELECT
    "ACCESSION",
    "SEXO",
    "IDADE"::INT AS IDADE,
    "EXAME",
    "DETALHE_EXAME",
    "DH_COLETA",
    "MUNICÍPIO",
    "ESTADO",
    "PATOGENO",
    "RESULTADO",
    file_name
FROM source_data