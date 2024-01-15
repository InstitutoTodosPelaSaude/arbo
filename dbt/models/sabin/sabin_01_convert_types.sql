{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
    *
    FROM {{ source('dagster', 'sabin_raw') }}
)
SELECT
    "Código Posto" AS codigo_posto,
    "OS" AS test_id,
    "Estado" AS state,
    "Municipio" AS location,
    "DataAtendimento" AS date_testing, -- WIP
    "DataNascimento",  -- WIP
    "Sexo" AS sex,
    "Descricao" AS exame,
    "Parametro" AS detalhe_exame,
    "Resultado" AS result,
    -- "DataAssinatura",  -- WIP
    "file_name"
FROM source_data
