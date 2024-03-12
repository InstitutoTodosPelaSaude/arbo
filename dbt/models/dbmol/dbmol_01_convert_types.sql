WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "dbmol_raw") }}

)
SELECT
    "NumeroPedido" AS test_id,
    "Sexo" AS sex,
    -- "idade"::INT AS age,
    "DataNascimento" AS birth_date,
    "Procedimento" AS exame,
    "CodigoProcedimento",
    "Parametro" AS detalhe_exame,
    TO_DATE("DataCadastro", 'YYYY-MM-DD') AS date_testing,
    "Cidade" AS location,
    "UF" AS state,
    "Resultado" AS result,
    file_name
FROM source_data