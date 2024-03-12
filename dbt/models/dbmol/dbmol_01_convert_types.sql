WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "dbmol_raw") }}

)
SELECT
    "NumeroPedido" AS test_id,
    "Sexo" AS sex,
    -- "idade"::INT AS age,
    {{ normalize_text("Procedimento") }} AS exame,
    {{ normalize_text("CodigoProcedimento") }} AS "CodigoProcedimento",
    {{ normalize_text("Parametro") }} AS detalhe_exame,
    TO_DATE("DataCadastro", 'YYYY-MM-DD') AS date_testing,
    TO_DATE("DataNascimento", 'YYYY-MM-DD') AS birth_date,
    "Cidade" AS location,
    "UF" AS state,
    {{ normalize_text("Resultado") }} AS result,
    file_name
FROM source_data