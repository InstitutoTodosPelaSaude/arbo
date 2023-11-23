

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "hlagyn_raw") }}

)
SELECT
    "Idade"::INT AS age,
    "Sexo" AS gender,
    "Pedido" AS test_id,
    "Data Coleta"::DATE AS date_testing,
    "Zika vírus" AS zika_result,
    "Dengue vírus" AS dengue_result,
    "Chikungunya vírus" AS chikungunya_result,
    "Tipo Material" AS tipo_material,
    "Metodologia" AS metodologia, 
    "Cidade" AS location,
    "UF" AS state_code,
    file_name,
    "Método" AS metodo,
    "Id cliente" AS id_cliente
FROM source_data