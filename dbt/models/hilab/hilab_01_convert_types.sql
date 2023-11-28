

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ source("dagster", "hilab_raw") }}

)
SELECT
    "Código Da Cápsula" AS test_id,
    "Estado" AS state_code,
    "Cidade" AS location,
    TO_DATE("Data Do Exame", 'DD/MM/YYYY') AS date_testing,
    "Exame" AS exame,
    "Resultado" AS result,
    "Idade"::INT AS age,
    "Paciente" AS patient_id,
    "Sexo" AS gender,
    file_name
FROM source_data
WHERE 
    "Exame" NOT IN ('Influenza A', 'Influenza B', 'Covid-19 Antígeno') AND
    "Resultado" NOT IN ('Inválido')
