{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        *
    FROM {{ ref("sabin_02_fix_values") }}
), source_data_no_ig_duplicates AS (
    SELECT 
        sample_id,
        detalhe_exame,
        test_id,
        test_kit,
        sex,
        age,
        location,
        state,
        result,
        date_testing,
        file_name
    FROM
    (
        SELECT
            *,
            CASE
                -- REMOVER DENGUEIG SE VIER ACOMPANHADO DE DENGUE IGG
                WHEN 
                    SUM( (detalhe_exame in ('DENGIGG', 'DENGUEGI'))::INT) 
                    OVER (PARTITION BY test_id) = 2 AND detalhe_exame = 'DENGUEGI' 
                THEN 1

                -- REMOVER NS1IMUNOCRO SE VIER ACOMPANHADO DE NS1ELISA
                WHEN
                    SUM( (detalhe_exame in ('NS1ELISA', 'NS1IMUNOCRO'))::INT)
                    OVER (PARTITION BY test_id) = 2 AND detalhe_exame = 'NS1IMUNOCRO'
                THEN 1

                ELSE 0
            END::BOOL AS remove_line
        FROM source_data
    ) AS _
    WHERE NOT remove_line
)
SELECT 
    *
FROM 
    source_data_no_ig_duplicates