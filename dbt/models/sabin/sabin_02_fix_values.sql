{{
    config(
        materialized='table'
    )
}}

WITH source_data AS (
    SELECT
        *
    FROM {{ ref("sabin_01_convert_types") }}
)
SELECT
    -- CREATE UNIQUE hash using test_id, detalhe_exame, and exame
    -- to avoid duplicates
    -- Testes PCR para Zika, Chikungunya e Dengue recebem o mesmo sample_id
    md5(
        CONCAT(
            test_id,
            exame,

            CASE
                WHEN exame ILIKE 'PCR para Zika, Chikungunya e Dengue' THEN 'MULTIPLEX'
                ELSE detalhe_exame
            END
        )
    ) AS sample_id,
    detalhe_exame,
    test_id,

    -- test_kit
    CASE
        WHEN detalhe_exame IN (
            'PCRCHIK', 'PCRDE', 'VIRUSZICA'

        ) THEN 'arbo_pcr_3'
        WHEN detalhe_exame IN (
            -- Dengue IgG
            'DENGIGG', 'DENGUEGI',
            -- Zika IgG
            'ZIKAGINDICE',
            -- Chikungunya IgG
            'RCHIKUNGMELISAIGG'

        ) THEN 'igg_serum'
        WHEN detalhe_exame IN (
            -- Dengue IgM
            'DENGUEMELISA',
            'DENGUEMIC',
            'DENGIGM',
            -- Zika IgM
            'ZIKAM1',
            -- Chikungunya IgM
            'RCHIKUNGMELISAIGM'
        ) THEN 'igm_serum'
        
        WHEN detalhe_exame IN (
            'DNS1'
        ) THEN 'ns1_antigen'

        -- PCR
        WHEN detalhe_exame IN (
            'PCRDE'
        ) THEN 'denv_pcr'
        WHEN detalhe_exame IN (
            'CHIKVPCR-BIOMOL'
        ) THEN 'chikv_pcr'
        WHEN detalhe_exame IN (
            'ZIKAPCRBIO'
        ) THEN 'zika_pcr'

        ELSE 'UNKNOWN'
    END AS test_kit,

    CASE 
        WHEN sex ILIKE 'F%' THEN 'F'
        WHEN sex ILIKE 'M%' THEN 'M'
        ELSE 'UNKNOWN'
    END AS sex,

    EXTRACT( YEAR FROM AGE(date_testing, birth_date) )::int AS age,
    
    regexp_replace(upper(unaccent(location)), '[^\w\s]', '', 'g') AS location,
    regexp_replace(upper(unaccent(state)), '[^\w\s]', '', 'g') AS state,

    CASE
        WHEN result = 'Detectado (Presença do material genético do Vírus Chikungunya)' THEN 1
        WHEN result = 'Detectado (Presença do material genético do Vírus Dengue)' THEN 1
        WHEN result = 'Não detectado (Ausência do material genético do Vírus Chikungunya)' THEN 0
        WHEN result = 'Não detectado (Ausência do material genético do Vírus Dengue)' THEN 0
        WHEN result = 'Não detectado (Ausência do material genético do Vírus Zika)' THEN 0
        WHEN result = 'NÃO REAGENTE' THEN 0
        WHEN result = 'Negativo' THEN 0
        WHEN result = 'Positivo' THEN 1
        WHEN result = 'REAGENTE' THEN 1
        WHEN result = 'REAGENTE ' THEN 1
        WHEN result = 'REAGENTE 1:200' THEN 1
        ELSE 
            CASE 
            WHEN regexp_replace(result , ',' , '.')::FLOAT < 0.80 THEN 0
            ELSE 1
        END
    END::FLOAT AS result,

    date_testing,
    file_name

FROM source_data
WHERE not detalhe_exame in ('OBSGERALINTERNA', 'FEBREGLC', 'FEBREMLC')
AND result IS NOT NULL