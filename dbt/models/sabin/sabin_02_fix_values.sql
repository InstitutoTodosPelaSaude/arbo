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

        ) AND exame ILIKE 'PCR para Zika, Chikungunya e Dengue'
        THEN 'arbo_pcr_3'
        WHEN detalhe_exame IN (
            -- Dengue IgG
            'DENGIGG', 'DENGUEGI',
            -- Zika IgG
            'ZIKAGINDICE',
            'ZIKAIGG2',
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
            'ZIKAM2',
            -- Chikungunya IgM
            'RCHIKUNGMELISAIGM'
        ) THEN 'igm_serum'
        
        WHEN detalhe_exame IN (
            'DNS1',
            'NS1ELISA',
            'NS1IMUNOCRO'
        ) THEN 'ns1_antigen'

        -- PCR
        WHEN detalhe_exame IN (
            'PCRDE'
        ) AND exame ILIKE 'DETECÇÃO MOLECULAR DO VÍRUS DENGUE '
        THEN 'denv_pcr'

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
        WHEN result = 'NÃO DETECTADO RNA DO VÍRUS DA DENGUE' THEN 0
        WHEN result = 'Não detectado (Ausência de material genético do vírus Chikungunya)' THEN 0
        WHEN result = 'Não detectado (Ausência do material genético do Vírus Chikungunya)' THEN 0
        WHEN result = 'Não detectado (Ausência do material genético do Vírus Dengue)' THEN 0
        WHEN result = 'Não detectado (Ausência do material genético do Vírus Zika)' THEN 0
        WHEN result = 'NÃO DETECTADO' THEN 0
        WHEN result = 'DETECTÁVEL' THEN 1
        WHEN result = 'NÃO REAGENTE' THEN 0
        WHEN result = 'Negativo' THEN 0
        WHEN result = 'Positivo' THEN 1
        WHEN result = 'REAGENTE' THEN 1
        WHEN result = 'REAGENTE ' THEN 1
        WHEN result = 'REAGENTE 1:200' THEN 1
        WHEN result = 'Reagente' THEN 1
        -- 9999 or 99,99 or 99.99
        WHEN result ~ '[0-9]+[,.]*[0-9]*' AND result ~ '^[0-9]' THEN
            CASE 
                WHEN regexp_replace(result , ',' , '.')::FLOAT < 0.80 THEN 0
                ELSE 1
            END
        ELSE -2 -- UNKNOWN
    END::FLOAT AS result,

    date_testing,
    file_name

FROM source_data
WHERE not detalhe_exame in ('OBSGERALINTERNA', 'FEBREGLC', 'FEBREMLC')
AND result IS NOT NULL
AND NOT result IN ('INDETERMINADO', '*')
-- WIP: remove this filter
AND NOT detalhe_exame IN (
        'ADOLFOLUTZPDF', 
        'DEVORO',
        'RCHIKUNGMIMUNOG', 
        'RCHIKUGMIMUNOM'
    )