
{{ config(materialized='table') }}

-- PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG	    VÍRUS DENGUE:
-- PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG	    VÍRUS CHIKUNGUNYA:
-- PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG	    VÍRUS ZIKA:
-- PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG --> arbo_pcr_3
-- SOROLOGIA PARA DENGUE	                DENGUE IGG --> igg_serum
-- SOROLOGIA PARA DENGUE	                DENGUE IGM --> igm_serum
-- TESTE RÁPIDO PARA DENGUE IGG	            DENGUE IGG, TESTE RÁPIDO --> igg_serum
-- TESTE RÁPIDO PARA DENGUE IGM E NS1	    DENGUE IGM, TESTE RÁPIDO --> igm_serum
-- TESTE RÁPIDO PARA DENGUE IGM E NS1	    DENGUE NS1, TESTE RÁPIDO --> ns1_antigen

WITH source_data AS (

    SELECT * FROM
    {{ ref("01_einstein_convert_types") }}

)
SELECT 
    
    -- CREATE UNIQUE hash using test_id, detalhe_exame, and exame
    -- to avoid duplicates
    -- Testes PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG recebem o mesmo id
    md5(
        CONCAT(
            test_id,
            exame,

            CASE
                WHEN exame ILIKE 'PCR MULTIPLEX ZIKA, DENGUE E CHIKUNG' THEN 'MULTIPLEX'
                ELSE detalhe_exame
            END
        )
    ) AS sample_id,
    detalhe_exame,
    test_id,

    -- test_kit
    CASE
        WHEN detalhe_exame IN (
            'VÍRUS DENGUE:',
            'VÍRUS CHIKUNGUNYA:',
            'VÍRUS ZIKA:'
        ) THEN 'arbo_pcr_3'
        WHEN detalhe_exame IN (
            'DENGUE IGG',
            'DENGUE IGG, TESTE RÁPIDO'
        ) THEN 'igg_serum'
        WHEN detalhe_exame IN (
            'DENGUE IGM',
            'DENGUE IGM, TESTE RÁPIDO'
        ) THEN 'igm_serum'
        WHEN detalhe_exame IN (
            'DENGUE NS1, TESTE RÁPIDO'
        ) THEN 'ns1_antigen'
        ELSE 'UNKNOWN'
    END AS test_kit,
    
    CASE 
        WHEN gender ILIKE 'F%' THEN 'F'
        WHEN gender ILIKE 'M%' THEN 'M'
        ELSE 'UNKNOWN'
    END AS gender,
    age,
    
    regexp_replace(lower(unaccent(location)), '[^\w]+','') AS location,
    state AS state,
    
    CASE 
        WHEN result = 'DETECTADO' THEN 1
        WHEN result = 'NÃO DETECTADO' THEN 0
        ELSE NULL
    END AS result,

    date_testing,
    file_name

FROM source_data