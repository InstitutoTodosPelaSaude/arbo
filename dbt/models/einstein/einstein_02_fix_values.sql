
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
    {{ ref("einstein_01_convert_types") }}

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
                WHEN exame ILIKE 'PCR MULTIPLEX ZIKA, DENGUE E CHIKUNGUNYA' THEN 'MULTIPLEX'
                ELSE detalhe_exame
            END
        )
    ) AS sample_id,
    UPPER(detalhe_exame) AS detalhe_exame,
    test_id,

    -- test_kit
    CASE
        WHEN UPPER(detalhe_exame) IN (
            'VÍRUS DENGUE:',
            'VÍRUS CHIKUNGUNYA:',
            'VÍRUS ZIKA:'
        ) THEN 'arbo_pcr_3'
        WHEN UPPER(detalhe_exame) IN (
            'DENGUE IGG',
            'DENGUE IGG, TESTE RÁPIDO'
        ) THEN 'igg_serum'
        WHEN UPPER(detalhe_exame) IN (
            'DENGUE IGM',
            'DENGUE IGM, TESTE RÁPIDO'
        ) THEN 'igm_serum'
        WHEN UPPER(detalhe_exame) IN (
            'DENGUE NS1, TESTE RÁPIDO',
            'ANTÍGENO NS1 DO VIRUS DA DENGUE'
        ) THEN 'ns1_antigen'
        ELSE 'UNKNOWN'
    END AS test_kit,
    
    CASE 
        WHEN sex ILIKE 'F%' THEN 'F'
        WHEN sex ILIKE 'M%' THEN 'M'
        ELSE 'UNKNOWN'
    END AS sex,
    age,
    
    regexp_replace(upper(unaccent(location)), '[^\w\s]', '', 'g') AS location,
    regexp_replace(upper(unaccent(state)), '[^\w\s]', '', 'g') AS state,
    
    CASE 
        WHEN result = 'DETECTADO' THEN 1
        WHEN result = 'NÃO DETECTADO' THEN 0
        ELSE NULL
    END AS result,

    date_testing,
    file_name

FROM source_data
WHERE
    test_id NOT IN ('2024234005520')