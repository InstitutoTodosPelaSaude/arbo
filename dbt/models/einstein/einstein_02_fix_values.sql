
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
CASE unaccent(UPPER(exame))
    WHEN 'ANTIGENO NS1 DO VIRUS DA DENGUE' THEN 'ns1_antigen'
    WHEN 'DENGUE' THEN 'denv_pcr'
    WHEN 'DENGUE TESTE RAPIDO IGG' THEN 'denv_igg'
    WHEN 'PCR MULTIPLEX ZIKA, DENGUE E CHIKUNGUNYA' THEN
        CASE unaccent(UPPER(detalhe_exame))
            WHEN 'VIRUS ZIKA:'        THEN 'zikv_pcr'
            WHEN 'VIRUS DENGUE:'      THEN 'denv_pcr'
            WHEN 'VIRUS CHIKUNGUNYA:' THEN 'chikv_pcr'
            ELSE 'UNKNOWN'
        END
    WHEN 'SOROLOGIA PARA DENGUE IGG E IGM' THEN
        CASE unaccent(UPPER(detalhe_exame))
            WHEN 'DENGUE IGM' THEN 'denv_igm'
            WHEN 'DENGUE IGG' THEN 'denv_igg'
            ELSE 'UNKNOWN'
        END
    WHEN 'TESTE RAPIDO PARA DENGUE IGG E NS1' THEN
        CASE unaccent(UPPER(detalhe_exame))
            WHEN 'DENGUE IGG, TESTE RAPIDO' THEN 'denv_igg'
            WHEN 'DENGUE NS1, TESTE RAPIDO' THEN 'ns1_antigen'
            ELSE 'UNKNOWN'
        END
    WHEN 'TESTE RAPIDO PARA DENGUE IGM E NS1' THEN
        CASE unaccent(UPPER(detalhe_exame))
            WHEN 'DENGUE IGM, TESTE RAPIDO' THEN 'denv_igm'
            WHEN 'DENGUE NS1, TESTE RAPIDO' THEN 'ns1_antigen'
            ELSE 'UNKNOWN'
        END
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
    AND date_testing >= '2025-08-01' -- Com as mudanças dos IDs do Einstein, consideramos dados a partir de 2025-08-01 para inclusão