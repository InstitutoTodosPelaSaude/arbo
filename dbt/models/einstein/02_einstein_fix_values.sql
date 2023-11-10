
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


-- test_id, exame, detalhe_exame

{%
    set 
    test_kit_map = {
        "Dengue IgM": "igm_serum",
        "Dengue IgG/IgM": "igg_serum",
        "Dengue IgG/IgM": "igm_serum",
        "Dengue NS1": "ns1_antigen",
        "Vírus Chikungunya PCR": "arbo_pcr_3",
        "Vírus Dengue PCR": "arbo_pcr_3",
        "Vírus Zika PCR": "arbo_pcr_3",
    }
%}

WITH source_data AS (

    SELECT * FROM
    {{ ref("01_einstein_convert_types") }}

)
SELECT 

    test_id,
    CASE
        {% for test_kit in test_kit_map %}
            WHEN test_kit ILIKE '{{ test_kit }}' THEN '{{ test_kit_map[test_kit] }}'
        {% endfor %}
        ELSE 'UNKNOWN'
    END AS test_kit,
    
    CASE 
        WHEN gender ILIKE 'F%' THEN 'F'
        WHEN gender ILIKE 'M%' THEN 'M'
        ELSE 'UNKNOWN'
    END AS gender,
    age,

    detalhe_exame,
    location,
    state,
    pathogen,
    
    CASE 
        WHEN result = 'DETECTADO' THEN 'Pos'
        WHEN result = 'NÃO DETECTADO' THEN 'Neg'
        ELSE 'UNKNOWN' 
    END AS result,

    date_testing,
    file_name

FROM source_data