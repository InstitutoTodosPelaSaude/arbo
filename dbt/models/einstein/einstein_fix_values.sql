
{{ config(materialized='table') }}

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
    {{ ref("einstein_convert_types") }}

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