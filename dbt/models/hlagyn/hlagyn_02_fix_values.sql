{{config(materialized='table')}}
{%
    set state_code_to_state_name = {
        'AC': 'Acre',
        'AL': 'Alagoas',
        'AP': 'Amapá',
        'AM': 'Amazonas',
        'BA': 'Bahia',
        'CE': 'Ceará',
        'DF': 'Distrito Federal',
        'ES': 'Espírito Santo',
        'GO': 'Goiás',
        'MA': 'Maranhão',
        'MT': 'Mato Grosso',
        'MS': 'Mato Grosso do Sul',
        'MG': 'Minas Gerais',
        'PA': 'Pará',
        'PB': 'Paraíba',
        'PR': 'Paraná',
        'PE': 'Pernambuco',
        'PI': 'Piauí',
        'RJ': 'Rio de Janeiro',
        'RN': 'Rio Grande do Norte',
        'RS': 'Rio Grande do Sul',
        'RO': 'Rondônia',
        'RR': 'Roraima',
        'SC': 'Santa Catarina',
        'SP': 'São Paulo',
        'SE': 'Sergipe',
        'TO': 'Tocantins'
    }
%}

WITH source_data AS (
    SELECT
    *
    FROM
    {{ ref("hlagyn_01_convert_types") }}
)
SELECT
    md5(
        CONCAT(
            test_id,
            date_testing,
            age,
            gender
        )
    ) AS sample_id,
    test_id,
    patient_id,
    
    age,
    CASE
        WHEN gender ILIKE 'M%' THEN 'M'
        WHEN gender ILIKE 'F%' THEN 'F'
        ELSE NULL
    END AS gender, 

    date_testing,
    CASE
        WHEN dengue_result='Detectado' THEN 1
        WHEN dengue_result='Não Detectado' THEN 0
        ELSE NULL
    END AS dengue_result,
    CASE
        WHEN zika_result='Detectado' THEN 1
        WHEN zika_result='Não Detectado' THEN 0
        ELSE NULL
    END AS zika_result,
    CASE
        WHEN chikungunya_result='Detectado' THEN 1
        WHEN chikungunya_result='Não Detectado' THEN 0
        ELSE NULL
    END AS chikungunya_result,

    location,
    CASE
        {% for state_code, state_name in state_code_to_state_name.items() %}
        WHEN state_code = '{{ state_code }}' THEN regexp_replace(upper(unaccent('{{ state_name }}')), '[^\w\s]', '', 'g')
        {% endfor %}
        ELSE NULL
    END AS state,

    -- tipo_material,
    -- COALESCE (metodologia, metodo) AS metodologia,
    'arbo_pcr_3' AS test_kit,
    
    file_name
FROM source_data
WHERE 1=1
    AND (
        -- At least one of the results is not null
        zika_result IS NOT NULL
        OR dengue_result IS NOT NULL
        OR chikungunya_result IS NOT NULL
    )