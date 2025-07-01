{{ config(materialized='table') }}
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
    SELECT * FROM
    {{ ref("fleury_01_convert_types") }}
)
SELECT
    md5(
        CONCAT(
            test_id,
            exame,
            pathogen
        )
    ) AS sample_id,

    test_id,
    date_testing,
    patient_id,
    sex,
    
    CASE
        -- 'D' means days, 'A' means years
        -- age only contains number of days if it's less than 1 year, so we consider it as 0
        WHEN regexp_like(age, '.+D')   THEN 0

        -- Fix ages in the format 10A, 20A10M, etc
        WHEN regexp_like(age, '^\d+A')
        THEN regexp_substr(age, '(\d+)')::int

        -- Fix ages in the format 5, 10, 100, etc
        WHEN regexp_like(age, '\d+') 
        THEN age::int
        
        ELSE NULL
    END AS age,

    CASE pathogen
        WHEN 'CHIKUNGUNYA, ANTICORPOS, IGG'       THEN 'igg_serum' 
        WHEN 'CHIKUNGUNYA, ANTICORPOS, IGM'       THEN 'igm_serum' 
        WHEN 'CHIKUNGUNYA, PCR'                   THEN 'chikv_pcr' 
        WHEN 'DENGUE, ANTIGENO NS1, TESTE RAPIDO' THEN 'ns1_antigen' 
        WHEN 'DENGUE, IGG'                        THEN 'igg_serum' 
        WHEN 'DENGUE, IGG, TESTE RAPIDO'          THEN 'igg_serum' 
        WHEN 'DENGUE, IGM'                        THEN 'igm_serum' 
        WHEN 'DENGUE, IGM, TESTE RAPIDO'          THEN 'igm_serum' 
        WHEN 'DENGUE, NS1'                        THEN 'ns1_antigen' 
        WHEN 'ZIKA VIRUS, DETECCAO NO RNA'        THEN 'zikv_pcr' 
        WHEN 'ZIKA VIRUS - IGG'                   THEN 'igg_serum' 
        WHEN 'ZIKA VIRUS - IGM'                   THEN 'igm_serum'
        WHEN 'FEBRE AMARELA, ANTICORPOS, IGG'     THEN 'igg_serum'
        WHEN 'FEBRE AMARELA, ANTICORPOS, IGM'     THEN 'igm_serum'
        WHEN 'FEBRE AMARELA, PCR'                 THEN 'yfv_pcr'
        WHEN 'MAYARO, ANTICORPOS, IGM'            THEN 'igm_serum'
        WHEN 'MAYARO, ANTICORPOS, IGG'            THEN 'igg_serum'
        WHEN 'MAYARO, DETECCAO DO RNA POR PCR'    THEN 'mayv_pcr'
        WHEN 'OROPOUCHE, ANTICORPOS, IGG'         THEN 'igg_serum'
        WHEN 'OROPOUCHE, ANTICORPOS, IGM'         THEN 'igm_serum'
        WHEN 'OROPOUCHE, DETECCAO DO RNA POR PCR' THEN 'orov_pcr'
        ELSE 'UNKNOWN'
    END AS test_kit,

    CASE 
        WHEN result = 'INDETECTAVEL'                    THEN 0

        WHEN result ILIKE 'NAO REAGENT%'                THEN 0
        WHEN result = 'NAO DETECTADO (NEGATIVO)'        THEN 0
        WHEN result = 'NEGATIVO'                        THEN 0

        WHEN result ILIKE 'REAGENT%'                    THEN 1
        WHEN result = 'DETECTADO (POSITIVO)'            THEN 1
        WHEN result = 'POSITIVO'                        THEN 1
        WHEN result = 'DETECTAVEL'                      THEN 1

        ELSE -2
    END AS result,

    location,
    CASE
        {% for state_code, state_name in state_code_to_state_name.items() %}
        WHEN state_code = '{{ state_code }}' THEN regexp_replace(upper(unaccent('{{ state_name }}')), '[^\w\s]', '', 'g')
        {% endfor %}
        ELSE NULL
    END AS state,
    file_name,
    pathogen

FROM source_data
WHERE 1=1
AND date_testing IS NOT NULL
AND exame NOT IN (
    -- Respiratory tests
    '2019NCOV', 'AGCOVIDNS', 'AGSINCURG', 'COVID19GX',
    'COVID19SALI', 'COVIDFLURSVGX', 'INFLUENZAPCR', 'VIRUSMOL', 'VRSAG',
    'AGINFLU', 'COVID19POCT', 'FLUABRSVGX', '2019NCOVTE'
)
AND NOT (
    result ILIKE 'RECOMENDAMOS A REPETICAO DESTE EXAME APOS UMA SEMANA%'
    OR result IS NULL
    OR result = 'INDETERMINADO'
    OR result = 'INCONCLUSIVO'
    
    -- Remoção temporária de resultados com valores
    OR result ILIKE '1/%'
    OR result ILIKE 'INFERIOR A 1/%'
    OR result ILIKE 'SUPERIOR A 1/%'
)
AND test_id NOT IN ('6330770357_400', '2020054349_200', '2020055814_200', '8950225454_100')
