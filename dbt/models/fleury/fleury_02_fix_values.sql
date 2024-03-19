{{ config(materialized='table') }}

WITH source_data AS (
    SELECT * FROM
    {{ ref("fleury_01_convert_types") }}
)
SELECT
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
        
        ELSE -1
    END AS age,
    CASE pathogen
        WHEN 'CHIKUNGUNYA, ANTICORPOS, IGG'       THEN 'igg_serumn' 
        WHEN 'CHIKUNGUNYA, ANTICORPOS, IGM'       THEN 'igm_serum' 
        WHEN 'CHIKUNGUNYA, PCR'                   THEN 'chikv_pcr' 
        WHEN 'DENGUE, ANTIGENO NS1, TESTE RAPIDO' THEN 'ns1_antigen' 
        WHEN 'DENGUE, IGG'                        THEN 'igg_serumn' 
        WHEN 'DENGUE, IGG, TESTE RAPIDO'          THEN 'igg_serumn' 
        WHEN 'DENGUE, IGM'                        THEN 'igm_serum' 
        WHEN 'DENGUE, IGM, TESTE RAPIDO'          THEN 'igm_serum' 
        WHEN 'DENGUE, NS1'                        THEN 'ns1_antigen' 
        WHEN 'ZIKA VIRUS, DETECCAO NO RNA'        THEN 'zikv_pcr' 
        WHEN 'ZIKA VIRUS - IGG'                    THEN 'igg_serumn' 
        WHEN 'ZIKA VIRUS - IGM'                    THEN 'igm_serum' 
        ELSE 'UNKNOWN'
    END AS test_kit,

    CASE 
        WHEN result = 'INDETECTAVEL' THEN 0
        WHEN result = 'NAO REAGENTE' THEN 0
        WHEN result = 'REAGENTE'     THEN 1
        ELSE -2
    END AS result,

    location,
    state,
    file_name

FROM source_data
WHERE 1=1
AND date_testing IS NOT NULL
AND exame NOT IN (
    -- Respiratory tests
    '2019NCOV', 'AGCOVIDNS', 'AGSINCURG', 'COVID19GX',
    'COVID19SALI', 'COVIDFLURSVGX', 'INFLUENZAPCR', 'VIRUSMOL', 'VRSAG',
    'AGINFLU'
)
AND (
    NOT result ILIKE 'RECOMENDAMOS A REPETICAO DESTE EXAME APOS UMA SEMANA%'
    OR result IS NULL
    OR result = 'INDETERMINADO'
)