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
    age,
    location,
    state,
    
    CASE pathogen
        WHEN 'CHIKUNGUNYA, Anticorpos, IgG'       THEN 'igg_serumn' 
        WHEN 'CHIKUNGUNYA, Anticorpos, IgM'       THEN 'igm_serum' 
        WHEN 'Chikungunya, PCR'                   THEN 'chikv_pcr' 
        WHEN 'Dengue, antígeno NS1, teste rápido' THEN 'ns1_antigen' 
        WHEN 'Dengue, IgG'                        THEN 'igg_serumn' 
        WHEN 'Dengue, IgG, teste rápido'          THEN 'igg_serumn' 
        WHEN 'Dengue, IgM'                        THEN 'igm_serum' 
        WHEN 'Dengue, IgM, teste rápido'          THEN 'igm_serum' 
        WHEN 'Dengue, NS1'                        THEN 'ns1_antigen' 
        WHEN 'Zika vírus, detecção no RNA'        THEN 'zikv_pcr' 
        WHEN 'Zika vírus, IgG'                    THEN 'igg_serumn' 
        WHEN 'Zika vírus, IgM'                    THEN 'igm_serum' 
        ELSE 'UNKNOWN'
    END AS test_kit,
    CASE 
        WHEN result = 'INDETECTAVEL' THEN 0
        WHEN result = 'NAO REAGENTE' THEN 0
        WHEN result = 'REAGENTE'     THEN 1
        ELSE -2
    END AS result,

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