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
        WHEN detalhe_exame IN ('RCHIKUNGMELISAIGG','IGGCHIKUN','CHIKUNG1','CHIKUNG2') THEN 'chikv_igg'
        WHEN detalhe_exame IN ('RCHIKUNGMELISAIGM','IGMCHIKUNELISA','IGMCHIKUN','CHIKUNM1','CHIKUNM2') THEN 'chikv_igm'
        WHEN detalhe_exame IN ('DENGIGG','DENGUEGI') THEN 'denv_igg'
        WHEN detalhe_exame IN ('DENGUEMELISA','DENGUEMIC','DENGIGM','DENGUEMI') THEN 'denv_igm'
        WHEN detalhe_exame IN ('ZIKAGINDICE','ZIKAIGG2','ZIKAG') THEN 'zikv_igg'
        WHEN detalhe_exame IN ('ZIKAM2','ZIKAM1','ZIKAMINDICE','ZIKAM') THEN 'zikv_igm'
        WHEN detalhe_exame = 'FEBREGLC' THEN 'yfv_igg'
        WHEN detalhe_exame = 'FEBREMLC' THEN 'yfv_igm'
        WHEN detalhe_exame = 'OROG' THEN 'orov_igg'
        WHEN detalhe_exame = 'OROM' THEN 'orov_igm'
        WHEN detalhe_exame IN ('DNS1','NS1IMUNOCRO','NS1ELISA') THEN 'ns1_antigen'
        WHEN detalhe_exame IN ('DEVORO','OROVPCR') THEN 'orov_pcr'
        WHEN detalhe_exame IN ('PCRDE','DENV1','DENV2','DENV3','DENV4','DENV','PCRD') THEN 'denv_pcr'
        WHEN detalhe_exame IN ('MAYARO','RESMAYARO') THEN 'mayv_pcr'
        WHEN detalhe_exame = 'YF' THEN 'yfv_pcr'
        WHEN detalhe_exame = 'CHIKVPCR-BIOMOL' THEN 'chikv_pcr'
        WHEN detalhe_exame IN ('ZIKAPCRBIO','ZIKAV','ZIKA1','ZIKA2','ZIKAPCR') THEN 'zikv_pcr'
        ELSE 'UNKNOWN'
    END AS test_kit,

    CASE 
        WHEN sex ILIKE 'F%' THEN 'F'
        WHEN sex ILIKE 'M%' THEN 'M'
        ELSE NULL
    END AS sex,

    EXTRACT( YEAR FROM AGE(date_testing, birth_date) )::int AS age,
    
    regexp_replace(upper(unaccent(location)), '[^\w\s]', '', 'g') AS location,
    regexp_replace(upper(unaccent(state)), '[^\w\s]', '', 'g') AS state,

    CASE

        WHEN result = 'DETECTADO (PRESENCA DO MATERIAL GENETICO DO VIRUS DENGUE)' THEN 1
        WHEN result = 'DETECTADO (PRESENCA DE MATERIAL GENETICO DO VIRUS DENGUE).' THEN 1
        WHEN result = 'PRESENCA DE MATERIAL GENETICO DO VIRUS DENGUE.' THEN 1
        WHEN result = 'DETECTADO (PRESENCA DO MATERIAL GENETICO DO VIRUS ZIKA)' THEN 1
        WHEN result = 'DETECTADO (PRESENCA DO MATERIAL GENETICO DO VIRUS CHIKUNGUNYA)' THEN 1
        WHEN result = 'DETECTADO DENGUE SOROTIPO 2' THEN 1
        WHEN result = 'PRESENCA DE MATERIAL GENETICO DO VIRUS DENGUE,' THEN 1
        WHEN result = 'DETECTADO VIRUS DENGUE TIPO 1' THEN 1
        WHEN result = 'DETECTADO VIRUS DENGUE TIPO 2' THEN 1
        WHEN result = 'DETECTADO VIRUS DENGUE TIPO 3' THEN 1
        WHEN result = 'DETECTADO VIRUS DENGUE TIPO 4' THEN 1

        WHEN result = 'NAO DETECTADO (AUSENCIA DE MATERIAL GENETICO DO VIRUS CHIKUNGUNYA)' THEN 0
        WHEN result = 'NAO DETECTADO (AUSENCIA DO MATERIAL GENETICO DO VIRUS CHIKUNGUNYA)' THEN 0
        WHEN result = 'NAO DETECTADO (AUSENCIA DO MATERIAL GENETICO DO VIRUS CHIKUNGUNYA)' THEN 0
        WHEN result = 'NAO DETECTADO (AUSENCIA DO MATERIAL GENETICO DO VIRUS DENGUE)' THEN 0
        WHEN result = 'NAO DETECTADO (AUSENCIA DO MATERIAL GENETICO DO VIRUS ZIKA)' THEN 0
        WHEN result = 'NAO DETECTADO RNA DO VIRUS MAYARO' THEN 0
        WHEN result = 'NAO DETECTADO RNA DO VIRUS DA DENGUE' THEN 0
        WHEN result = 'NAO DETECTADO' THEN 0
        WHEN result = 'AUSENCIA DE MATERIAL GENETICO DO VIRUS ZIKA,' THEN 0
        WHEN result = 'NAO' THEN 0
        
        WHEN result = 'DETECTAVEL' THEN 1
        WHEN result = 'INDETECTAVEL' THEN 0
        WHEN result = '''' THEN 0

        WHEN result = 'NAO REAGENTE' THEN 0
        WHEN result = 'NAO RE' THEN 0
        
        WHEN result = 'NEGATIVO' THEN 0
        WHEN result = 'POSITIVO' THEN 1
        
        WHEN result = 'REAGENTE' THEN 1
        WHEN result = 'REAGENTE 1:200' THEN 1
        WHEN result = 'REAGENTE''' THEN 1
        WHEN result = 'REAGEN' THEN 1

        WHEN result = 'DETECTADO' THEN 1

        -- 9999 or 99,99 or 99.99
        WHEN result ~ '[0-9]+[,.]*[0-9]*' AND result ~ '^[0-9]' THEN
            CASE 
                WHEN regexp_replace(result , ',' , '.')::FLOAT < 0.80 THEN 0
                ELSE 1
            END

        -- .99
        WHEN result ~ '^[,.]+[0-9]*' THEN
            CASE 
                WHEN (regexp_replace(regexp_replace(result , ',' , '.'), '.', '')::FLOAT / 100) < 0.80 THEN 0
                ELSE 1
            END

        -- Historical data 2022 and 2023
        -- Avoid using this logic for new data
        WHEN date_testing < '2024-01-01' AND result ILIKE 'NAO REA%'                       THEN 0
        WHEN date_testing < '2024-01-01' AND result ILIKE 'NAO REGENTE'                    THEN 0
        WHEN date_testing < '2024-01-01' AND result ILIKE 'NAO DETECTADO%'                 THEN 0
        WHEN date_testing < '2024-01-01' AND result ILIKE 'INFERIOR A%'                    THEN 0
        WHEN date_testing < '2024-01-01' AND result ILIKE 'AUSENCIA DE MATERIAL GENETICO%' THEN 0
        WHEN date_testing < '2024-01-01' AND result ILIKE 'REAGEN%'                        THEN 1
        WHEN date_testing < '2024-01-01' AND result ILIKE 'PRESENCA DE MATERIAL GENETICO%' THEN 1
        WHEN date_testing < '2024-01-01' AND result ILIKE 'DETECTADO %'                    THEN 1


        
        ELSE -2 -- UNKNOWN
    END::FLOAT AS result,

    date_testing,
    file_name

FROM source_data
WHERE NOT detalhe_exame in (
    'OBSGERALINTERNA'
    --'FEBREGLC', 'FEBREMLC' -- Redundance of results
)
AND result IS NOT NULL
AND NOT result IN (
    -- Historical data 2022 and 2023
    -- Not relevant for new data
    'INDETERMINADO', 
    'INCONCLUSIVO',
    'INCONCLUSIVE',
    'INCONCLUSIVO, VIDE NOTA.',
    '-',
    '*', 
    'E',
    '',
    'TESTE',
    'XXXXXX',
    'XXXXXXX',
    'XXXXXXXX',
    'XXXXXXXXX'
)
-- WIP: remove this filter
AND NOT detalhe_exame IN (
    'PCRDECT',
    'CHIKUNGMPDF',
    'RES.ARQUIVADO.TXT',
    'INDICEM1',
    'INDICEG',
    'RCHIKUNGMIMUNOG',
    'ZIKAPCRCT',
    'YFV',
    'DENGUEGPDF',
    'CHIKUNGGELISA',
    'CHIKUNGMELISAG',
    'CHIKUNGMIMUN',
    'RES.ARQUIVADO.RTF',
    'YFIC',
    'RCHIKUGMIMUNOM',
    'CHIKUNGGIMUN',
    'VITAKPDF',
    'PCRCHIKCT',
    'ADOLFOLUTZPDF',
)
AND NOT (
    -- Remover exames com detalhe_exame YFV e exame PAINEL MOLECULAR PARA DENGUE
    -- Inconsistência de dados
    detalhe_exame IN ('YFV') AND exame ILIKE 'PAINEL MOLECULAR PARA DENGUE %'
)
AND detalhe_exame IN (
  -- Chikungunya
  'RCHIKUNGMELISAIGG','IGGCHIKUN','CHIKUNG1','CHIKUNG2',
  'RCHIKUNGMELISAIGM','IGMCHIKUNELISA','IGMCHIKUN','CHIKUNM1','CHIKUNM2',
  'CHIKVPCR-BIOMOL',
  -- Dengue
  'DENGIGG','DENGUEGI',
  'DENGUEMELISA','DENGUEMIC','DENGIGM','DENGUEMI',
  'DNS1','NS1IMUNOCRO','NS1ELISA',
  'PCRDE','DENV1','DENV2','DENV3','DENV4','DENV','PCRD',
  -- Zika
  'ZIKAGINDICE','ZIKAIGG2','ZIKAG',
  'ZIKAM2','ZIKAM1','ZIKAMINDICE','ZIKAM',
  'ZIKAPCRBIO','ZIKAV','ZIKA1','ZIKA2','ZIKAPCR',
  -- Febre amarela
  'FEBREGLC','FEBREMLC','YF',
  -- Oropouche
  'OROG','OROM','DEVORO','OROVPCR',
  -- Mayaro
  'MAYARO','RESMAYARO'
)