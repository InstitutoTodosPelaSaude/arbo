{{ config(materialized='table') }}

{% set result_is_numeric = "result ~ '[0-9]+[.]*[0-9]*' AND result ~ '^[0-9]'" %}
{% set INDETERMINADO   = -3 %}
{% set NAO_RECONHECIDO = -2 %}

WITH source_data AS (

    SELECT 
        *
    FROM
    {{ ref("dbmol_01_convert_types") }}

),
source_data_fix_values AS (
    SELECT
        MD5(
            CONCAT(
                test_id,
                exame,
                CASE exame
                    WHEN 'ESTUDO SOROLOGICO VIRUS MAYARO - ANTICORPOS IGG E IGM' THEN detalhe_exame
                    WHEN 'ANTICORPOS ANTI CHIKUNGUNYA IGG E IGM' THEN detalhe_exame
                    ELSE ''
                END
            )
        ) AS sample_id,

        test_id,
        
        CASE sex
            WHEN 'F' THEN 'F'
            WHEN 'M' THEN 'M'
            ELSE NULL
        END AS sex,

        CASE
            WHEN EXTRACT( YEAR FROM AGE(date_testing, birth_date) )::int > 120
            THEN NULL
            ELSE EXTRACT( YEAR FROM AGE(date_testing, birth_date) )::int
        END AS age,
        
        CASE exame
            -- VALIDAR ESSES EXAMES
            WHEN 'ZIKA VIRUS DETECCAO POR PCR - PLASMA' THEN 'zikv_pcr'
            WHEN 'ZIKA VIRUS ANTICORPOS IGM' THEN 'igm_serum'
            WHEN 'ZIKA VIRUS ANTICORPOS IGG' THEN 'igg_serum'
            WHEN 'PESQUISA MOLECULAR DO VIRUS CHIKUNGUNYA' THEN 'chikv_pcr'
            WHEN 'PAINEL DE ARBOVIROSES (DENGUE, ZIKA E CHIKUNGUNYA)' THEN 'arbo_pcr_3'
            WHEN 'ESTUDO SOROLOGICO VIRUS MAYARO - ANTICORPOS IGG E IGM' 
            THEN 
                CASE 
                    WHEN detalhe_exame = 'IGG' THEN 'igg_serum'
                    WHEN detalhe_exame = 'IGM' THEN 'igm_serum'
                    ELSE 'UNKNOWN'
                END
            
            WHEN 'DETECCAO MOLECULAR DO ZIKA VIRUS' THEN 'zikv_pcr'
            WHEN 'DETECCAO E TIPAGEM DO VIRUS DA DENGUE' THEN 'denv_pcr'
            WHEN 'DENGUE NS1' THEN 'ns1_antigen'
            WHEN 'DENGUE - ANTICORPOS IGM' THEN 'igm_serum'
            WHEN 'DENGUE - ANTICORPOS IGG' THEN 'igg_serum'
            WHEN 'CHIKUNGUNYA VIRUS IGM' THEN 'igm_serum'
            WHEN 'CHIKUNGUNYA VIRUS IGG' THEN 'igg_serum'
            WHEN 'ANTICORPOS ANTI CHIKUNGUNYA IGG E IGM'
            THEN 
                CASE 
                    WHEN detalhe_exame = 'AACHIG' THEN 'igg_serum'
                    WHEN detalhe_exame = 'AACHIM' THEN 'igm_serum'
                    ELSE 'UNKNOWN'
                END
            --- Acrescimo de novos exames
            WHEN 'MONKEYPOX' THEN 'mpox_pcr'
	        WHEN 'FEBRE AMARELA - DETECÇÃO POR PCR' THEN 'feama_pcr'
	        WHEN 'ESTUDO SOROLÓGICO -  VÍRUS FEBRE AMARELA IGG E IGM'
	        THEN 
		        CASE 
	 	            WHEN detalhe_exame = 'IGG' THEN 'feama_igg'
                    WHEN detalhe_exame = 'IGM' THEN 'feama_igm'
                    ELSE 'UNKNOWN' 
                END

            ELSE 'UNKNOWN'
        END AS test_kit,

        "CodigoProcedimento",

        CASE 
            WHEN exame = 'ESTUDO SOROLOGICO VIRUS MAYARO - ANTICORPOS IGG E IGM' and detalhe_exame = 'IGG' THEN 'MAYV_IGG'
            WHEN exame = 'ESTUDO SOROLOGICO VIRUS MAYARO - ANTICORPOS IGG E IGM' and detalhe_exame = 'IGM' THEN 'MAYV_IGM'
            WHEN exame = 'CHIKUNGUNYA VIRUS IGM' AND detalhe_exame = 'RESUL'  THEN 'CHIKV_IGM' 
            WHEN exame = 'CHIKUNGUNYA VIRUS IGG' AND detalhe_exame = 'RESUL'  THEN 'CHIKV_IGG'
            ELSE detalhe_exame
        END AS detalhe_exame,

        date_testing,
        location,
        {{ map_sigla_uf_to_name('state', null_if_not_match=True) }} AS state,
        
        CASE

            -- Testes ENZIMAIMUNOENSAIO com resultado numérico
            WHEN
                detalhe_exame IN ('DENGM','DENGG','AACHIG', 'AACHIM', 'ZICAM','ZICAG','CHIKUNG','CHIKUNG')
                OR ( exame in ('CHIKUNGUNYA VIRUS IGM', 'CHIKUNGUNYA VIRUS IGG') AND detalhe_exame = 'RESUL')
            THEN
                CASE
                    WHEN {{ result_is_numeric }} THEN
                        CASE 
                            WHEN result::FLOAT <  0.80 THEN 0
                            WHEN result::FLOAT >= 0.80 AND result::FLOAT <= 1.00 THEN {{ INDETERMINADO }}
                            WHEN result::FLOAT >  1.00 THEN 1
                            ELSE {{ NAO_RECONHECIDO }}
                        END
                    ELSE 
                        CASE 
                            WHEN result ILIKE 'INFERIOR A %' THEN 0
                            ELSE {{ NAO_RECONHECIDO }}
                        END
                END

            -- Testes diversos com resultado textual
            WHEN 
                detalhe_exame IN ('DNS1', 'ZIKA', 'TDENGE', 'CHIKV', 'ZIKAP', 'FEAMA', 'MPOX')
                OR
                ( exame ILIKE 'PAINEL DE ARBOVIROSES%' AND detalhe_exame IN ('CHIKUN', 'DENGUE', 'ZIKAV') ) -- PZDC
                OR
                (exame = 'ESTUDO SOROLÓGICO -  VÍRUS FEBRE AMARELA IGG E IGM' AND detalhe_exame IN ('IGG', 'IGM'))
            THEN
                CASE 
                    WHEN result IN ('POSITIVO', 'POSITIVA', 'DETECTADO')     
                        OR result ILIKE 'REAGENTE%' THEN 1
                    WHEN result IN ('NEGATIVO', 'NEGATIVA', 'NAO DETECTADO', 'NAO REAGENTE') THEN 0
                    ELSE {{ NAO_RECONHECIDO }}
                END

            -- Mayaro
            WHEN exame = 'ESTUDO SOROLOGICO VIRUS MAYARO - ANTICORPOS IGG E IGM'
            THEN
                CASE
                    WHEN result ILIKE 'INFERIOR A %'             THEN 0
                    WHEN result IN ('POSITIVO', 'DETECTADO')     THEN 1
                    WHEN result IN ('NEGATIVO', 'NAO DETECTADO') THEN 0
                    ELSE {{ NAO_RECONHECIDO }}
                END

            ELSE {{ NAO_RECONHECIDO }}
        END AS result,
        file_name

    FROM source_data
    WHERE 1=1
    AND "CodigoProcedimento" NOT IN
    (
    'AALM1',
    'AAU',
    'AAYE',
    'ACIMR',
    'ACIMR2',
    'ADENF',
    'ADENO',
    'AGLM1',
    'AGLM4',
    'AHBE',
    'AHCV',
    'ALM4B',
    'ANSP',
    'B19PC',
    'BORPC',
    'CCARB',
    'CESBL',
    'CHLAI',
    'CHLGT',
    'CHLMT',
    'CHLTM',
    'CLHGE',
    'CMGL',
    'CMPCR',
    'CMVAV',
    'CMVG',
    'CMVM',
    'CMVQT',
    'COV19A,'
    'COVI19Q',
    'CRYPC',
    'CRYUM',
    'CTAG',
    'CTCH',
    'CTNG',
    'CTPCR',
    'CUBAR',
    'CUL2',
    'CULANA,'
    'CULB',
    'CULBA',
    'CULE',
    'CULEFC',
    'CULES',
    'CULF',
    'CULF10',
    'CULF2',
    'CULF3',
    'CULF4',
    'CULF5',
    'CULF6',
    'CULF7',
    'CULF8',
    'CULF9',
    'CULFS',
    'CULFU',
    'CULH',
    'CULIQ',
    'CULLA',
    'CULLP',
    'CULLS',
    'CULM',
    'CULMU',
    'CULN',
    'CULNE',
    'CULNF',
    'CULNV',
    'CULOS',
    'CULOU',
    'CULPC',
    'CULPE',
    'CULPR',
    'CULS2',
    'CULSB',
    'CULSO',
    'CULST',
    'CULT',
    'CULU',
    'CULUR',
    'CULV',
    'CUVG',
    'CVRE',
    'DNADE',
    'EHECPC',
    'FLUAB',
    'FTAG',
    'FTAGL',
    'HBCG',
    'HBCM',
    'HBEAG',
    'HBPCR',
    'HBQL',
    'HBQT',
    'HCVGE',
    'HCVPC',
    'HCVQF',
    'HCVQT',
    'HCVRI',
    'HEMA1',
    'HEMC',
    'HEMOC',
    'HEPD',
    'HEPDAG',
    'HEPDM',
    'HERG',
    'HERGL',
    'HERM',
    'HERP',
    'HERPS',
    'HFUN',
    'HIVPC',
    'HIVQT',
    'HIVWB',
    'HMA1',
    'HPVAB',
    'HPVABP',
    'HPVC',
    'HPVG',
    'HPVRT',
    'HTLV',
    'HTLVGE,'
    'HTLVI',
    'HTLVSE',
    'HTLVW',
    'IGGCM',
    'IGGRB',
    'IGMRB',
    'INFAG',
    'LEGIO',
    'LEGPA',
    'LEGPG',
    'LEGPM',
    'LEGPN',
    'LISPCR',
    'MENLX',
    'MYPNA',
    'MYPNE',
    'MYPNG',
    'MYPNM',
    'NCMVM',
    'NEUCOV',
    'NGCH',
    'NGPCR',
    'NOROV',
    'NRUBM',
    'PB19G',
    'PB19I',
    'PB19M',
    'PCRYP',
    'PCRYS',
    'PMDD',
    'PNEUPC',
    'POSHEMO',
    'POSURI',
    'PSEMR',
    'PVIROC',
    'PVIROL',
    'RESP4',
    'ROTA',
    'RUBAV',
    'RUBE',
    'RUBEG',
    'RUBEM',
    'SALG',
    'SALM',
    'SARAG',
    'SARAM',
    'SIFT',
    'SINRE',
    'STREAG',
    'STREP',
    'STREPT',
    'TREPCR',
    'TSMIC',
    'UREAG',
    'UREAM',
    'USONDA',
    'UUPCR'
    )
    AND NOT detalhe_exame IN (
        'MAT', 'MATERIAL', 'METODO', 'SOROTI',
        'TITG', 'TITM' -- MAYARO TIT
    )
    AND NOT result IN ('RESULTADO CONFERIDO E LIBERADO.')
    AND NOT result ILIKE '%RESULTADO FORMATADO%'
)
SELECT
    *
FROM source_data_fix_values
WHERE result != {{ INDETERMINADO }}