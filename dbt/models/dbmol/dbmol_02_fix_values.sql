WITH source_data AS (

    SELECT * FROM
    {{ ref("dbmol_01_convert_types") }}

)
SELECT
    MD5(
        CONCAT(
            test_id,
            exame
            -- Falta adicionar o detalhe do exame aqui
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

        ELSE 'UNKNOWN'
    END AS test_kit,

    "CodigoProcedimento",
        
    CASE 
        WHEN exame = 'ESTUDO SOROLOGICO VIRUS MAYARO - ANTICORPOS IGG E IGM'
        THEN 'MAYV_IGG_IGM'
        WHEN exame in ('CHIKUNGUNYA VIRUS IGM', 'CHIKUNGUNYA VIRUS IGG') AND detalhe_exame = 'RESUL' 
        THEN 'CHIKV_IGG_IGM'
        ELSE detalhe_exame
    END AS detalhe_exame,

    date_testing,
    location,
    state,
    
    CASE
        WHEN result = 'POSITIVO' THEN 1
        WHEN result = 'NEGATIVO' THEN 0
        WHEN result = 'NAO DETECTADO' THEN 0
        WHEN result = 'DETECTADO' THEN 0

        -- WIP TEMPORARY
        -- REMEMBER REACTIVATE TEST ON dbmol.yaml
        WHEN result = 'INFERIOR A 5.0' THEN 0
        WHEN result = 'INFERIOR A 1/20' THEN 0
        WHEN result = 'INFERIOR A 0.1' THEN 0

        WHEN result ~ '[0-9]+[.]*[0-9]*' AND result ~ '^[0-9]' THEN
            CASE 
                WHEN result::FLOAT < 0.80 THEN 0
                ELSE 1
            END
        
        ELSE -2
    END AS result,


    file_name

FROM source_data
WHERE 1=1
AND "CodigoProcedimento" IN
(
    'DENGM',
    'DENGG',
    'AACHI',
    'ZICAM',
    'ZICAG',
    'DNS1',
    'CHIKUNM',
    'CHIKUNG',
    'PZDC',
    'ZIKAP',
    'CHIKV',
    'MAYA',
    'TDENGE',
    'ZIKA'
)
AND NOT detalhe_exame IN ('MAT', 'MATERIAL', 'METODO', 'SOROTI')