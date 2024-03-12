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

    birth_date,

    
    exame,
    "CodigoProcedimento",
    detalhe_exame,
    date_testing,
    location,
    state,
    
    CASE
        WHEN result = 'POSITIVO' THEN 1
        WHEN result = 'NEGATIVO' THEN 0
        WHEN result = 'NAO DETECTADO' THEN 0
        WHEN result = 'DETECTADO' THEN 0

        -- WIP TEMPORARY
        WHEN result = 'INFERIOR A 5.0' THEN 0
        WHEN result = 'INFERIOR A 1/20' THEN 0
        WHEN result = 'INFERIOR A 0.1' THEN 0

        WHEN result ~ '[0-9]+[.]*[0-9]*' THEN
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