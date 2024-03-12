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
    result,
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