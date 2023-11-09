
{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref("einstein_convert_types") }}

)
SELECT 

    test_id,
    
    CASE 
        WHEN gender ILIKE 'F%' THEN 'F'
        WHEN gender ILIKE 'M%' THEN 'M'
        ELSE 'UNKNOWN'
    END AS gender,

    age,
    test_kit,
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