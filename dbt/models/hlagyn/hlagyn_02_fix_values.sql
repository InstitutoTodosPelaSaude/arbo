{{config(materialized='table')}}

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

    -- tipo_material,
    location,
    state_code,
    
    COALESCE (metodologia, metodo) AS metodologia,
    
    file_name
FROM source_data
WHERE 1=1
    AND (
        -- At least one of the results is not null
        zika_result IS NOT NULL
        OR dengue_result IS NOT NULL
        OR chikungunya_result IS NOT NULL
    )