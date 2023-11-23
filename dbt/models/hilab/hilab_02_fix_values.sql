

{{ config(materialized='table') }}

WITH source_data AS (

    SELECT * FROM
    {{ ref('hilab_01_convert_types') }}

)
SELECT
    md5(
        CONCAT(
            test_id,
            exame,
            patient_id
        )
    ) AS sample_id,
    test_id,
    state_code,
    regexp_replace(upper(unaccent(location)), '[^\w\s]', '', 'g') AS location,
    date_testing,
    exame,
    CASE 
        WHEN exame = 'Dengue IgM' THEN 'igm_serum'
        WHEN exame = 'Dengue IgG' THEN 'igg_serum'
        WHEN exame = 'Dengue NS1' THEN 'ns1_antigen'
        WHEN exame = 'Zika IgG' THEN 'igg_serum'
        WHEN exame = 'Zika IgM' THEN 'igm_serum'
        ELSE NULL
    END AS test_kit,
    CASE
        WHEN result = 'Não Reagente' THEN 0
        WHEN result = 'Reagente' THEN 1
        ELSE NULL
    END AS result,
    age,
    patient_id,
    CASE
        WHEN gender ILIKE 'F%' THEN 'F'
        WHEN gender ILIKE 'M%' THEN 'M'
        ELSE NULL
    END AS gender,
    file_name
FROM source_data