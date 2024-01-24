WITH source_data AS (
    SELECT * FROM
    {{ ref("sabin_01_convert_types") }}
)
SELECT
*
FROM source_data
WHERE date_testing > CURRENT_DATE