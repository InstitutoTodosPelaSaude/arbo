WITH source_data AS (
    SELECT * FROM
    {{ ref("02_einstein_fix_values") }}
)
SELECT
    *
FROM source_data
WHERE date_testing > CURRENT_DATE