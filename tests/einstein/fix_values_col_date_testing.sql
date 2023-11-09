WITH source_data AS (
    SELECT * FROM
    {{ ref("einstein_fix_values") }}
)
SELECT
    *
FROM source_data
WHERE date_testing > CURRENT_DATE