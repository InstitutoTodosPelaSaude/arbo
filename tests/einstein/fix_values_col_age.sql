WITH source_data AS (
    SELECT * FROM
    {{ ref("einstein_fix_values") }}
)
SELECT
    *
FROM source_data
WHERE age < 0 OR age > 120 OR age IS NULL