WITH source_data AS (
    SELECT * FROM
    {{ ref("02_einstein_fix_values") }}
)
SELECT
    COUNT(*)
FROM source_data
GROUP BY sample_id
HAVING COUNT(*) > 3