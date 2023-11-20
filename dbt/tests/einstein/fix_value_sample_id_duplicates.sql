WITH source_data AS (
    SELECT * FROM
    {{ ref("einstein_02_fix_values") }}
)
SELECT
    COUNT(*)
FROM source_data
GROUP BY sample_id
HAVING COUNT(*) > 3