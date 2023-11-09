

WITH source_data AS (
    SELECT * FROM
    {{ ref("einstein_fix_values") }}
)
SELECT
    *
FROM source_data
WHERE result != 'Pos' OR result != 'Neg'