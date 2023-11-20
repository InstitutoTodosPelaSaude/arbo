WITH source_data AS (
    SELECT * FROM
    {{ ref("einstein_02_fix_values") }}
)
SELECT
    *
FROM source_data
WHERE test_kit='UNKNOWN' OR test_kit IS NULL