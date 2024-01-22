-- Number of repetitions for each sample id is 1 or 3
WITH source_data AS (
    SELECT * FROM
    {{ ref("sabin_02_fix_values") }}
)
SELECT
    *
FROM 
(
    SELECT DISTINCT
        COUNT(*) AS count_lines
    FROM source_data
    GROUP BY sample_id
) AS count_lines_sample_id
WHERE count_lines != 1 AND count_lines != 3